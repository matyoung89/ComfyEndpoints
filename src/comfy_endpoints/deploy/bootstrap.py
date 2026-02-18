from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from comfy_endpoints.contracts.validators import parse_workflow_contract
from comfy_endpoints.deploy.cache_manager import CacheManager
from comfy_endpoints.gateway.comfy_client import ComfyClient, ComfyClientError
from comfy_endpoints.gateway.prompt_mapper import build_preflight_payload
from comfy_endpoints.models import ArtifactSourceSpec


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def ensure_contract_file(contract_path: Path) -> None:
    if contract_path.exists():
        return

    contract_json = os.getenv("COMFY_ENDPOINTS_CONTRACT_JSON", "").strip()
    if not contract_json:
        raise RuntimeError(f"Contract path missing: {contract_path}")

    try:
        parsed = json.loads(contract_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid COMFY_ENDPOINTS_CONTRACT_JSON payload") from exc

    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(json.dumps(parsed, indent=2), encoding="utf-8")


def ensure_workflow_file(workflow_path: Path) -> None:
    if workflow_path.exists():
        return

    workflow_json = os.getenv("COMFY_ENDPOINTS_WORKFLOW_JSON", "").strip()
    if not workflow_json:
        raise RuntimeError(f"Workflow path missing: {workflow_path}")

    try:
        parsed = json.loads(workflow_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid COMFY_ENDPOINTS_WORKFLOW_JSON payload") from exc

    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    workflow_path.write_text(json.dumps(parsed, indent=2), encoding="utf-8")


def wait_for_comfy_ready(comfy_url: str, timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    last_error = "unknown"
    while time.time() < deadline:
        request = urllib.request.Request(f"{comfy_url.rstrip('/')}/system_stats", method="GET")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    return
                last_error = f"unexpected_status:{response.status}"
        except urllib.error.HTTPError as exc:
            last_error = f"http_error:{exc.code}"
        except urllib.error.URLError as exc:
            last_error = f"url_error:{exc.reason}"
        time.sleep(3)
    raise RuntimeError(f"Comfy startup timeout waiting for readiness: {last_error}")


def _probe_manager_endpoint_status(comfy_url: str, path: str) -> str:
    request = urllib.request.Request(
        f"{comfy_url.rstrip('/')}{path}",
        headers={"accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        reason = str(exc.reason) if exc.reason else "connection error"
        return f"URL error: {reason}"


def _log_manager_endpoint_probes(comfy_url: str) -> None:
    probe_paths = (
        "/customnode/getmappings?mode=default",
        "/customnode/getlist?mode=default&skip_update=true",
        "/externalmodel/getlist?mode=default",
    )
    for path in probe_paths:
        status = _probe_manager_endpoint_status(comfy_url, path)
        print(f"[bootstrap] manager endpoint probe {path} -> {status}", file=sys.stderr)


MISSING_MODEL_PATTERN = re.compile(
    r"Value not in list:\s*(?P<input_name>[A-Za-z0-9_]+)\s*:\s*'(?P<filename>[^']+)'",
)
MISSING_NODE_PATTERN = re.compile(r"Node '(?P<class_type>[^']+)' not found")

MODEL_DIR_BY_INPUT_NAME = {
    "ckpt_name": "checkpoints",
    "unet_name": "diffusion_models",
    "clip_name": "text_encoders",
    "clip_name1": "text_encoders",
    "clip_name2": "text_encoders",
    "vae_name": "vae",
    "lora_name": "loras",
    "control_net_name": "controlnet",
}

MODEL_DIR_BY_TYPE = {
    "checkpoint": "checkpoints",
    "checkpoints": "checkpoints",
    "diffusion_model": "diffusion_models",
    "diffusion_models": "diffusion_models",
    "unet": "diffusion_models",
    "text_encoder": "text_encoders",
    "text_encoders": "text_encoders",
    "clip": "text_encoders",
    "vae": "vae",
    "lora": "loras",
    "loras": "loras",
    "controlnet": "controlnet",
    "control_net": "controlnet",
}

NODE_CLASS_REPO_OVERRIDES = {
    "Wan22Animate": ["https://github.com/kijai/ComfyUI-WanVideoWrapper"],
    "WanVideoVAELoader": ["https://github.com/kijai/ComfyUI-WanVideoWrapper"],
    "WanVideoModelLoader": ["https://github.com/kijai/ComfyUI-WanVideoWrapper"],
    "WanVideoAnimateEmbeds": ["https://github.com/kijai/ComfyUI-WanVideoWrapper"],
    "WanVideoSampler": ["https://github.com/kijai/ComfyUI-WanVideoWrapper"],
    "WanVideoDecode": ["https://github.com/kijai/ComfyUI-WanVideoWrapper"],
}

NODE_CLASS_PIP_OVERRIDES = {
    "Wan22Animate": ["accelerate"],
}
NODE_CLASS_INPUT_MODEL_DIR_OVERRIDES = {
    ("WanVideoModelLoader", "model"): "diffusion_models",
    ("WanVideoVAELoader", "model_name"): "vae",
}
MODEL_SUBDIRS = {
    "checkpoints",
    "diffusion_models",
    "text_encoders",
    "vae",
    "loras",
    "controlnet",
}


@dataclass(slots=True)
class MissingModelRequirement:
    input_name: str
    filename: str
    class_type: str | None = None


@dataclass(slots=True)
class MissingNodeRequirement:
    class_type: str


def _required_nodes_from_prompt(prompt_payload: dict) -> list[MissingNodeRequirement]:
    prompt = prompt_payload.get("prompt")
    if not isinstance(prompt, dict):
        return []
    required: list[MissingNodeRequirement] = []
    seen: set[str] = set()
    for node in prompt.values():
        if not isinstance(node, dict):
            continue
        class_type = node.get("class_type")
        if not isinstance(class_type, str) or not class_type:
            continue
        if class_type in seen:
            continue
        seen.add(class_type)
        required.append(MissingNodeRequirement(class_type=class_type))
    return required


def _json_error_payload(stage: str, message: str, details: dict | None = None) -> dict:
    payload = {
        "status": "artifact_resolver_failed",
        "stage": stage,
        "message": message,
    }
    if details:
        payload["details"] = details
    return payload


def _load_app_artifact_specs_from_env() -> list[ArtifactSourceSpec]:
    raw = os.getenv("COMFY_ENDPOINTS_APP_ARTIFACTS_JSON", "").strip()
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise RuntimeError("COMFY_ENDPOINTS_APP_ARTIFACTS_JSON must be a JSON array")
    specs: list[ArtifactSourceSpec] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise RuntimeError(f"artifact entry at index {index} must be an object")
        specs.append(
            ArtifactSourceSpec(
                match=str(item.get("match", "")).strip(),
                source_url=str(item.get("source_url", "")).strip(),
                target_subdir=str(item.get("target_subdir", "")).strip(),
                target_path=str(item.get("target_path", "")).strip(),
                kind=str(item.get("kind", "model")).strip() or "model",
                ref=str(item.get("ref", "")).strip() or None,
                provides=[
                    str(value).strip()
                    for value in (item.get("provides", []) if isinstance(item.get("provides", []), list) else [])
                    if str(value).strip()
                ],
            )
        )
    return specs


def _artifact_candidates_from_spec(item: ArtifactSourceSpec) -> set[str]:
    candidates = _catalog_filename_candidates(item.match)
    candidates.update(_catalog_filename_candidates(item.target_path))
    return {value for value in candidates if value}


def _missing_models_from_preflight_error(exc: ComfyClientError) -> list[MissingModelRequirement]:
    text_candidates: list[str] = []
    if exc.response_text:
        text_candidates.append(exc.response_text)
    if isinstance(exc.response_json, dict):
        for key in ("details", "message"):
            value = exc.response_json.get(key)
            if isinstance(value, str):
                text_candidates.append(value)

    parsed: list[MissingModelRequirement] = []
    seen: set[tuple[str, str]] = set()
    for blob in text_candidates:
        for match in MISSING_MODEL_PATTERN.finditer(blob):
            input_name = match.group("input_name").strip()
            filename = match.group("filename").strip()
            if not input_name or not filename:
                continue
            key = (input_name, filename)
            if key in seen:
                continue
            seen.add(key)
            parsed.append(MissingModelRequirement(class_type=None, input_name=input_name, filename=filename))
    return parsed


def _missing_nodes_from_preflight_error(exc: ComfyClientError) -> list[MissingNodeRequirement]:
    text_candidates: list[str] = []
    if exc.response_text:
        text_candidates.append(exc.response_text)
    if isinstance(exc.response_json, dict):
        for key in ("details", "message"):
            value = exc.response_json.get(key)
            if isinstance(value, str):
                text_candidates.append(value)

    parsed: list[MissingNodeRequirement] = []
    seen: set[str] = set()
    for blob in text_candidates:
        for match in MISSING_NODE_PATTERN.finditer(blob):
            class_type = match.group("class_type").strip()
            if not class_type or class_type in seen:
                continue
            seen.add(class_type)
            parsed.append(MissingNodeRequirement(class_type=class_type))
    return parsed


def _extract_dropdown_options(input_spec: object) -> set[str]:
    if not isinstance(input_spec, list) or not input_spec:
        return set()

    first = input_spec[0]
    if isinstance(first, list):
        return {str(item) for item in first if isinstance(item, str)}

    return set()


def _missing_models_from_object_info(
    prompt_payload: dict,
    object_info_payload: dict,
) -> list[MissingModelRequirement]:
    prompt = prompt_payload.get("prompt")
    if not isinstance(prompt, dict):
        return []

    parsed: list[MissingModelRequirement] = []
    seen: set[tuple[str, str]] = set()
    for node in prompt.values():
        if not isinstance(node, dict):
            continue
        class_type = node.get("class_type")
        if not isinstance(class_type, str) or not class_type:
            continue

        class_info = object_info_payload.get(class_type)
        if not isinstance(class_info, dict):
            continue

        input_block = class_info.get("input")
        if not isinstance(input_block, dict):
            continue

        required = input_block.get("required")
        optional = input_block.get("optional")
        if not isinstance(required, dict):
            required = {}
        if not isinstance(optional, dict):
            optional = {}

        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue

        for input_name, value in inputs.items():
            if not isinstance(input_name, str) or not isinstance(value, str):
                continue
            if not value:
                continue

            input_spec = required.get(input_name)
            if input_spec is None:
                input_spec = optional.get(input_name)

            options = _extract_dropdown_options(input_spec)
            if not options:
                continue
            if value in options:
                continue

            key = (class_type, input_name, value)
            if key in seen:
                continue
            seen.add(key)
            parsed.append(
                MissingModelRequirement(class_type=class_type, input_name=input_name, filename=value)
            )

    return parsed


def _known_model_requirements_from_prompt(prompt_payload: dict) -> list[MissingModelRequirement]:
    prompt = prompt_payload.get("prompt")
    if not isinstance(prompt, dict):
        return []

    parsed: list[MissingModelRequirement] = []
    seen: set[tuple[str, str]] = set()
    for node in prompt.values():
        if not isinstance(node, dict):
            continue
        class_type = node.get("class_type")
        if not isinstance(class_type, str) or not class_type:
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue

        for input_name, value in inputs.items():
            override_key = (class_type, input_name)
            if input_name not in MODEL_DIR_BY_INPUT_NAME and override_key not in NODE_CLASS_INPUT_MODEL_DIR_OVERRIDES:
                continue
            if not isinstance(value, str) or not value.strip():
                continue
            filename = value.strip()
            key = (class_type, input_name, filename)
            if key in seen:
                continue
            seen.add(key)
            parsed.append(
                MissingModelRequirement(class_type=class_type, input_name=input_name, filename=filename)
            )

    return parsed


def _missing_nodes_from_object_info(
    prompt_payload: dict,
    object_info_payload: dict,
) -> list[MissingNodeRequirement]:
    prompt = prompt_payload.get("prompt")
    if not isinstance(prompt, dict):
        return []

    known_node_classes = {key for key in object_info_payload.keys() if isinstance(key, str) and key}
    if not known_node_classes:
        return []

    parsed: list[MissingNodeRequirement] = []
    seen: set[str] = set()
    for node in prompt.values():
        if not isinstance(node, dict):
            continue
        class_type = node.get("class_type")
        if not isinstance(class_type, str) or not class_type:
            continue
        if class_type in known_node_classes:
            continue
        if class_type in seen:
            continue
        seen.add(class_type)
        parsed.append(MissingNodeRequirement(class_type=class_type))

    return parsed


def _iter_model_entries(payload: object) -> list[dict]:
    entries: list[dict] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            values_lower = {str(k).lower(): v for k, v in node.items()}
            filename_raw = values_lower.get("filename") or values_lower.get("name")
            url_raw = values_lower.get("url") or values_lower.get("download_url")
            if isinstance(filename_raw, str) and isinstance(url_raw, str):
                item = {
                    "filename": filename_raw.strip(),
                    "url": url_raw.strip(),
                    "type": str(values_lower.get("type", "")).strip().lower(),
                }
                if item["filename"] and item["url"]:
                    entries.append(item)
            for value in node.values():
                walk(value)
            return

        if isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    unique: dict[tuple[str, str], dict] = {}
    for item in entries:
        unique[(item["filename"], item["url"])] = item
    return list(unique.values())


def _is_url_like(value: str) -> bool:
    lower = value.lower()
    return lower.startswith("http://") or lower.startswith("https://") or lower.startswith("git@")


def _normalize_repo_url(url: str) -> str:
    value = url.strip()
    if not value:
        return ""
    if value.startswith("git@github.com:"):
        owner_repo = value.removeprefix("git@github.com:")
        if owner_repo.endswith(".git"):
            owner_repo = owner_repo[:-4]
        return f"https://github.com/{owner_repo.strip('/')}"

    parsed = urllib.parse.urlparse(value)
    if parsed.netloc in {"raw.githubusercontent.com", "github.com"}:
        segments = [segment for segment in parsed.path.split("/") if segment]
        if parsed.netloc == "raw.githubusercontent.com":
            if len(segments) >= 2:
                return f"https://github.com/{segments[0]}/{segments[1]}"
        if parsed.netloc == "github.com":
            if len(segments) >= 2:
                repo_url = f"https://github.com/{segments[0]}/{segments[1]}"
                if repo_url.endswith(".git"):
                    return repo_url[:-4]
                return repo_url
    return value.removesuffix(".git")


def _collect_repo_urls(value: object) -> set[str]:
    urls: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, str):
            if _is_url_like(node):
                normalized = _normalize_repo_url(node)
                if normalized:
                    urls.add(normalized)
            return
        if isinstance(node, dict):
            for key, item in node.items():
                if isinstance(key, str) and _is_url_like(key):
                    normalized = _normalize_repo_url(key)
                    if normalized:
                        urls.add(normalized)
                walk(item)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(value)
    return urls


def _find_repo_urls_for_node_class(class_type: str, payload: object) -> set[str]:
    matches: set[str] = set()

    def walk(node: object) -> bool:
        if isinstance(node, str):
            return node == class_type
        if isinstance(node, dict):
            local_hit = False
            local_urls: set[str] = set()
            for key, value in node.items():
                key_hit = isinstance(key, str) and key == class_type
                value_hit = walk(value)
                if key_hit or value_hit:
                    local_hit = True
                    if isinstance(key, str) and _is_url_like(key):
                        normalized = _normalize_repo_url(key)
                        if normalized:
                            local_urls.add(normalized)
                    local_urls.update(_collect_repo_urls(value))
            if local_hit:
                if local_urls:
                    matches.update(local_urls)
                else:
                    matches.update(_collect_repo_urls(node))
            return local_hit
        if isinstance(node, list):
            local_hit = False
            local_urls: set[str] = set()
            for value in node:
                value_hit = walk(value)
                if value_hit:
                    local_hit = True
                    local_urls.update(_collect_repo_urls(value))
            if local_hit:
                if local_urls:
                    matches.update(local_urls)
                else:
                    matches.update(_collect_repo_urls(node))
            return local_hit
        return False

    walk(payload)
    return matches


def _find_package_ids_for_node_class(class_type: str, payload: object) -> set[str]:
    package_ids: set[str] = set()

    def walk(node: object) -> bool:
        if isinstance(node, str):
            return node == class_type
        if isinstance(node, dict):
            local_hit = False
            for key, value in node.items():
                key_hit = isinstance(key, str) and key == class_type
                value_hit = walk(value)
                if key_hit or value_hit:
                    local_hit = True
                    if isinstance(key, str) and key != class_type and not _is_url_like(key):
                        package_ids.add(key)
                    if key_hit and isinstance(value, str) and value != class_type and not _is_url_like(value):
                        package_ids.add(value)
            return local_hit
        if isinstance(node, list):
            local_hit = False
            for value in node:
                if walk(value):
                    local_hit = True
            return local_hit
        return False

    walk(payload)
    return package_ids


def _find_repo_urls_for_package_ids(package_ids: set[str], payload: object) -> set[str]:
    if not package_ids:
        return set()
    matches: set[str] = set()

    def walk(node: object) -> bool:
        if isinstance(node, str):
            return node in package_ids
        if isinstance(node, dict):
            direct_hit = False
            for key, value in node.items():
                if isinstance(key, str) and key in package_ids:
                    direct_hit = True
                if isinstance(value, str) and value in package_ids:
                    direct_hit = True
            child_hit = False
            for value in node.values():
                if walk(value):
                    child_hit = True
            if direct_hit:
                matches.update(_collect_repo_urls(node))
            return direct_hit or child_hit
        if isinstance(node, list):
            for value in node:
                if walk(value):
                    return True
            return False
        return False

    walk(payload)
    return matches


def _fetch_manager_default_node_mappings() -> object:
    url = "https://raw.githubusercontent.com/Comfy-Org/ComfyUI-Manager/main/extension-node-map.json"
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode("utf-8")
    return json.loads(body or "{}")


def _repo_dir_name(repo_url: str) -> str:
    cleaned = repo_url.rstrip("/")
    name = cleaned.rsplit("/", 1)[-1]
    if name.endswith(".git"):
        name = name[:-4]
    return name.strip()


def _install_custom_node_by_git_clone(repo_url: str, ref: str = "main") -> bool:
    repo = repo_url.strip()
    if not repo:
        return False
    custom_nodes_root = Path("/opt/comfy/custom_nodes")
    custom_nodes_root.mkdir(parents=True, exist_ok=True)
    repo_name = _repo_dir_name(repo)
    if not repo_name:
        return False

    target_dir = custom_nodes_root / repo_name
    if target_dir.exists():
        subprocess.run(
            ["git", "-C", str(target_dir), "fetch", "--depth", "1", "origin", ref],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            ["git", "-C", str(target_dir), "checkout", ref],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True

    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", ref, repo, str(target_dir)],
        check=True,
    )
    return True


def _install_custom_node_python_dependencies(repo_url: str) -> None:
    repo_name = _repo_dir_name(repo_url)
    if not repo_name:
        return
    node_root = Path("/opt/comfy/custom_nodes") / repo_name
    if not node_root.exists() or not node_root.is_dir():
        return
    requirements_path = node_root / "requirements.txt"
    if requirements_path.exists():
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-cache-dir", "-r", str(requirements_path)],
            check=True,
        )


def _install_custom_node_override_packages(class_type: str) -> None:
    packages = NODE_CLASS_PIP_OVERRIDES.get(class_type, [])
    if not packages:
        return
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-cache-dir", *packages],
        check=True,
    )


def _install_missing_custom_nodes(
    comfy_client: ComfyClient,
    requirements: list[MissingNodeRequirement],
) -> int:
    if not requirements:
        return 0

    mapping_payload: object = {}
    try:
        mapping_payload = comfy_client.get_custom_node_mappings()
    except ComfyClientError as exc:
        print(f"[bootstrap] manager custom node mappings unavailable: {exc}", file=sys.stderr)

    list_payload: object = {}
    try:
        list_payload = comfy_client.get_custom_node_list()
    except ComfyClientError as exc:
        print(f"[bootstrap] manager custom node list unavailable: {exc}", file=sys.stderr)

    if not mapping_payload:
        try:
            mapping_payload = _fetch_manager_default_node_mappings()
            print("[bootstrap] using fallback extension-node-map catalog", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            print(f"[bootstrap] fallback node catalog unavailable: {exc}", file=sys.stderr)
            mapping_payload = {}

    installed_count = 0
    attempted_repo_urls: set[str] = set()
    for requirement in requirements:
        candidate_urls = _find_repo_urls_for_node_class(requirement.class_type, mapping_payload)
        candidate_urls.update(_find_repo_urls_for_node_class(requirement.class_type, list_payload))
        if not candidate_urls:
            package_ids = _find_package_ids_for_node_class(requirement.class_type, mapping_payload)
            candidate_urls.update(_find_repo_urls_for_package_ids(package_ids, list_payload))
        preferred_urls = NODE_CLASS_REPO_OVERRIDES.get(requirement.class_type, [])
        if not candidate_urls and not preferred_urls:
            print(
                f"[bootstrap] no catalog match for required node class_type={requirement.class_type}",
                file=sys.stderr,
            )
            continue

        installed = False
        ordered_candidate_urls = [*preferred_urls]
        ordered_candidate_urls.extend(
            sorted(url for url in candidate_urls if url not in set(preferred_urls))
        )

        for repo_url in ordered_candidate_urls:
            if repo_url in attempted_repo_urls:
                continue
            attempted_repo_urls.add(repo_url)
            try:
                comfy_client.install_custom_node_by_git_url(repo_url)
                installed_count += 1
                installed = True
                print(
                    f"[bootstrap] installed custom node class_type={requirement.class_type} repo={repo_url}",
                    file=sys.stderr,
                )
                break
            except ComfyClientError as exc:
                print(
                    f"[bootstrap] failed custom node install repo={repo_url}: {exc}",
                    file=sys.stderr,
                )
                try:
                    if _install_custom_node_by_git_clone(repo_url):
                        _install_custom_node_python_dependencies(repo_url)
                        _install_custom_node_override_packages(requirement.class_type)
                        installed_count += 1
                        installed = True
                        print(
                            f"[bootstrap] installed custom node via git clone "
                            f"class_type={requirement.class_type} repo={repo_url}",
                            file=sys.stderr,
                        )
                        break
                except Exception as clone_exc:  # noqa: BLE001
                    print(
                        f"[bootstrap] fallback git clone failed repo={repo_url}: {clone_exc}",
                        file=sys.stderr,
                    )

        if not installed:
            print(
                f"[bootstrap] unable to install required node class_type={requirement.class_type}",
                file=sys.stderr,
            )

    return installed_count


def _install_missing_custom_nodes_from_catalog(
    requirements: list[MissingNodeRequirement],
    mapping_payload: object,
    list_payload: object,
) -> int:
    if not requirements:
        return 0

    installed_count = 0
    attempted_repo_urls: set[str] = set()
    for requirement in requirements:
        candidate_urls = _find_repo_urls_for_node_class(requirement.class_type, mapping_payload)
        candidate_urls.update(_find_repo_urls_for_node_class(requirement.class_type, list_payload))
        if not candidate_urls:
            package_ids = _find_package_ids_for_node_class(requirement.class_type, mapping_payload)
            candidate_urls.update(_find_repo_urls_for_package_ids(package_ids, list_payload))
        preferred_urls = NODE_CLASS_REPO_OVERRIDES.get(requirement.class_type, [])
        if not candidate_urls and not preferred_urls:
            continue

        ordered_candidate_urls = [*preferred_urls]
        ordered_candidate_urls.extend(
            sorted(url for url in candidate_urls if url not in set(preferred_urls))
        )

        for repo_url in ordered_candidate_urls:
            if repo_url in attempted_repo_urls:
                continue
            attempted_repo_urls.add(repo_url)
            try:
                if _install_custom_node_by_git_clone(repo_url):
                    _install_custom_node_python_dependencies(repo_url)
                    _install_custom_node_override_packages(requirement.class_type)
                    installed_count += 1
                    print(
                        f"[bootstrap] prestart installed custom node class_type={requirement.class_type} repo={repo_url}",
                        file=sys.stderr,
                    )
                    break
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[bootstrap] prestart custom node install failed repo={repo_url}: {exc}",
                    file=sys.stderr,
                )

    return installed_count


def _is_custom_node_repo_installed(repo_url: str) -> bool:
    repo_name = _repo_dir_name(repo_url)
    if not repo_name:
        return False
    return (Path("/opt/comfy/custom_nodes") / repo_name).exists()


def _resolve_missing_custom_nodes_from_catalog(
    requirements: list[MissingNodeRequirement],
    mapping_payload: object,
    list_payload: object,
) -> tuple[int, list[dict]]:
    if not requirements:
        return 0, []

    installed_count = 0
    unresolved: list[dict] = []
    attempted_repo_urls: set[str] = set()
    for requirement in requirements:
        candidate_urls = _find_repo_urls_for_node_class(requirement.class_type, mapping_payload)
        candidate_urls.update(_find_repo_urls_for_node_class(requirement.class_type, list_payload))
        if not candidate_urls:
            package_ids = _find_package_ids_for_node_class(requirement.class_type, mapping_payload)
            candidate_urls.update(_find_repo_urls_for_package_ids(package_ids, list_payload))
        preferred_urls = NODE_CLASS_REPO_OVERRIDES.get(requirement.class_type, [])
        ordered_candidate_urls = [*preferred_urls]
        ordered_candidate_urls.extend(
            sorted(url for url in candidate_urls if url not in set(preferred_urls))
        )

        if not ordered_candidate_urls:
            unresolved.append(
                {
                    "class_type": requirement.class_type,
                    "reason": "no_catalog_match",
                }
            )
            continue

        resolved = False
        for repo_url in ordered_candidate_urls:
            if _is_custom_node_repo_installed(repo_url):
                resolved = True
                break

            if repo_url in attempted_repo_urls:
                continue
            attempted_repo_urls.add(repo_url)
            try:
                if _install_custom_node_by_git_clone(repo_url):
                    _install_custom_node_python_dependencies(repo_url)
                    _install_custom_node_override_packages(requirement.class_type)
                    installed_count += 1
                    resolved = True
                    print(
                        f"[bootstrap] prestart installed custom node class_type={requirement.class_type} repo={repo_url}",
                        file=sys.stderr,
                    )
                    break
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[bootstrap] prestart custom node install failed repo={repo_url}: {exc}",
                    file=sys.stderr,
                )

        if not resolved:
            unresolved.append(
                {
                    "class_type": requirement.class_type,
                    "reason": "install_failed",
                    "candidate_repos": ordered_candidate_urls,
                }
            )

    return installed_count, unresolved


def _target_model_dir(requirement: MissingModelRequirement, external_type: str) -> str:
    if requirement.class_type:
        override_key = (requirement.class_type, requirement.input_name)
        if override_key in NODE_CLASS_INPUT_MODEL_DIR_OVERRIDES:
            return NODE_CLASS_INPUT_MODEL_DIR_OVERRIDES[override_key]
    if requirement.input_name in MODEL_DIR_BY_INPUT_NAME:
        return MODEL_DIR_BY_INPUT_NAME[requirement.input_name]
    if external_type in MODEL_DIR_BY_TYPE:
        return MODEL_DIR_BY_TYPE[external_type]
    return "checkpoints"


def _download_file(url: str, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_suffix(target_path.suffix + ".part")
    headers: dict[str, str] = {}
    if "huggingface.co" in url:
        hf_token = (
            os.getenv("HUGGINGFACE_TOKEN", "").strip()
            or os.getenv("HF_TOKEN", "").strip()
            or os.getenv("HUGGING_FACE_HUB_TOKEN", "").strip()
        )
        if hf_token:
            headers["Authorization"] = f"Bearer {hf_token}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=600) as response:
        with tmp_path.open("wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    tmp_path.replace(target_path)


def _catalog_filename_candidates(value: str) -> set[str]:
    trimmed = value.strip().replace("\\", "/")
    if not trimmed:
        return set()
    candidates = {trimmed}
    candidates.add(Path(trimmed).name)
    return {candidate for candidate in candidates if candidate}


def _resolve_model_target_relative_path(requirement_filename: str, catalog_filename: str) -> Path:
    preferred = requirement_filename.strip().replace("\\", "/")
    candidate = preferred if preferred else catalog_filename.strip().replace("\\", "/")
    relative_path = Path(candidate)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        return Path(Path(catalog_filename).name)
    return relative_path


def _fetch_manager_default_model_list() -> object:
    url = "https://raw.githubusercontent.com/Comfy-Org/ComfyUI-Manager/main/model-list.json"
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode("utf-8")
    return json.loads(body or "{}")


def _install_missing_models(
    comfy_client: ComfyClient,
    requirements: list[MissingModelRequirement],
    cache_models_root: Path,
) -> int:
    if not requirements:
        return 0

    entries: list[dict] = []
    try:
        external_payload = comfy_client.get_external_models()
        entries = _iter_model_entries(external_payload)
        if entries:
            print(
                f"[bootstrap] manager external catalog entries={len(entries)}",
                file=sys.stderr,
            )
    except ComfyClientError as exc:
        print(
            f"[bootstrap] manager external catalog unavailable: {exc}",
            file=sys.stderr,
        )

    if not entries:
        try:
            fallback_payload = _fetch_manager_default_model_list()
            entries = _iter_model_entries(fallback_payload)
            if entries:
                print(
                    f"[bootstrap] fallback model catalog entries={len(entries)}",
                    file=sys.stderr,
                )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[bootstrap] fallback model catalog unavailable: {exc}",
                file=sys.stderr,
            )

    if not entries:
        print("[bootstrap] no model catalog entries available", file=sys.stderr)
        return 0

    return _install_missing_models_from_entries(
        requirements=requirements,
        entries=entries,
        cache_models_root=cache_models_root,
    )


def _install_missing_models_from_entries(
    requirements: list[MissingModelRequirement],
    entries: list[dict],
    cache_models_root: Path,
) -> int:
    if not requirements or not entries:
        return 0

    installed_count = 0
    for requirement in requirements:
        requirement_candidates = _catalog_filename_candidates(requirement.filename)
        selected = None
        for item in entries:
            item_candidates = _catalog_filename_candidates(item["filename"])
            if requirement_candidates.intersection(item_candidates):
                selected = item
                break
        if not selected:
            print(
                f"[bootstrap] no catalog match for required model filename={requirement.filename}",
                file=sys.stderr,
            )
            continue

        model_dir = _target_model_dir(requirement, selected.get("type", ""))
        relative_path = _resolve_model_target_relative_path(
            requirement_filename=requirement.filename,
            catalog_filename=selected["filename"],
        )
        target_path = cache_models_root / model_dir / relative_path
        if target_path.exists():
            continue

        try:
            _download_file(selected["url"], target_path)
            installed_count += 1
            print(
                f"[bootstrap] installed model filename={requirement.filename} into {model_dir}",
                file=sys.stderr,
            )
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and "huggingface.co" in selected["url"]:
                print(
                    f"[bootstrap] failed to install model filename={requirement.filename}: "
                    "401 Unauthorized from Hugging Face (set HUGGINGFACE_TOKEN/HF_TOKEN)",
                    file=sys.stderr,
                )
            else:
                print(
                    f"[bootstrap] failed to install model filename={requirement.filename}: HTTP {exc.code}",
                    file=sys.stderr,
                )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[bootstrap] failed to install model filename={requirement.filename}: {exc}",
                file=sys.stderr,
            )

    return installed_count


def _resolve_missing_models_from_entries(
    requirements: list[MissingModelRequirement],
    entries: list[dict],
    cache_models_root: Path,
) -> tuple[int, list[dict]]:
    if not requirements:
        return 0, []
    if not entries:
        unresolved = [
            {
                "class_type": requirement.class_type,
                "input_name": requirement.input_name,
                "filename": requirement.filename,
                "reason": "no_model_catalog_entries",
            }
            for requirement in requirements
        ]
        return 0, unresolved

    installed_count = 0
    unresolved: list[dict] = []
    for requirement in requirements:
        requirement_candidates = _catalog_filename_candidates(requirement.filename)
        selected = None
        for item in entries:
            item_candidates = _catalog_filename_candidates(item["filename"])
            if requirement_candidates.intersection(item_candidates):
                selected = item
                break
        if not selected:
            unresolved.append(
                {
                    "class_type": requirement.class_type,
                    "input_name": requirement.input_name,
                    "filename": requirement.filename,
                    "reason": "no_catalog_match",
                }
            )
            continue

        model_dir = _target_model_dir(requirement, selected.get("type", ""))
        relative_path = _resolve_model_target_relative_path(
            requirement_filename=requirement.filename,
            catalog_filename=selected["filename"],
        )
        target_path = cache_models_root / model_dir / relative_path
        if target_path.exists():
            continue

        try:
            _download_file(selected["url"], target_path)
            installed_count += 1
            print(
                f"[bootstrap] prestart installed model filename={requirement.filename} into {model_dir}",
                file=sys.stderr,
            )
        except Exception as exc:  # noqa: BLE001
            unresolved.append(
                {
                    "class_type": requirement.class_type,
                    "input_name": requirement.input_name,
                    "filename": requirement.filename,
                    "reason": "download_failed",
                    "error": str(exc),
                }
            )

    return installed_count, unresolved


def _prestart_resolve_artifacts_or_error(
    preflight_payload: dict,
    cache_models_root: Path,
) -> dict | None:
    try:
        artifact_specs = _load_app_artifact_specs_from_env()
    except Exception as exc:  # noqa: BLE001
        return _json_error_payload(
            stage="artifact_config",
            message="Failed to parse app artifact configuration",
            details={"error": str(exc)},
        )

    if not artifact_specs:
        return None

    custom_node_specs = [item for item in artifact_specs if item.kind == "custom_node"]
    model_specs = [item for item in artifact_specs if item.kind != "custom_node"]

    unresolved_custom_nodes: list[dict] = []
    for spec in custom_node_specs:
        repo_url = spec.source_url
        repo_name = spec.target_path.strip() or _repo_dir_name(repo_url)
        node_dir = Path("/opt/comfy/custom_nodes") / repo_name
        if node_dir.exists():
            continue
        try:
            if _install_custom_node_by_git_clone(repo_url, ref=spec.ref or "main"):
                _install_custom_node_python_dependencies(repo_url)
                print(
                    f"[bootstrap] prestart installed custom node repo={repo_url}",
                    file=sys.stderr,
                )
            if not node_dir.exists():
                unresolved_custom_nodes.append(
                    {
                        "kind": "custom_node",
                        "source_url": repo_url,
                        "reason": "missing_after_install",
                        "expected_path": str(node_dir),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            unresolved_custom_nodes.append(
                {
                    "kind": "custom_node",
                    "source_url": repo_url,
                    "reason": "install_failed",
                    "error": str(exc),
                }
            )
    if unresolved_custom_nodes:
        return _json_error_payload(
            stage="custom_node_resolution",
            message="Failed to resolve required custom node artifacts before startup",
            details={"unresolved_custom_nodes": unresolved_custom_nodes},
        )

    known_model_requirements = _known_model_requirements_from_prompt(preflight_payload)
    unresolved_models: list[dict] = []
    resolved_items: list[tuple[MissingModelRequirement, ArtifactSourceSpec]] = []
    for requirement in known_model_requirements:
        requirement_candidates = _catalog_filename_candidates(requirement.filename)
        selected = None
        for spec in model_specs:
            if requirement_candidates.intersection(_artifact_candidates_from_spec(spec)):
                selected = spec
                break
        if not selected:
            unresolved_models.append(
                {
                    "class_type": requirement.class_type,
                    "input_name": requirement.input_name,
                    "filename": requirement.filename,
                    "reason": "required_model_not_declared_in_app_artifacts",
                }
            )
            continue
        resolved_items.append((requirement, selected))

    source_failures: list[dict] = []
    installed_count = 0
    for requirement, spec in resolved_items:
        target_path = cache_models_root / spec.target_subdir / spec.target_path
        if target_path.exists():
            continue
        try:
            _download_file(spec.source_url, target_path)
            installed_count += 1
            print(
                f"[bootstrap] prestart installed model filename={requirement.filename} into {spec.target_subdir}",
                file=sys.stderr,
            )
        except Exception as exc:  # noqa: BLE001
            source_failures.append(
                {
                    "class_type": requirement.class_type,
                    "input_name": requirement.input_name,
                    "filename": requirement.filename,
                    "source_url": spec.source_url,
                    "reason": "download_failed",
                    "error": str(exc),
                }
            )

    if source_failures:
        unresolved_models.extend(source_failures)

    for spec in model_specs:
        target_path = cache_models_root / spec.target_subdir / spec.target_path
        if not target_path.exists():
            unresolved_models.append(
                {
                    "kind": "model",
                    "match": spec.match,
                    "source_url": spec.source_url,
                    "reason": "missing_on_disk_after_resolution",
                    "expected_path": str(target_path),
                }
            )

    if unresolved_models:
        return _json_error_payload(
            stage="model_resolution",
            message="Failed to resolve required models from app-defined artifacts before startup",
            details={
                "unresolved_models": unresolved_models,
                "configured_artifact_count": len(artifact_specs),
            },
        )

    for spec in custom_node_specs:
        repo_name = spec.target_path.strip() or _repo_dir_name(spec.source_url)
        node_dir = Path("/opt/comfy/custom_nodes") / repo_name
        if not node_dir.exists():
            return _json_error_payload(
                stage="artifact_verification",
                message="Custom node artifact missing on disk before startup",
                details={"source_url": spec.source_url, "expected_path": str(node_dir)},
            )
    if installed_count > 0:
        print(f"[bootstrap] prestart installed models count={installed_count}", file=sys.stderr)
    return None


def _run_artifact_failure_endpoint(
    listen_host: str,
    listen_port: int,
    api_key: str,
    error_payload: dict,
) -> int:
    class ArtifactFailureHandler(BaseHTTPRequestHandler):
        def _json_response(self, status: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            return self.headers.get("x-api-key", "") == api_key

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "ok": False,
                        "status": "artifact_resolver_failed",
                    },
                )
                return

            if self.path == "/artifact-resolver/error":
                if not self._authorized():
                    self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                    return
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, error_payload)
                return

            self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path == "/run":
                if not self._authorized():
                    self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                    return
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, error_payload)
                return
            self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    server = ThreadingHTTPServer((listen_host, listen_port), ArtifactFailureHandler)
    print(
        f"[bootstrap] artifact resolver failed; serving error endpoint on http://{listen_host}:{listen_port}",
        file=sys.stderr,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


def _move_dir_contents(source_dir: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for entry in source_dir.iterdir():
        target_path = target_dir / entry.name
        if target_path.exists():
            continue
        shutil.move(str(entry), str(target_path))


def _ensure_model_roots_on_cache(
    comfy_models_root: Path,
    cache_models_root: Path,
) -> None:
    cache_models_root.mkdir(parents=True, exist_ok=True)
    comfy_models_root.mkdir(parents=True, exist_ok=True)
    for model_subdir in sorted(MODEL_SUBDIRS):
        cache_target_dir = cache_models_root / model_subdir
        cache_target_dir.mkdir(parents=True, exist_ok=True)
        comfy_dir = comfy_models_root / model_subdir

        if comfy_dir.is_symlink():
            symlink_target = comfy_dir.resolve(strict=False)
            if symlink_target == cache_target_dir:
                continue
            comfy_dir.unlink()
            comfy_dir.symlink_to(cache_target_dir, target_is_directory=True)
            continue

        if comfy_dir.exists():
            if comfy_dir.is_dir():
                _move_dir_contents(comfy_dir, cache_target_dir)
                shutil.rmtree(comfy_dir)
            else:
                raise RuntimeError(f"Model path is not a directory: {comfy_dir}")

        comfy_dir.symlink_to(cache_target_dir, target_is_directory=True)


def run_bootstrap(
    cache_root: Path,
    watch_paths: list[Path],
    min_file_size_mb: int,
    contract_path: Path,
    workflow_path: Path,
    api_key: str,
    gateway_port: int,
    app_id: str | None = None,
) -> int:
    ensure_contract_file(contract_path)
    ensure_workflow_file(workflow_path)

    contract = parse_workflow_contract(contract_path)
    workflow_payload = json.loads(workflow_path.read_text(encoding="utf-8"))
    preflight_payload = build_preflight_payload(workflow_payload, contract)

    cache_models_root = Path(
        os.getenv("COMFY_ENDPOINTS_CACHE_MODEL_ROOT", str(cache_root / "models")).strip()
    )
    comfy_models_root = Path(
        os.getenv("COMFY_ENDPOINTS_COMFY_MODELS_ROOT", "/opt/comfy/models").strip()
    )
    _ensure_model_roots_on_cache(
        comfy_models_root=comfy_models_root,
        cache_models_root=cache_models_root,
    )

    manager = CacheManager(
        cache_root=cache_root,
        watch_paths=watch_paths,
        min_file_size_mb=min_file_size_mb,
    )
    manager.reconcile()

    resolver_error = _prestart_resolve_artifacts_or_error(
        preflight_payload=preflight_payload,
        cache_models_root=cache_models_root,
    )
    if resolver_error:
        return _run_artifact_failure_endpoint(
            listen_host="0.0.0.0",
            listen_port=gateway_port,
            api_key=api_key,
            error_payload=resolver_error,
        )

    comfy_command = os.getenv(
        "COMFY_START_COMMAND",
        "python /opt/comfy/main.py --listen 127.0.0.1 --port 8188 --disable-auto-launch --enable-manager",
    )

    gateway_command = (
        "python -m comfy_endpoints.gateway.server "
        f"--listen-host 0.0.0.0 --listen-port {gateway_port} "
        f"--api-key {shlex.quote(api_key)} "
        f"--contract-path {shlex.quote(str(contract_path))} "
        f"--workflow-path {shlex.quote(str(workflow_path))} "
        "--comfy-url http://127.0.0.1:8188 "
        f"--state-db {shlex.quote(os.getenv('COMFY_ENDPOINTS_STATE_DB', '/opt/comfy_endpoints/runtime/jobs.db'))}"
    )
    if app_id:
        gateway_command = f"{gateway_command} --app-id {shlex.quote(app_id)}"

    def start_comfy_process() -> subprocess.Popen[bytes]:
        return subprocess.Popen(shlex.split(comfy_command))

    comfy_process = start_comfy_process()
    gateway_process = None

    def shutdown(_sig: int, _frame: object) -> None:
        for process in (gateway_process, comfy_process):
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        comfy_url = "http://127.0.0.1:8188"
        wait_for_comfy_ready(comfy_url)
        comfy_client = ComfyClient(comfy_url)
        try:
            preflight_prompt_id = comfy_client.queue_prompt(preflight_payload)
            print(
                f"[bootstrap] comfy preflight queue passed prompt_id={preflight_prompt_id}",
                file=sys.stderr,
            )
        except ComfyClientError as exc:
            details: dict[str, object] = {}
            missing_nodes = _missing_nodes_from_preflight_error(exc)
            if missing_nodes:
                details["missing_nodes"] = [item.class_type for item in missing_nodes]
            missing_models = _missing_models_from_preflight_error(exc)
            if missing_models:
                details["missing_models"] = [
                    {
                        "class_type": item.class_type,
                        "input_name": item.input_name,
                        "filename": item.filename,
                    }
                    for item in missing_models
                ]
            raise RuntimeError(
                f"Comfy preflight queue failed after prestart artifact resolution: {exc}; details={details}"
            ) from exc
    except ComfyClientError as exc:
        if comfy_process.poll() is None:
            comfy_process.terminate()
        raise RuntimeError(f"Comfy preflight queue failed: {exc}") from exc
    except Exception:
        if comfy_process.poll() is None:
            comfy_process.terminate()
        raise

    gateway_process = subprocess.Popen(shlex.split(gateway_command))

    while True:
        comfy_status = comfy_process.poll()
        gateway_status = gateway_process.poll() if gateway_process else None

        if comfy_status is not None:
            if gateway_process and gateway_process.poll() is None:
                gateway_process.terminate()
            return comfy_status

        if gateway_status is not None:
            if comfy_process.poll() is None:
                comfy_process.terminate()
            return gateway_status


def main() -> int:
    parser = argparse.ArgumentParser(prog="comfy-endpoints-bootstrap")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--watch-paths", required=True)
    parser.add_argument("--min-file-size-mb", type=int, default=100)
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--workflow-path", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--gateway-port", type=int, default=3000)
    parser.add_argument("--app-id", default=None)
    args = parser.parse_args()

    return run_bootstrap(
        cache_root=Path(args.cache_root),
        watch_paths=[Path(item) for item in _split_csv(args.watch_paths)],
        min_file_size_mb=args.min_file_size_mb,
        contract_path=Path(args.contract_path),
        workflow_path=Path(args.workflow_path),
        api_key=args.api_key,
        gateway_port=args.gateway_port,
        app_id=str(args.app_id).strip() if args.app_id else None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
