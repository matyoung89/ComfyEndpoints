from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from comfy_endpoints.contracts.validators import parse_workflow_contract
from comfy_endpoints.deploy.cache_manager import CacheManager
from comfy_endpoints.gateway.comfy_client import ComfyClient, ComfyClientError
from comfy_endpoints.gateway.prompt_mapper import build_preflight_payload


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


MISSING_MODEL_PATTERN = re.compile(
    r"Value not in list:\s*(?P<input_name>[A-Za-z0-9_]+)\s*:\s*'(?P<filename>[^']+)'",
)

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


@dataclass(slots=True)
class MissingModelRequirement:
    input_name: str
    filename: str


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
            parsed.append(MissingModelRequirement(input_name=input_name, filename=filename))
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

            key = (input_name, value)
            if key in seen:
                continue
            seen.add(key)
            parsed.append(MissingModelRequirement(input_name=input_name, filename=value))

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
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue

        for input_name, value in inputs.items():
            if input_name not in MODEL_DIR_BY_INPUT_NAME:
                continue
            if not isinstance(value, str) or not value.strip():
                continue
            filename = value.strip()
            key = (input_name, filename)
            if key in seen:
                continue
            seen.add(key)
            parsed.append(MissingModelRequirement(input_name=input_name, filename=filename))

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


def _target_model_dir(requirement: MissingModelRequirement, external_type: str) -> str:
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


def _fetch_manager_default_model_list() -> object:
    url = "https://raw.githubusercontent.com/Comfy-Org/ComfyUI-Manager/main/model-list.json"
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode("utf-8")
    return json.loads(body or "{}")


def _install_missing_models(
    comfy_client: ComfyClient,
    requirements: list[MissingModelRequirement],
    comfy_root: Path,
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

    installed_count = 0
    for requirement in requirements:
        selected = None
        for item in entries:
            if item["filename"] == requirement.filename:
                selected = item
                break
        if not selected:
            print(
                f"[bootstrap] no catalog match for required model filename={requirement.filename}",
                file=sys.stderr,
            )
            continue

        model_dir = _target_model_dir(requirement, selected.get("type", ""))
        target_path = comfy_root / "models" / model_dir / requirement.filename
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

    manager = CacheManager(
        cache_root=cache_root,
        watch_paths=watch_paths,
        min_file_size_mb=min_file_size_mb,
    )
    manager.reconcile()

    comfy_command = os.getenv(
        "COMFY_START_COMMAND",
        "python /opt/comfy/main.py --listen 127.0.0.1 --port 8188 --disable-auto-launch --enable-manager --disable-manager-ui",
    )

    gateway_command = (
        "python -m comfy_endpoints.gateway.server "
        f"--listen-host 0.0.0.0 --listen-port {gateway_port} "
        f"--api-key {shlex.quote(api_key)} "
        f"--contract-path {shlex.quote(str(contract_path))} "
        f"--workflow-path {shlex.quote(str(workflow_path))} "
        "--comfy-url http://127.0.0.1:8188"
    )
    if app_id:
        gateway_command = f"{gateway_command} --app-id {shlex.quote(app_id)}"

    comfy_process = subprocess.Popen(shlex.split(comfy_command))
    gateway_process = None

    def shutdown(_sig: int, _frame: object) -> None:
        for process in (gateway_process, comfy_process):
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        wait_for_comfy_ready("http://127.0.0.1:8188")
        preflight_payload = build_preflight_payload(workflow_payload, contract)
        comfy_client = ComfyClient("http://127.0.0.1:8188")
        max_preflight_attempts = int(os.getenv("COMFY_ENDPOINTS_PREFLIGHT_MAX_ATTEMPTS", "8"))
        object_info_payload: dict | None = None
        for attempt in range(1, max_preflight_attempts + 1):
            try:
                preflight_prompt_id = comfy_client.queue_prompt(preflight_payload)
                print(
                    f"[bootstrap] comfy preflight queue passed prompt_id={preflight_prompt_id}",
                    file=sys.stderr,
                )
                break
            except ComfyClientError as exc:
                missing_models = _missing_models_from_preflight_error(exc)
                if not missing_models:
                    if object_info_payload is None:
                        try:
                            object_info_payload = comfy_client.get_object_info()
                        except ComfyClientError:
                            object_info_payload = {}
                    missing_models = _missing_models_from_object_info(
                        prompt_payload=preflight_payload,
                        object_info_payload=object_info_payload,
                    )

                known_requirements = _known_model_requirements_from_prompt(preflight_payload)
                if known_requirements:
                    dedup: dict[tuple[str, str], MissingModelRequirement] = {
                        (item.input_name, item.filename): item for item in missing_models
                    }
                    for item in known_requirements:
                        dedup.setdefault((item.input_name, item.filename), item)
                    missing_models = list(dedup.values())
                if missing_models:
                    print(
                        "[bootstrap] detected missing models: "
                        + ", ".join(f"{item.input_name}={item.filename}" for item in missing_models),
                        file=sys.stderr,
                    )
                if not missing_models or attempt == max_preflight_attempts:
                    raise

                installed = _install_missing_models(
                    comfy_client=comfy_client,
                    requirements=missing_models,
                    comfy_root=Path("/opt/comfy"),
                )
                if installed <= 0:
                    raise

                print(
                    f"[bootstrap] preflight retry attempt={attempt + 1} after installing {installed} model(s)",
                    file=sys.stderr,
                )
                time.sleep(2)
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
