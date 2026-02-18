from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from comfy_endpoints.contracts.validators import validate_deployable_spec
from comfy_endpoints.deploy.bootstrap import (
    _catalog_filename_candidates,
    _known_model_requirements_from_prompt,
    _required_nodes_from_prompt,
)
from comfy_endpoints.gateway.prompt_mapper import build_preflight_payload
from comfy_endpoints.models import ArtifactSourceSpec

MODEL_SUBDIRS = {
    "checkpoints",
    "diffusion_models",
    "text_encoders",
    "vae",
    "loras",
    "controlnet",
}

PLATFORM_PROVIDED_NODE_CLASSES = {
    "ApiInput",
    "ApiOutput",
    "PathToImageTensor",
    "PathToVideoTensor",
    "VideoTensorToPath",
}


@dataclass(slots=True)
class ArtifactValidationResult:
    ok: bool
    errors: list[dict]
    warnings: list[dict]
    matched_models: list[dict]
    source_urls: list[str]

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "errors": self.errors,
            "warnings": self.warnings,
            "matched_models": self.matched_models,
            "source_urls": self.source_urls,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def _artifact_candidates(item: ArtifactSourceSpec) -> set[str]:
    candidates = _catalog_filename_candidates(item.match)
    candidates.update(_catalog_filename_candidates(item.target_path))
    return {value for value in candidates if value}


def _probe_source_url(url: str) -> tuple[bool, str]:
    head_req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(head_req, timeout=30) as response:
            return True, f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        if exc.code in {403, 405, 429, 500, 501}:
            pass
        else:
            return False, f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        return False, f"URL error: {exc.reason}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)

    get_req = urllib.request.Request(url, headers={"Range": "bytes=0-0"}, method="GET")
    try:
        with urllib.request.urlopen(get_req, timeout=30) as response:
            return True, f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        if exc.code in {200, 206, 416}:
            return True, f"HTTP {exc.code}"
        return False, f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        return False, f"URL error: {exc.reason}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def validate_preflight_artifacts(
    preflight_payload: dict,
    artifact_specs: list[ArtifactSourceSpec],
) -> ArtifactValidationResult:
    errors: list[dict] = []
    warnings: list[dict] = []
    matched_models: list[dict] = []

    requirements = _known_model_requirements_from_prompt(preflight_payload)
    required_nodes = _required_nodes_from_prompt(preflight_payload)

    provided_nodes: set[str] = set()
    for item in artifact_specs:
        if item.kind == "custom_node":
            provided_nodes.update(item.provides)
    for node in required_nodes:
        if node.class_type in PLATFORM_PROVIDED_NODE_CLASSES:
            continue
        if node.class_type in provided_nodes:
            continue
        warnings.append(
            {
                "type": "node_provider_not_declared",
                "class_type": node.class_type,
            }
        )

    if not artifact_specs and requirements:
        errors.append(
            {
                "type": "no_artifacts_defined",
                "message": "App spec must include artifacts for required model inputs",
            }
        )
        return ArtifactValidationResult(
            ok=False,
            errors=errors,
            warnings=warnings,
            matched_models=matched_models,
            source_urls=[],
        )
    if not artifact_specs and not requirements:
        return ArtifactValidationResult(
            ok=True,
            errors=[],
            warnings=[],
            matched_models=[],
            source_urls=[],
        )

    source_urls = [item.source_url for item in artifact_specs]

    for index, item in enumerate(artifact_specs):
        if item.kind == "model" and item.target_subdir not in MODEL_SUBDIRS:
            errors.append(
                {
                    "type": "invalid_target_subdir",
                    "artifact_index": index,
                    "target_subdir": item.target_subdir,
                }
            )
            continue
        if not item.match or not item.source_url or not item.target_path:
            errors.append(
                {
                    "type": "invalid_artifact_entry",
                    "artifact_index": index,
                    "message": "match/source_url/target_path must be non-empty",
                }
            )
            continue

        ok, detail = _probe_source_url(item.source_url)
        if not ok:
            errors.append(
                {
                    "type": "artifact_source_unreachable",
                    "artifact_index": index,
                    "source_url": item.source_url,
                    "detail": detail,
                }
            )

    for requirement in requirements:
        requirement_candidates = _catalog_filename_candidates(requirement.filename)
        selected = None
        for item in artifact_specs:
            if requirement_candidates.intersection(_artifact_candidates(item)):
                selected = item
                break
        if not selected:
            errors.append(
                {
                    "type": "required_model_not_declared",
                    "class_type": requirement.class_type,
                    "input_name": requirement.input_name,
                    "filename": requirement.filename,
                }
            )
            continue
        matched_models.append(
            {
                "class_type": requirement.class_type,
                "input_name": requirement.input_name,
                "requested_filename": requirement.filename,
                "artifact_match": selected.match,
                "target_subdir": selected.target_subdir,
                "target_path": selected.target_path,
                "source_url": selected.source_url,
            }
        )

    return ArtifactValidationResult(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        matched_models=matched_models,
        source_urls=source_urls,
    )


def validate_artifacts_for_app_spec(app_spec_path: Path) -> ArtifactValidationResult:
    app_spec, contract = validate_deployable_spec(app_spec_path)
    workflow_payload = json.loads(app_spec.workflow_path.read_text(encoding="utf-8"))
    preflight_payload = build_preflight_payload(workflow_payload, contract)
    return validate_preflight_artifacts(
        preflight_payload=preflight_payload,
        artifact_specs=app_spec.artifacts,
    )
