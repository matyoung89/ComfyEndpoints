from __future__ import annotations

from pathlib import Path

from comfy_endpoints.models import AppSpecV1
from comfy_endpoints.runtime.image_fingerprint import compute_image_fingerprint


def resolve_golden_image(app_spec: AppSpecV1) -> str:
    if app_spec.build.image_ref:
        return app_spec.build.image_ref

    repository = app_spec.build.image_repository or "ghcr.io/comfy-endpoints/golden"
    dockerfile_path = Path(app_spec.build.dockerfile_path or "docker/Dockerfile.golden")
    if not dockerfile_path.is_absolute():
        dockerfile_path = Path(__file__).resolve().parents[3] / dockerfile_path
    dockerfile_contents = dockerfile_path.read_text(encoding="utf-8")
    fingerprint = compute_image_fingerprint(app_spec, dockerfile_contents)
    tag = f"{app_spec.build.comfy_version}-{app_spec.version}-{fingerprint}"
    return f"{repository}:{tag}"
