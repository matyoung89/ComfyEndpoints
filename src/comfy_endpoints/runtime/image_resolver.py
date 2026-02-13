from __future__ import annotations

from pathlib import Path

from comfy_endpoints.models import AppSpecV1
from comfy_endpoints.runtime.image_fingerprint import (
    compute_comfybase_fingerprint,
    compute_golden_fingerprint,
)

DEFAULT_COMFYBASE_REPOSITORY = "ghcr.io/comfy-endpoints/comfybase"
DEFAULT_GOLDEN_REPOSITORY = "ghcr.io/comfy-endpoints/golden"


def resolve_comfyui_source(app_spec: AppSpecV1) -> tuple[str, str]:
    for plugin in app_spec.build.plugins:
        if "comfyui" in plugin.repo.lower():
            return plugin.repo, plugin.ref
    return "https://github.com/comfyanonymous/ComfyUI.git", "master"


def _default_base_repository(app_spec: AppSpecV1) -> str:
    if app_spec.build.base_image_repository:
        return app_spec.build.base_image_repository
    if app_spec.build.image_repository:
        return f"{app_spec.build.image_repository}-base"
    return DEFAULT_COMFYBASE_REPOSITORY


def resolve_comfybase_image(app_spec: AppSpecV1) -> str:
    repository = _default_base_repository(app_spec)
    project_root = Path(__file__).resolve().parents[3]
    dockerfile_path = Path(app_spec.build.base_dockerfile_path or "docker/Dockerfile.comfybase")
    if not dockerfile_path.is_absolute():
        dockerfile_path = project_root / dockerfile_path
    dockerfile_contents = dockerfile_path.read_text(encoding="utf-8")
    comfyui_repo, comfyui_ref = resolve_comfyui_source(app_spec)
    fingerprint = compute_comfybase_fingerprint(
        app_spec=app_spec,
        dockerfile_contents=dockerfile_contents,
        project_root=project_root,
        comfyui_repo=comfyui_repo,
        comfyui_ref=comfyui_ref,
    )
    tag = f"{app_spec.build.comfy_version}-base-{fingerprint}"
    return f"{repository}:{tag}"


def resolve_golden_image(app_spec: AppSpecV1, comfybase_image_ref: str) -> str:
    if app_spec.build.image_ref:
        return app_spec.build.image_ref

    repository = app_spec.build.image_repository or DEFAULT_GOLDEN_REPOSITORY
    project_root = Path(__file__).resolve().parents[3]
    dockerfile_path = Path(app_spec.build.dockerfile_path or "docker/Dockerfile.golden")
    if not dockerfile_path.is_absolute():
        dockerfile_path = project_root / dockerfile_path
    dockerfile_contents = dockerfile_path.read_text(encoding="utf-8")
    fingerprint = compute_golden_fingerprint(
        app_spec=app_spec,
        dockerfile_contents=dockerfile_contents,
        project_root=project_root,
        comfybase_image_ref=comfybase_image_ref,
    )
    tag = f"{app_spec.build.comfy_version}-{app_spec.version}-{fingerprint}"
    return f"{repository}:{tag}"
