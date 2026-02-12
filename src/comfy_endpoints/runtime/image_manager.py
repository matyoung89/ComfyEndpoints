from __future__ import annotations

import base64
import json
import os
import shlex
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from comfy_endpoints.models import AppSpecV1
from comfy_endpoints.runtime.image_resolver import resolve_golden_image


class ImageBuildError(RuntimeError):
    pass


@dataclass(slots=True)
class ImageResolution:
    image_ref: str
    image_exists: bool
    built: bool


class ImageManager:
    def __init__(self, project_root: Path | None = None):
        self.project_root = project_root or Path(__file__).resolve().parents[3]

    def ensure_image(self, app_spec: AppSpecV1) -> ImageResolution:
        image_ref = resolve_golden_image(app_spec)
        if self._image_exists(image_ref):
            return ImageResolution(image_ref=image_ref, image_exists=True, built=False)

        self._build_and_push(app_spec, image_ref)
        self._wait_for_image(image_ref)

        return ImageResolution(image_ref=image_ref, image_exists=True, built=True)

    def _build_and_push(self, app_spec: AppSpecV1, image_ref: str) -> None:
        backend = os.getenv("COMFY_ENDPOINTS_IMAGE_BUILD_BACKEND", "auto").strip().lower()
        if backend not in {"auto", "local", "github_actions"}:
            raise ImageBuildError(
                "COMFY_ENDPOINTS_IMAGE_BUILD_BACKEND must be one of: auto, local, github_actions"
            )

        if backend == "local":
            self._build_and_push_local(app_spec, image_ref)
            return

        if backend == "github_actions":
            self._dispatch_github_actions_build(app_spec, image_ref)
            return

        if self._docker_available():
            self._build_and_push_local(app_spec, image_ref)
            return

        self._dispatch_github_actions_build(app_spec, image_ref)

    def _wait_for_image(self, image_ref: str) -> None:
        timeout_seconds = int(os.getenv("COMFY_ENDPOINTS_IMAGE_BUILD_TIMEOUT_SECONDS", "1800"))
        poll_seconds = int(os.getenv("COMFY_ENDPOINTS_IMAGE_BUILD_POLL_SECONDS", "15"))
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            if self._image_exists(image_ref):
                return
            time.sleep(poll_seconds)

        raise ImageBuildError(
            f"Image not available before timeout ({timeout_seconds}s): {image_ref}"
        )

    def _docker_available(self) -> bool:
        return shutil.which("docker") is not None

    def _image_exists(self, image_ref: str) -> bool:
        if self._docker_available():
            command = ["docker", "manifest", "inspect", image_ref]
            result = subprocess.run(command, cwd=self.project_root, capture_output=True, text=True)
            return result.returncode == 0

        return self._registry_manifest_exists(image_ref)

    def _registry_manifest_exists(self, image_ref: str) -> bool:
        if not image_ref.startswith("ghcr.io/"):
            raise ImageBuildError(
                "Docker is unavailable and only ghcr.io registry checks are implemented. "
                f"Unsupported image: {image_ref}"
            )

        name_tag = image_ref.removeprefix("ghcr.io/")
        if ":" not in name_tag:
            raise ImageBuildError(f"Image ref missing tag: {image_ref}")

        image_name, tag = name_tag.rsplit(":", 1)
        req = urllib.request.Request(
            f"https://ghcr.io/v2/{image_name}/manifests/{tag}",
            headers={
                "Accept": "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json",
            },
            method="HEAD",
        )

        auth_header = self._ghcr_basic_auth_header()
        if auth_header:
            req.add_header("Authorization", auth_header)

        try:
            with urllib.request.urlopen(req, timeout=20):
                return True
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403, 404}:
                return False
            raise ImageBuildError(f"Unexpected GHCR status while checking image: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ImageBuildError(f"Failed reaching GHCR for image check: {exc.reason}") from exc

    def _ghcr_basic_auth_header(self) -> str | None:
        username = os.getenv("GHCR_USERNAME", "").strip()
        token = os.getenv("GHCR_TOKEN", "").strip()
        if not username or not token:
            return None

        pair = f"{username}:{token}".encode("utf-8")
        return f"Basic {base64.b64encode(pair).decode('ascii')}"

    def _build_and_push_local(self, app_spec: AppSpecV1, image_ref: str) -> None:
        self._ensure_registry_login(image_ref)

        dockerfile_path = app_spec.build.dockerfile_path or "docker/Dockerfile.golden"
        build_context = app_spec.build.build_context or "."

        command = [
            "docker",
            "buildx",
            "build",
            "--platform",
            "linux/amd64",
            "-f",
            dockerfile_path,
            "-t",
            image_ref,
            "--push",
            build_context,
        ]

        result = subprocess.run(command, cwd=self.project_root, capture_output=True, text=True)
        if result.returncode != 0:
            raise ImageBuildError(
                "Failed building/pushing golden image. "
                f"Command: {' '.join(shlex.quote(part) for part in command)}\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )

    def _dispatch_github_actions_build(self, app_spec: AppSpecV1, image_ref: str) -> None:
        token = os.getenv("GITHUB_TOKEN", "").strip()
        repository = os.getenv("GITHUB_REPOSITORY", "").strip()
        workflow = os.getenv("COMFY_ENDPOINTS_GHA_WORKFLOW", "build_golden_image.yml").strip()
        ref = os.getenv("COMFY_ENDPOINTS_GHA_REF", "main").strip()

        if not token or not repository:
            raise ImageBuildError(
                "GitHub Actions build backend requires GITHUB_TOKEN and GITHUB_REPOSITORY"
            )

        payload = {
            "ref": ref,
            "inputs": {
                "image_ref": image_ref,
                "dockerfile_path": app_spec.build.dockerfile_path or "docker/Dockerfile.golden",
                "build_context": app_spec.build.build_context or ".",
            },
        }
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repository}/actions/workflows/{workflow}/dispatches",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.status not in {200, 201, 204}:
                    raise ImageBuildError(
                        f"Unexpected GitHub workflow dispatch status: {response.status}"
                    )
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ImageBuildError(
                f"GitHub workflow dispatch failed HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise ImageBuildError(f"GitHub workflow dispatch connection error: {exc.reason}") from exc

    def _ensure_registry_login(self, image_ref: str) -> None:
        if not image_ref.startswith("ghcr.io/"):
            return

        username = os.getenv("GHCR_USERNAME", "").strip()
        token = os.getenv("GHCR_TOKEN", "").strip()
        if not username or not token:
            return

        command = ["docker", "login", "ghcr.io", "-u", username, "--password-stdin"]
        result = subprocess.run(
            command,
            cwd=self.project_root,
            input=token,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise ImageBuildError(
                "Failed docker login for ghcr.io. "
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
