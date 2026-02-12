from __future__ import annotations

import base64
import calendar
import json
import os
import shlex
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

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

    def ensure_image(
        self,
        app_spec: AppSpecV1,
        progress_callback: Callable[[str], None] | None = None,
    ) -> ImageResolution:
        image_ref = resolve_golden_image(app_spec)
        if progress_callback:
            progress_callback(f"[image] resolved image_ref={image_ref}")
        if self._image_exists(image_ref):
            if progress_callback:
                progress_callback("[image] image already available in registry")
            return ImageResolution(image_ref=image_ref, image_exists=True, built=False)

        if progress_callback:
            progress_callback("[image] image missing; starting build/push")
        self._build_and_push(app_spec, image_ref, progress_callback=progress_callback)
        self._wait_for_image(image_ref, progress_callback=progress_callback)

        return ImageResolution(image_ref=image_ref, image_exists=True, built=True)

    def _build_and_push(
        self,
        app_spec: AppSpecV1,
        image_ref: str,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        backend = os.getenv("COMFY_ENDPOINTS_IMAGE_BUILD_BACKEND", "auto").strip().lower()
        if backend not in {"auto", "local", "github_actions"}:
            raise ImageBuildError(
                "COMFY_ENDPOINTS_IMAGE_BUILD_BACKEND must be one of: auto, local, github_actions"
            )

        if backend == "local":
            if progress_callback:
                progress_callback("[image] backend=local")
            self._build_and_push_local(app_spec, image_ref, progress_callback=progress_callback)
            return

        if backend == "github_actions":
            if progress_callback:
                progress_callback("[image] backend=github_actions")
            self._dispatch_github_actions_build(app_spec, image_ref, progress_callback=progress_callback)
            return

        if self._docker_available():
            if progress_callback:
                progress_callback("[image] backend=auto selected local docker")
            self._build_and_push_local(app_spec, image_ref, progress_callback=progress_callback)
            return

        if progress_callback:
            progress_callback("[image] backend=auto selected github_actions")
        self._dispatch_github_actions_build(app_spec, image_ref, progress_callback=progress_callback)

    def _wait_for_image(
        self,
        image_ref: str,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        timeout_seconds = int(os.getenv("COMFY_ENDPOINTS_IMAGE_BUILD_TIMEOUT_SECONDS", "1800"))
        poll_seconds = int(os.getenv("COMFY_ENDPOINTS_IMAGE_BUILD_POLL_SECONDS", "15"))
        deadline = time.time() + timeout_seconds
        poll_count = 0

        while time.time() < deadline:
            poll_count += 1
            if self._image_exists(image_ref):
                if progress_callback:
                    progress_callback(f"[image] image available after {poll_count} poll(s)")
                return
            if progress_callback:
                remaining = int(deadline - time.time())
                progress_callback(
                    f"[image] waiting for registry availability (poll={poll_count}, remaining={remaining}s)"
                )
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
        bearer_token = self._ghcr_bearer_token(image_name)
        accept_header = ",".join(
            [
                "application/vnd.oci.image.index.v1+json",
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.list.v2+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            ]
        )
        req = urllib.request.Request(
            f"https://ghcr.io/v2/{image_name}/manifests/{tag}",
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "Accept": accept_header,
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                return response.status in {200, 201}
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403, 404}:
                return False
            raise ImageBuildError(f"Unexpected GHCR status while checking image: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ImageBuildError(f"Failed reaching GHCR for image check: {exc.reason}") from exc

    def _ghcr_basic_auth_header(self) -> str | None:
        username = os.getenv("GHCR_USERNAME", "").strip()
        token = os.getenv("GHCR_TOKEN", "").strip()
        if (not username or not token) and os.getenv("GITHUB_TOKEN", "").strip():
            repository = os.getenv("GITHUB_REPOSITORY", "").strip()
            if repository and "/" in repository:
                username = repository.split("/", 1)[0]
                token = os.getenv("GITHUB_TOKEN", "").strip()
        if not username or not token:
            return None

        pair = f"{username}:{token}".encode("utf-8")
        return f"Basic {base64.b64encode(pair).decode('ascii')}"

    def _ghcr_bearer_token(self, image_name: str) -> str:
        basic = self._ghcr_basic_auth_header()
        if not basic:
            raise ImageBuildError(
                "GHCR access requires credentials. Set GHCR_USERNAME/GHCR_TOKEN or "
                "GITHUB_TOKEN/GITHUB_REPOSITORY."
            )

        query = urllib.parse.urlencode(
            {
                "service": "ghcr.io",
                "scope": f"repository:{image_name}:pull",
            }
        )
        req = urllib.request.Request(
            f"https://ghcr.io/token?{query}",
            headers={
                "Authorization": basic,
                "Accept": "application/json",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ImageBuildError(f"Failed GHCR token exchange HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise ImageBuildError(f"Failed GHCR token exchange: {exc.reason}") from exc

        token = payload.get("token") or payload.get("access_token")
        if not token:
            raise ImageBuildError("GHCR token exchange succeeded but token was missing")
        return str(token)

    def _build_and_push_local(
        self,
        app_spec: AppSpecV1,
        image_ref: str,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self._ensure_registry_login(image_ref, progress_callback=progress_callback)

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
        if progress_callback:
            progress_callback(
                "[image] running local buildx push: "
                + " ".join(shlex.quote(part) for part in command)
            )

        result = subprocess.run(command, cwd=self.project_root, capture_output=True, text=True)
        if result.returncode != 0:
            raise ImageBuildError(
                "Failed building/pushing golden image. "
                f"Command: {' '.join(shlex.quote(part) for part in command)}\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        if progress_callback:
            progress_callback("[image] local buildx push completed")

    def _dispatch_github_actions_build(
        self,
        app_spec: AppSpecV1,
        image_ref: str,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
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
        if progress_callback:
            progress_callback(
                f"[image] dispatching github workflow {workflow} on {repository}@{ref}"
            )
        dispatch_started_at = time.time()
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

        self._wait_for_github_workflow_run(
            repository=repository,
            workflow=workflow,
            token=token,
            earliest_epoch=dispatch_started_at,
            progress_callback=progress_callback,
        )

    def _wait_for_github_workflow_run(
        self,
        repository: str,
        workflow: str,
        token: str,
        earliest_epoch: float,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        timeout_seconds = int(os.getenv("COMFY_ENDPOINTS_GHA_TIMEOUT_SECONDS", "1800"))
        poll_seconds = int(os.getenv("COMFY_ENDPOINTS_GHA_POLL_SECONDS", "10"))
        deadline = time.time() + timeout_seconds
        last_state = ""

        while time.time() < deadline:
            run = self._latest_workflow_run(
                repository,
                workflow,
                token,
                earliest_epoch=earliest_epoch,
            )
            if run is None:
                if progress_callback:
                    progress_callback("[image] waiting for workflow run to appear")
                time.sleep(poll_seconds)
                continue

            run_id = run.get("id")
            status = run.get("status")
            conclusion = run.get("conclusion")
            state = f"{status}/{conclusion}"
            if progress_callback and state != last_state:
                progress_callback(f"[image] workflow run {run_id} state={state}")
                last_state = state

            if status == "completed":
                if conclusion == "success":
                    return
                raise ImageBuildError(
                    f"GitHub workflow run failed (id={run_id}, conclusion={conclusion})"
                )

            time.sleep(poll_seconds)

        raise ImageBuildError(
            f"Timed out waiting for workflow completion after {timeout_seconds}s"
        )

    @staticmethod
    def _latest_workflow_run(
        repository: str,
        workflow: str,
        token: str,
        earliest_epoch: float,
    ) -> dict | None:
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repository}/actions/workflows/{workflow}/runs?per_page=1",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        runs = payload.get("workflow_runs") or []
        if not runs:
            return None
        for run in runs:
            created_at = str(run.get("created_at") or "")
            try:
                created_epoch = calendar.timegm(time.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ"))
            except Exception:  # noqa: BLE001
                created_epoch = 0
            if created_epoch >= earliest_epoch - 5:
                return run
        return None

    def _ensure_registry_login(
        self,
        image_ref: str,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        if not image_ref.startswith("ghcr.io/"):
            return

        username = os.getenv("GHCR_USERNAME", "").strip()
        token = os.getenv("GHCR_TOKEN", "").strip()
        if not username or not token:
            return

        command = ["docker", "login", "ghcr.io", "-u", username, "--password-stdin"]
        if progress_callback:
            progress_callback("[image] logging into ghcr.io for local push")
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
