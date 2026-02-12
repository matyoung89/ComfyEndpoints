from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from comfy_endpoints.models import AppSpecV1, DeploymentState
from comfy_endpoints.providers.base import CloudProviderAdapter, DeploymentStatus
from comfy_endpoints.utils.env_loader import load_local_env


class RunpodError(RuntimeError):
    pass


@dataclass(slots=True)
class RunpodConfig:
    api_url: str = "https://api.runpod.io/graphql"
    rest_api_url: str = "https://rest.runpod.io/v1"
    api_key_env: str = "RUNPOD_API_KEY"
    keychain_service: str = "COMFY_ENDPOINTS_RUNPOD_API_KEY"
    keychain_account_env: str = "COMFY_ENDPOINTS_RUNPOD_KEYCHAIN_ACCOUNT"
    default_data_center_id: str = "US-KS-2"


class RunpodProvider(CloudProviderAdapter):
    name = "runpod"

    def __init__(self, config: RunpodConfig | None = None):
        self.config = config or RunpodConfig()

    def _api_key_from_keychain(self) -> str | None:
        account = os.getenv(self.config.keychain_account_env, os.getenv("USER", "")).strip()
        if not account:
            return None

        command = [
            "security",
            "find-generic-password",
            "-a",
            account,
            "-s",
            self.config.keychain_service,
            "-w",
        ]
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

        value = result.stdout.strip()
        return value or None

    def _api_key(self) -> str:
        key = os.getenv(self.config.api_key_env, "").strip()
        if key:
            return key

        load_local_env()
        key = os.getenv(self.config.api_key_env, "").strip()
        if key:
            return key

        key = self._api_key_from_keychain() or ""
        if not key:
            raise RunpodError(
                "Missing RunPod API key. Set RUNPOD_API_KEY or store it in macOS Keychain "
                f"service '{self.config.keychain_service}'."
            )
        return key

    def _rest_request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        query_params: dict[str, str] | None = None,
    ) -> Any:
        query = ""
        if query_params:
            query = "?" + urllib.parse.urlencode(query_params)

        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            f"{self.config.rest_api_url}{path}{query}",
            data=data,
            headers={
                "content-type": "application/json",
                "authorization": f"Bearer {self._api_key()}",
            },
            method=method,
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RunpodError(f"RunPod REST HTTP error {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RunpodError(f"RunPod REST connection error: {exc.reason}") from exc

        if not raw:
            return {}

        return json.loads(raw)

    @staticmethod
    def _map_region_to_data_center(region_hint: str | None, default: str) -> str:
        if not region_hint:
            return default

        normalized = region_hint.strip().upper()
        if normalized in {"US", "USA", "NA"}:
            return "US-KS-2"
        if normalized in {"EU", "EUR"}:
            return "EU-RO-1"
        if normalized in {"APAC", "ASIA"}:
            return "AP-JP-1"
        return default

    def create_deployment(self, app_spec: AppSpecV1) -> str:
        data_center_id = self._map_region_to_data_center(
            app_spec.regions[0] if app_spec.regions else None,
            self.config.default_data_center_id,
        )

        env = dict(app_spec.env)
        env["COMFY_HEADLESS"] = "1"

        image_ref = app_spec.build.image_ref or "ghcr.io/comfy-endpoints/golden:latest"
        payload = {
            "name": f"comfy-endpoints-{app_spec.app_id}",
            "imageName": image_ref,
            "gpuCount": 1,
            "cloudType": "SECURE",
            "containerDiskInGb": 30,
            "volumeInGb": 100,
            "volumeMountPath": "/cache",
            "ports": ["8080/http", "3000/http", "8188/http"],
            "env": env,
            "dataCenterIds": [data_center_id],
            "supportPublicIp": True,
        }
        if app_spec.build.container_registry_auth_id:
            payload["containerRegistryAuthId"] = app_spec.build.container_registry_auth_id

        created = self._rest_request("POST", "/pods", payload)
        if not isinstance(created, dict):
            raise RunpodError("RunPod create pod response was not an object")

        deployment_id = created.get("id")
        if not deployment_id:
            raise RunpodError(f"RunPod create pod missing id: {created}")

        return str(deployment_id)

    def ensure_volume(self, deployment_id: str, size_gb: int) -> str:
        pod = self._rest_request("GET", f"/pods/{deployment_id}")
        if not isinstance(pod, dict):
            raise RunpodError(f"Unexpected pod response for {deployment_id}")

        current_volume_size = int(pod.get("volumeInGb") or 0)
        if current_volume_size >= size_gb:
            return f"pod-volume:{deployment_id}:{current_volume_size}"

        self._rest_request(
            "PATCH",
            f"/pods/{deployment_id}",
            {
                "volumeInGb": size_gb,
                "volumeMountPath": "/cache",
            },
        )
        return f"pod-volume:{deployment_id}:{size_gb}"

    def deploy_image(
        self,
        deployment_id: str,
        image_ref: str,
        env: dict[str, str],
        mounts: list[dict[str, str]],
        container_registry_auth_id: str | None = None,
    ) -> None:
        mount_target = "/cache"
        if mounts:
            mount_target = mounts[0].get("target", "/cache")

        patch_payload: dict[str, Any] = {
            "imageName": image_ref,
            "env": env,
            "ports": ["8080/http", "3000/http", "8188/http"],
            "volumeMountPath": mount_target,
            "containerDiskInGb": 30,
        }
        if container_registry_auth_id:
            patch_payload["containerRegistryAuthId"] = container_registry_auth_id

        self._rest_request(
            "PATCH",
            f"/pods/{deployment_id}",
            patch_payload,
        )

        self._rest_request("POST", f"/pods/{deployment_id}/start")

    def get_status(self, deployment_id: str) -> DeploymentStatus:
        pod = self._rest_request("GET", f"/pods/{deployment_id}")
        if not isinstance(pod, dict):
            return DeploymentStatus(state=DeploymentState.FAILED, detail="Invalid pod response")

        desired_status = str(pod.get("desiredStatus") or "UNKNOWN").upper()

        state = DeploymentState.BOOTSTRAPPING
        if desired_status == "RUNNING":
            state = DeploymentState.READY
        elif desired_status in {"EXITED", "STOPPED"}:
            state = DeploymentState.DEGRADED
        elif desired_status == "TERMINATED":
            state = DeploymentState.TERMINATED

        return DeploymentStatus(state=state, detail=desired_status)

    def get_endpoint(self, deployment_id: str) -> str:
        pod = self._rest_request("GET", f"/pods/{deployment_id}")
        if not isinstance(pod, dict):
            return f"https://{deployment_id}-8080.proxy.runpod.net"

        ports = pod.get("ports") or []
        proxy_port = "8080"
        if isinstance(ports, list):
            for item in ports:
                if isinstance(item, str) and item.endswith("/http"):
                    proxy_port = item.split("/", 1)[0]
                    break

        return f"https://{deployment_id}-{proxy_port}.proxy.runpod.net"

    def destroy(self, deployment_id: str) -> None:
        try:
            self._rest_request("POST", f"/pods/{deployment_id}/stop")
        except RunpodError:
            pass
        self._rest_request("DELETE", f"/pods/{deployment_id}")
