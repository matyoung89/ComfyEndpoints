from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from json import JSONDecodeError
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
    default_cloud_type: str = "COMMUNITY"
    default_interruptible: bool = True
    user_agent: str = "comfy-endpoints/1.0"


class RunpodProvider(CloudProviderAdapter):
    name = "runpod"

    def __init__(self, config: RunpodConfig | None = None):
        self.config = config or RunpodConfig()
        self._create_profile_index = 0
        self._rest_gpu_type_enum_cache: set[str] | None = None

    @staticmethod
    def _parse_bool(raw: str) -> bool:
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _deployment_profiles(self) -> list[tuple[str, bool]]:
        # Cheapest to most stable fallback order.
        default_profiles = [
            ("COMMUNITY", True),
            ("COMMUNITY", False),
            ("SECURE", True),
            ("SECURE", False),
        ]
        raw = os.getenv("COMFY_ENDPOINTS_RUNPOD_DEPLOY_PROFILES", "").strip()
        if not raw:
            return default_profiles

        parsed: list[tuple[str, bool]] = []
        for item in raw.split(","):
            chunk = item.strip()
            if not chunk:
                continue
            if ":" not in chunk:
                continue
            cloud_type, interruptible_raw = chunk.split(":", 1)
            parsed.append((cloud_type.strip().upper(), self._parse_bool(interruptible_raw)))

        return parsed or default_profiles

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
        suppress_http_errors: tuple[int, ...] = (),
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
                "user-agent": self.config.user_agent,
            },
            method=method,
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code in suppress_http_errors:
                return {"_suppressed_http_error": exc.code, "_detail": detail}
            raise RunpodError(f"RunPod REST HTTP error {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RunpodError(f"RunPod REST connection error: {exc.reason}") from exc

        if not raw:
            return {}

        try:
            return json.loads(raw)
        except JSONDecodeError:
            return raw

    def _graphql_request(self, query: str, variables: dict[str, Any] | None = None) -> Any:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        req = urllib.request.Request(
            self.config.api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "authorization": f"Bearer {self._api_key()}",
                "user-agent": self.config.user_agent,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RunpodError(f"RunPod GraphQL HTTP error {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RunpodError(f"RunPod GraphQL connection error: {exc.reason}") from exc

        try:
            parsed = json.loads(raw or "{}")
        except JSONDecodeError as exc:
            raise RunpodError("RunPod GraphQL returned invalid JSON") from exc

        if not isinstance(parsed, dict):
            raise RunpodError("RunPod GraphQL response was not an object")
        if parsed.get("errors"):
            raise RunpodError(f"RunPod GraphQL error: {parsed.get('errors')}")
        return parsed.get("data", {})

    @staticmethod
    def _gpu_vram_gb(gpu_entry: dict[str, Any]) -> float:
        for key in ("memoryInGb", "memoryInGB", "memoryGb", "memory", "vram"):
            value = gpu_entry.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _gpu_type_ids_with_min_vram(self, min_vram_gb: int) -> list[str]:
        data = self._graphql_request(
            """
            query GetGpuTypes {
              gpuTypes {
                id
                displayName
                memoryInGb
              }
            }
            """
        )
        gpu_types = []
        if isinstance(data, dict):
            raw_gpu_types = data.get("gpuTypes")
            if isinstance(raw_gpu_types, list):
                gpu_types = [item for item in raw_gpu_types if isinstance(item, dict)]

        if not gpu_types:
            raise RunpodError("Unable to resolve RunPod GPU catalog for min_vram_gb filtering")

        candidate_ids: list[str] = []
        discovered: list[str] = []
        for item in gpu_types:
            gpu_id = str(item.get("id") or "").strip()
            if not gpu_id:
                continue
            vram_gb = self._gpu_vram_gb(item)
            display_name = str(item.get("displayName") or gpu_id)
            discovered.append(f"{display_name}:{vram_gb:g}GB")
            if vram_gb >= float(min_vram_gb):
                candidate_ids.append(gpu_id)

        if not candidate_ids:
            details = ", ".join(discovered[:20]) if discovered else "none"
            raise RunpodError(
                f"No RunPod GPU types satisfy min_vram_gb={min_vram_gb}. Available: {details}"
            )

        allowed_gpu_type_ids = self._rest_gpu_type_enum()
        if allowed_gpu_type_ids:
            filtered = [item for item in candidate_ids if item in allowed_gpu_type_ids]
            if not filtered:
                raise RunpodError(
                    "RunPod GraphQL GPU catalog and REST schema have no overlap for "
                    f"min_vram_gb={min_vram_gb}. Candidates: {', '.join(candidate_ids[:20])}"
                )
            candidate_ids = filtered
        return candidate_ids

    def _rest_gpu_type_enum(self) -> set[str] | None:
        if self._rest_gpu_type_enum_cache is not None:
            return self._rest_gpu_type_enum_cache

        try:
            spec = self._rest_request("GET", "/openapi.json")
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(spec, dict):
            return None

        try:
            body_schema = (
                spec["paths"]["/pods"]["post"]["requestBody"]["content"]["application/json"]["schema"]
            )
            if isinstance(body_schema, dict) and "$ref" in body_schema:
                ref_name = str(body_schema["$ref"]).split("/")[-1]
                body_schema = spec["components"]["schemas"][ref_name]
            props = body_schema.get("properties", {})
            gpu_type_ids_schema = props.get("gpuTypeIds", {})
            items = gpu_type_ids_schema.get("items", {})
            if isinstance(items, dict) and "$ref" in items:
                ref_name = str(items["$ref"]).split("/")[-1]
                items = spec["components"]["schemas"][ref_name]
            enum_values = items.get("enum", [])
        except Exception:  # noqa: BLE001
            return None

        if not isinstance(enum_values, list):
            return None
        normalized = {str(item).strip() for item in enum_values if str(item).strip()}
        if not normalized:
            return None
        self._rest_gpu_type_enum_cache = normalized
        return normalized

    @staticmethod
    def _collect_log_lines(payload: Any, lines: list[str]) -> None:
        if payload is None:
            return
        if isinstance(payload, str):
            for entry in payload.splitlines():
                value = entry.strip()
                if value:
                    lines.append(value)
            return
        if isinstance(payload, list):
            for item in payload:
                RunpodProvider._collect_log_lines(item, lines)
            return
        if isinstance(payload, dict):
            preferred_keys = (
                "message",
                "log",
                "line",
                "text",
                "detail",
                "error",
                "status",
                "lastStatusChange",
            )
            for key in preferred_keys:
                if key in payload:
                    RunpodProvider._collect_log_lines(payload[key], lines)
            for key in ("logs", "events", "data", "items"):
                if key in payload:
                    RunpodProvider._collect_log_lines(payload[key], lines)

    @classmethod
    def _normalize_logs(cls, payload: Any, tail_lines: int) -> str:
        lines: list[str] = []
        cls._collect_log_lines(payload, lines)
        if not lines:
            return ""

        deduped: list[str] = []
        seen: set[str] = set()
        for line in lines:
            if line in seen:
                continue
            seen.add(line)
            deduped.append(line)
        return "\n".join(deduped[-tail_lines:])

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
        compute_policy = app_spec.compute_policy
        gpu_count = compute_policy.gpu_count if compute_policy else 1
        min_ram_per_gpu_gb = compute_policy.min_ram_per_gpu_gb if compute_policy else None
        min_vram_gb = compute_policy.min_vram_gb if compute_policy else None
        gpu_type_ids: list[str] | None = None
        if min_vram_gb is not None:
            gpu_type_ids = self._gpu_type_ids_with_min_vram(min_vram_gb)
        profiles = self._deployment_profiles()
        last_error: Exception | None = None
        for idx in range(self._create_profile_index, len(profiles)):
            cloud_type, interruptible = profiles[idx]
            self._create_profile_index = idx + 1
            payload = {
                "name": f"comfy-endpoints-{app_spec.app_id}",
                "imageName": image_ref,
                "gpuCount": gpu_count,
                "cloudType": cloud_type,
                "interruptible": interruptible,
                "containerDiskInGb": 30,
                "volumeInGb": 100,
                "volumeMountPath": "/cache",
                "ports": ["8080/http", "3000/http", "8188/http"],
                "env": env,
                "dataCenterIds": [data_center_id],
                "supportPublicIp": True,
            }
            if min_ram_per_gpu_gb is not None:
                payload["minRAMPerGPU"] = min_ram_per_gpu_gb
            if gpu_type_ids:
                payload["gpuTypeIds"] = gpu_type_ids
            if app_spec.build.container_registry_auth_id:
                payload["containerRegistryAuthId"] = app_spec.build.container_registry_auth_id

            try:
                created = self._rest_request("POST", "/pods", payload)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue

            if not isinstance(created, dict):
                last_error = RunpodError("RunPod create pod response was not an object")
                continue

            deployment_id = created.get("id")
            if not deployment_id:
                last_error = RunpodError(f"RunPod create pod missing id: {created}")
                continue

            return str(deployment_id)

        if last_error:
            raise RunpodError(
                "Failed to create deployment across cheapest profiles; last error: "
                f"{last_error}"
            ) from last_error
        raise RunpodError("Failed to create deployment across cheapest profiles")

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

        start_response = self._rest_request(
            "POST",
            f"/pods/{deployment_id}/start",
            suppress_http_errors=(500,),
        )
        if isinstance(start_response, dict):
            detail = str(start_response.get("_detail") or "")
            if detail and "not in exited state" not in detail.lower():
                raise RunpodError(f"RunPod pod start failed: {detail}")

    def get_status(self, deployment_id: str) -> DeploymentStatus:
        pod = self._rest_request("GET", f"/pods/{deployment_id}")
        if not isinstance(pod, dict):
            return DeploymentStatus(state=DeploymentState.FAILED, detail="Invalid pod response")

        desired_status = str(pod.get("desiredStatus") or "UNKNOWN").upper()
        last_status_change = str(pod.get("lastStatusChange") or "")
        detail = f"{desired_status}: {last_status_change}".strip(": ")

        state = DeploymentState.BOOTSTRAPPING
        if desired_status == "RUNNING":
            # RunPod can report RUNNING while the container is still being created or image is still pulling.
            warmup_markers = [
                "create container",
                "still fetching image",
                "pulling image",
                "downloading",
            ]
            if any(marker in last_status_change.lower() for marker in warmup_markers):
                state = DeploymentState.BOOTSTRAPPING
            else:
                state = DeploymentState.READY
        elif desired_status in {"EXITED", "STOPPED"}:
            state = DeploymentState.DEGRADED
        elif desired_status == "TERMINATED":
            state = DeploymentState.TERMINATED

        return DeploymentStatus(state=state, detail=detail or desired_status)

    def get_endpoint(self, deployment_id: str) -> str:
        pod = self._rest_request("GET", f"/pods/{deployment_id}")
        if not isinstance(pod, dict):
            return f"https://{deployment_id}-3000.proxy.runpod.net"

        ports = pod.get("ports") or []
        preferred_port = os.getenv("COMFY_ENDPOINTS_PUBLIC_PORT", "3000").strip() or "3000"
        proxy_port = preferred_port
        candidate_ports: list[str] = []
        if isinstance(ports, list):
            for item in ports:
                if isinstance(item, str) and item.endswith("/http"):
                    candidate_ports.append(item.split("/", 1)[0])

        if candidate_ports:
            if preferred_port in candidate_ports:
                proxy_port = preferred_port
            elif "3000" in candidate_ports:
                proxy_port = "3000"
            else:
                proxy_port = candidate_ports[0]

        return f"https://{deployment_id}-{proxy_port}.proxy.runpod.net"

    def destroy(self, deployment_id: str) -> None:
        try:
            self._rest_request("POST", f"/pods/{deployment_id}/stop")
        except RunpodError:
            pass
        delete_response = self._rest_request(
            "DELETE",
            f"/pods/{deployment_id}",
            suppress_http_errors=(404,),
        )
        if isinstance(delete_response, dict) and "_suppressed_http_error" in delete_response:
            return

    def get_logs(self, deployment_id: str, tail_lines: int = 200) -> str:
        logs_response = self._rest_request(
            "GET",
            f"/pods/{deployment_id}/logs",
            query_params={"tail": str(tail_lines)},
            suppress_http_errors=(400, 404),
        )
        if isinstance(logs_response, dict) and "_suppressed_http_error" in logs_response:
            logs_response = {}
        logs = self._normalize_logs(logs_response, tail_lines)
        if logs:
            return logs

        events_response = self._rest_request(
            "GET",
            f"/pods/{deployment_id}/events",
            query_params={"limit": str(tail_lines)},
            suppress_http_errors=(400, 404),
        )
        if isinstance(events_response, dict) and "_suppressed_http_error" in events_response:
            events_response = {}
        events = self._normalize_logs(events_response, tail_lines)
        if events:
            return events

        pod_response = self._rest_request(
            "GET",
            f"/pods/{deployment_id}",
            suppress_http_errors=(404,),
        )
        if isinstance(pod_response, dict) and "_suppressed_http_error" in pod_response:
            return ""
        return self._normalize_logs(pod_response, tail_lines)
