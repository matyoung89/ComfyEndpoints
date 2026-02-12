from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from comfy_endpoints.contracts.validators import validate_deployable_spec
from comfy_endpoints.models import DeploymentRecord, DeploymentState
from comfy_endpoints.providers import build_provider
from comfy_endpoints.runtime.image_manager import ImageManager
from comfy_endpoints.runtime.state_store import DeploymentStore


class DeploymentService:
    def __init__(self, state_dir: Path):
        self.state_store = DeploymentStore(state_dir=state_dir)
        self.image_manager = ImageManager()

    def validate(self, app_spec_path: Path) -> tuple[str, str]:
        app_spec, contract = validate_deployable_spec(app_spec_path)
        return app_spec.app_id, contract.contract_id

    @staticmethod
    def _is_endpoint_ready(endpoint_url: str) -> bool:
        try:
            with urllib.request.urlopen(f"{endpoint_url}/healthz", timeout=10) as response:
                return response.status == 200
        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

    def deploy(
        self,
        app_spec_path: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> DeploymentRecord:
        app_spec, contract = validate_deployable_spec(app_spec_path)
        provider = build_provider(app_spec.provider)
        if progress_callback:
            progress_callback("[deploy] validated app spec and workflow contract")

        image_resolution = self.image_manager.ensure_image(
            app_spec,
            progress_callback=progress_callback,
        )
        image_ref = image_resolution.image_ref
        app_spec.build.image_ref = image_ref
        if progress_callback:
            progress_callback(
                f"[deploy] image ready: {image_ref} (built={image_resolution.built})"
            )
        max_attempts = int(os.getenv("COMFY_ENDPOINTS_RUNPOD_MAX_DEPLOY_ATTEMPTS", "4"))
        env = dict(app_spec.env)
        env["COMFY_ENDPOINTS_APP_ID"] = app_spec.app_id
        env["COMFY_ENDPOINTS_CONTRACT_ID"] = contract.contract_id
        env["COMFY_ENDPOINTS_CONTRACT_PATH"] = "/opt/comfy_endpoints/runtime/workflow.contract.json"
        env["COMFY_ENDPOINTS_CONTRACT_JSON"] = json.dumps(asdict(contract))
        mounts = [{"source": "cache", "target": "/cache"}]

        deployment_id = ""
        endpoint_url = ""
        status = None
        last_error = ""

        for attempt in range(1, max_attempts + 1):
            if progress_callback:
                progress_callback(f"[deploy] deploy attempt {attempt}/{max_attempts}")

            deployment_id = provider.create_deployment(app_spec)
            if progress_callback:
                progress_callback(f"[deploy] created deployment_id={deployment_id}")
            try:
                provider.ensure_volume(deployment_id=deployment_id, size_gb=100)
                if progress_callback:
                    progress_callback("[deploy] volume ensured (>=100GB)")
                provider.deploy_image(
                    deployment_id=deployment_id,
                    image_ref=image_ref,
                    env=env,
                    mounts=mounts,
                    container_registry_auth_id=app_spec.build.container_registry_auth_id,
                )
                if progress_callback:
                    progress_callback("[deploy] image/env/mounts applied to pod")

                status = provider.get_status(deployment_id)
                endpoint_url = provider.get_endpoint(deployment_id)
                if progress_callback:
                    progress_callback(f"[deploy] endpoint candidate={endpoint_url}")
                deadline = time.time() + 900
                last_detail = ""
                while True:
                    if progress_callback and status.detail != last_detail:
                        progress_callback(f"[deploy] pod status: {status.detail}")
                        last_detail = status.detail

                    if status.state == DeploymentState.FAILED:
                        break
                    if status.state in {DeploymentState.DEGRADED, DeploymentState.TERMINATED}:
                        break

                    if status.state == DeploymentState.READY and self._is_endpoint_ready(endpoint_url):
                        break

                    if time.time() > deadline:
                        break
                    time.sleep(5)
                    status = provider.get_status(deployment_id)

                if status.state == DeploymentState.READY and self._is_endpoint_ready(endpoint_url):
                    if progress_callback:
                        progress_callback("[deploy] endpoint health check passed")
                    break

                detail = status.detail if status else "unknown"
                last_error = detail
                outbid = "outbid" in detail.lower()
                retryable_state = status.state in {
                    DeploymentState.DEGRADED,
                    DeploymentState.TERMINATED,
                    DeploymentState.FAILED,
                }
                if attempt < max_attempts and (outbid or retryable_state):
                    if progress_callback:
                        progress_callback(f"[deploy] retrying with next profile due to: {detail}")
                    provider.destroy(deployment_id)
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                try:
                    provider.destroy(deployment_id)
                except Exception:  # noqa: BLE001
                    pass
                if attempt < max_attempts:
                    if progress_callback:
                        progress_callback(f"[deploy] retrying after error: {last_error}")
                    continue
                raise

        if status is None:
            raise RuntimeError(f"Deployment did not start. Last error: {last_error}")

        if status.state == DeploymentState.READY and not self._is_endpoint_ready(endpoint_url):
            status.state = DeploymentState.BOOTSTRAPPING
            status.detail = f"{status.detail} (endpoint not ready)"

        record = DeploymentRecord(
            app_id=app_spec.app_id,
            deployment_id=deployment_id,
            provider=app_spec.provider,
            state=status.state,
            endpoint_url=endpoint_url,
            api_key_ref=f"secret://{app_spec.app_id}/api_key",
            metadata={
                "image_ref": image_ref,
                "image_built": image_resolution.built,
                "contract_id": contract.contract_id,
                "status_detail": status.detail,
            },
        )
        self.state_store.put(record)

        return record

    def status(self, app_id: str) -> DeploymentRecord:
        record = self.state_store.get(app_id)
        if not record:
            raise RuntimeError(f"No deployment record found for app_id={app_id}")

        provider = build_provider(record.provider)
        latest = provider.get_status(record.deployment_id)
        record.state = latest.state
        record.metadata["status_detail"] = latest.detail
        self.state_store.put(record)
        return record

    def logs(self, app_id: str) -> str:
        record = self.state_store.get(app_id)
        if not record:
            raise RuntimeError(f"No deployment record found for app_id={app_id}")

        summary = {
            "app_id": record.app_id,
            "deployment_id": record.deployment_id,
            "state": record.state.value,
            "endpoint_url": record.endpoint_url,
            "status_detail": record.metadata.get("status_detail", "unknown"),
        }
        return json.dumps(summary, indent=2)

    def destroy(self, app_spec_path: Path) -> None:
        app_spec, _ = validate_deployable_spec(app_spec_path)
        record = self.state_store.get(app_spec.app_id)
        if not record:
            return

        provider = build_provider(record.provider)
        provider.destroy(record.deployment_id)
        self.state_store.delete(record.app_id)
