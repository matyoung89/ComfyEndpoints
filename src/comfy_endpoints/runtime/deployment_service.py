from __future__ import annotations

import json
import time
from pathlib import Path

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

    def deploy(self, app_spec_path: Path) -> DeploymentRecord:
        app_spec, contract = validate_deployable_spec(app_spec_path)
        provider = build_provider(app_spec.provider)

        image_resolution = self.image_manager.ensure_image(app_spec)
        image_ref = image_resolution.image_ref
        app_spec.build.image_ref = image_ref

        deployment_id = provider.create_deployment(app_spec)
        provider.ensure_volume(deployment_id=deployment_id, size_gb=100)
        env = dict(app_spec.env)
        env["COMFY_ENDPOINTS_APP_ID"] = app_spec.app_id
        env["COMFY_ENDPOINTS_CONTRACT_ID"] = contract.contract_id

        mounts = [{"source": "cache", "target": "/cache"}]
        provider.deploy_image(
            deployment_id=deployment_id,
            image_ref=image_ref,
            env=env,
            mounts=mounts,
            container_registry_auth_id=app_spec.build.container_registry_auth_id,
        )

        status = provider.get_status(deployment_id)
        deadline = time.time() + 90
        while status.state not in {DeploymentState.READY, DeploymentState.FAILED}:
            if time.time() > deadline:
                break
            time.sleep(3)
            status = provider.get_status(deployment_id)

        endpoint_url = provider.get_endpoint(deployment_id)
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
