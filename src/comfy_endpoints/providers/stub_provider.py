from __future__ import annotations

from comfy_endpoints.models import AppSpecV1, DeploymentState
from comfy_endpoints.providers.base import CloudProviderAdapter, DeploymentStatus


class UnsupportedProviderAdapter(CloudProviderAdapter):
    def __init__(self, name: str):
        self.name = name

    def _unsupported(self) -> RuntimeError:
        return RuntimeError(f"Provider '{self.name}' is not implemented in v1")

    def create_deployment(self, app_spec: AppSpecV1) -> str:
        _ = app_spec
        raise self._unsupported()

    def ensure_volume(self, deployment_id: str, size_gb: int) -> str:
        _ = (deployment_id, size_gb)
        raise self._unsupported()

    def deploy_image(
        self,
        deployment_id: str,
        image_ref: str,
        env: dict[str, str],
        mounts: list[dict[str, str]],
        container_registry_auth_id: str | None = None,
    ) -> None:
        _ = (deployment_id, image_ref, env, mounts, container_registry_auth_id)
        raise self._unsupported()

    def get_status(self, deployment_id: str) -> DeploymentStatus:
        _ = deployment_id
        return DeploymentStatus(state=DeploymentState.DEGRADED, detail="Unsupported provider")

    def get_endpoint(self, deployment_id: str) -> str:
        _ = deployment_id
        raise self._unsupported()

    def destroy(self, deployment_id: str) -> None:
        _ = deployment_id
        raise self._unsupported()
