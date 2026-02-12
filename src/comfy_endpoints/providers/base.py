from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from comfy_endpoints.models import AppSpecV1, DeploymentState


@dataclass(slots=True)
class DeploymentStatus:
    state: DeploymentState
    detail: str


class CloudProviderAdapter(ABC):
    name: str

    @abstractmethod
    def create_deployment(self, app_spec: AppSpecV1) -> str:
        raise NotImplementedError

    @abstractmethod
    def ensure_volume(self, deployment_id: str, size_gb: int) -> str:
        raise NotImplementedError

    @abstractmethod
    def deploy_image(
        self,
        deployment_id: str,
        image_ref: str,
        env: dict[str, str],
        mounts: list[dict[str, str]],
        container_registry_auth_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_status(self, deployment_id: str) -> DeploymentStatus:
        raise NotImplementedError

    @abstractmethod
    def get_endpoint(self, deployment_id: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def destroy(self, deployment_id: str) -> None:
        raise NotImplementedError

    def build_metadata(self) -> dict[str, Any]:
        return {}
