from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class DeploymentState(str, Enum):
    PENDING = "PENDING"
    BOOTSTRAPPING = "BOOTSTRAPPING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    TERMINATED = "TERMINATED"


class ProviderName(str, Enum):
    RUNPOD = "runpod"
    VAST = "vast"
    LAMBDA = "lambda"
    AWS = "aws"
    GCP = "gcp"


class AuthMode(str, Enum):
    API_KEY = "api_key"


@dataclass(slots=True)
class EndpointSpec:
    name: str
    mode: str
    auth_mode: AuthMode
    timeout_seconds: int
    max_payload_mb: int


@dataclass(slots=True)
class CachePolicy:
    watch_paths: list[str]
    min_file_size_mb: int = 100
    symlink_targets: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ComputePolicy:
    min_vram_gb: int | None = None
    min_ram_per_gpu_gb: int | None = None
    gpu_count: int = 1


@dataclass(slots=True)
class BuildPluginSpec:
    repo: str
    ref: str


@dataclass(slots=True)
class BuildSpec:
    comfy_version: str
    plugins: list[BuildPluginSpec]
    image_ref: str | None = None
    image_repository: str | None = None
    base_image_repository: str | None = None
    container_registry_auth_id: str | None = None
    dockerfile_path: str | None = None
    base_dockerfile_path: str | None = None
    build_context: str | None = None
    base_build_context: str | None = None


@dataclass(slots=True)
class AppSpecV1:
    app_id: str
    version: str
    workflow_path: Path
    provider: ProviderName
    gpu_profile: str
    regions: list[str]
    env: dict[str, str]
    endpoint: EndpointSpec
    cache_policy: CachePolicy
    build: BuildSpec
    compute_policy: ComputePolicy | None = None


@dataclass(slots=True)
class ContractInputField:
    name: str
    type: str
    required: bool
    node_id: str


@dataclass(slots=True)
class ContractOutputField:
    name: str
    type: str
    node_id: str


@dataclass(slots=True)
class WorkflowApiContractV1:
    contract_id: str
    version: str
    inputs: list[ContractInputField]
    outputs: list[ContractOutputField]


@dataclass(slots=True)
class DeploymentRecord:
    app_id: str
    deployment_id: str
    provider: ProviderName
    state: DeploymentState
    endpoint_url: str | None = None
    api_key_ref: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
