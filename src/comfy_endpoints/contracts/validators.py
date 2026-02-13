from __future__ import annotations

from pathlib import Path

from comfy_endpoints.contracts.parser import ContractError, load_structured_file
from comfy_endpoints.models import (
    AppSpecV1,
    AuthMode,
    BuildPluginSpec,
    BuildSpec,
    CachePolicy,
    ContractInputField,
    ContractOutputField,
    EndpointSpec,
    ProviderName,
    WorkflowApiContractV1,
)


REQUIRED_APP_FIELDS = {
    "app_id",
    "version",
    "workflow_path",
    "provider",
    "gpu_profile",
    "regions",
    "env",
    "endpoint",
    "cache_policy",
    "build",
}


class ValidationError(ValueError):
    pass


def _expect_fields(mapping: dict, required: set[str], context: str) -> None:
    missing = sorted(required - set(mapping.keys()))
    if missing:
        raise ValidationError(f"Missing required fields in {context}: {', '.join(missing)}")


def parse_app_spec(path: Path) -> AppSpecV1:
    raw = load_structured_file(path)
    _expect_fields(raw, REQUIRED_APP_FIELDS, "app spec")

    workflow_path = Path(raw["workflow_path"])
    if not workflow_path.is_absolute():
        workflow_path = (path.parent / workflow_path).resolve()

    endpoint_raw = raw["endpoint"]
    _expect_fields(endpoint_raw, {"name", "mode", "auth_mode", "timeout_seconds", "max_payload_mb"}, "endpoint")

    cache_raw = raw["cache_policy"]
    _expect_fields(cache_raw, {"watch_paths", "min_file_size_mb", "symlink_targets"}, "cache_policy")

    build_raw = raw["build"]
    _expect_fields(build_raw, {"comfy_version", "plugins"}, "build")

    plugins = [
        BuildPluginSpec(repo=item["repo"], ref=item["ref"])
        for item in build_raw.get("plugins", [])
    ]

    return AppSpecV1(
        app_id=str(raw["app_id"]),
        version=str(raw["version"]),
        workflow_path=workflow_path,
        provider=ProviderName(str(raw["provider"])),
        gpu_profile=str(raw["gpu_profile"]),
        regions=[str(item) for item in raw.get("regions", [])],
        env={str(k): str(v) for k, v in raw.get("env", {}).items()},
        endpoint=EndpointSpec(
            name=str(endpoint_raw["name"]),
            mode=str(endpoint_raw["mode"]),
            auth_mode=AuthMode(str(endpoint_raw["auth_mode"])),
            timeout_seconds=int(endpoint_raw["timeout_seconds"]),
            max_payload_mb=int(endpoint_raw["max_payload_mb"]),
        ),
        cache_policy=CachePolicy(
            watch_paths=[str(item) for item in cache_raw.get("watch_paths", [])],
            min_file_size_mb=int(cache_raw.get("min_file_size_mb", 100)),
            symlink_targets=[str(item) for item in cache_raw.get("symlink_targets", [])],
        ),
        build=BuildSpec(
            comfy_version=str(build_raw["comfy_version"]),
            plugins=plugins,
            image_ref=str(build_raw["image_ref"]) if build_raw.get("image_ref") else None,
            image_repository=(
                str(build_raw["image_repository"])
                if build_raw.get("image_repository")
                else None
            ),
            base_image_repository=(
                str(build_raw["base_image_repository"])
                if build_raw.get("base_image_repository")
                else None
            ),
            container_registry_auth_id=(
                str(build_raw["container_registry_auth_id"])
                if build_raw.get("container_registry_auth_id")
                else None
            ),
            dockerfile_path=str(build_raw["dockerfile_path"]) if build_raw.get("dockerfile_path") else None,
            base_dockerfile_path=(
                str(build_raw["base_dockerfile_path"])
                if build_raw.get("base_dockerfile_path")
                else None
            ),
            build_context=str(build_raw["build_context"]) if build_raw.get("build_context") else None,
            base_build_context=(
                str(build_raw["base_build_context"])
                if build_raw.get("base_build_context")
                else None
            ),
        ),
    )


def parse_workflow_contract(path: Path) -> WorkflowApiContractV1:
    raw = load_structured_file(path)
    _expect_fields(raw, {"contract_id", "version", "inputs", "outputs"}, "workflow contract")

    inputs = [
        ContractInputField(
            name=str(item["name"]),
            type=str(item["type"]),
            required=bool(item["required"]),
            node_id=str(item["node_id"]),
        )
        for item in raw.get("inputs", [])
    ]

    outputs = [
        ContractOutputField(
            name=str(item["name"]),
            type=str(item["type"]),
            node_id=str(item["node_id"]),
        )
        for item in raw.get("outputs", [])
    ]

    if not inputs:
        raise ValidationError("Workflow contract must declare at least one input")

    if not outputs:
        raise ValidationError("Workflow contract must declare at least one output")

    return WorkflowApiContractV1(
        contract_id=str(raw["contract_id"]),
        version=str(raw["version"]),
        inputs=inputs,
        outputs=outputs,
    )


def validate_deployable_spec(path: Path) -> tuple[AppSpecV1, WorkflowApiContractV1]:
    app_spec = parse_app_spec(path)
    if not app_spec.workflow_path.exists():
        raise ValidationError(f"Workflow file not found: {app_spec.workflow_path}")

    contract_path = app_spec.workflow_path.with_suffix(".contract.json")
    if not contract_path.exists():
        raise ValidationError(
            "Missing workflow contract export. Expected file next to workflow: "
            f"{contract_path}"
        )

    contract = parse_workflow_contract(contract_path)
    return app_spec, contract


__all__ = [
    "ContractError",
    "ValidationError",
    "parse_app_spec",
    "parse_workflow_contract",
    "validate_deployable_spec",
]
