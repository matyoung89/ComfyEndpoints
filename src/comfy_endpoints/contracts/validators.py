from __future__ import annotations

import json
import re
from pathlib import Path

from comfy_endpoints.contracts.parser import ContractError, load_structured_file
from comfy_endpoints.models import (
    AppSpecV1,
    ArtifactSourceSpec,
    AuthMode,
    BuildPluginSpec,
    BuildSpec,
    CachePolicy,
    ComputePolicy,
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


SCALAR_CONTRACT_TYPES = {"string", "integer", "number", "boolean", "object", "array"}
MEDIA_TYPE_PATTERN = re.compile(r"^(image|video|audio|file)/[A-Za-z0-9][A-Za-z0-9.+-]*$")


def _expect_fields(mapping: dict, required: set[str], context: str) -> None:
    missing = sorted(required - set(mapping.keys()))
    if missing:
        raise ValidationError(f"Missing required fields in {context}: {', '.join(missing)}")


def _positive_int_or_none(raw: object, field_name: str) -> int | None:
    if raw is None:
        return None
    value = int(raw)
    if value <= 0:
        raise ValidationError(f"{field_name} must be > 0")
    return value


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
    compute_raw = raw.get("compute_policy")
    compute_policy: ComputePolicy | None = None
    if compute_raw is not None:
        if not isinstance(compute_raw, dict):
            raise ValidationError("compute_policy must be an object")
        compute_policy = ComputePolicy(
            min_vram_gb=_positive_int_or_none(
                compute_raw.get("min_vram_gb"),
                "compute_policy.min_vram_gb",
            ),
            min_ram_per_gpu_gb=_positive_int_or_none(
                compute_raw.get("min_ram_per_gpu_gb"),
                "compute_policy.min_ram_per_gpu_gb",
            ),
            gpu_count=int(compute_raw.get("gpu_count", 1)),
        )
        if compute_policy.gpu_count <= 0:
            raise ValidationError("compute_policy.gpu_count must be > 0")

    artifact_specs: list[ArtifactSourceSpec] = []
    for index, item in enumerate(raw.get("artifacts", [])):
        if not isinstance(item, dict):
            raise ValidationError(f"artifacts[{index}] must be an object")
        kind = str(item.get("kind", "model")).strip() or "model"
        if kind not in {"model", "custom_node"}:
            raise ValidationError(f"artifacts[{index}].kind must be 'model' or 'custom_node'")
        required_fields = {"source_url"}
        if kind == "model":
            required_fields.update({"match", "target_subdir", "target_path"})
        _expect_fields(item, required_fields, f"artifacts[{index}]")
        provides_raw = item.get("provides", [])
        if provides_raw is None:
            provides_raw = []
        if not isinstance(provides_raw, list):
            raise ValidationError(f"artifacts[{index}].provides must be an array")
        artifact_specs.append(
            ArtifactSourceSpec(
                match=str(item.get("match", "")).strip(),
                source_url=str(item["source_url"]).strip(),
                target_subdir=str(item.get("target_subdir", "")).strip(),
                target_path=str(item.get("target_path", "")).strip(),
                kind=kind,
                ref=str(item.get("ref", "")).strip() or None,
                provides=[str(value).strip() for value in provides_raw if str(value).strip()],
            )
        )

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
        compute_policy=compute_policy,
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
        artifacts=artifact_specs,
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

    _validate_output_fields(outputs)

    return WorkflowApiContractV1(
        contract_id=str(raw["contract_id"]),
        version=str(raw["version"]),
        inputs=inputs,
        outputs=outputs,
    )


def _is_supported_output_type(type_name: str) -> bool:
    normalized = type_name.strip().lower()
    if normalized in SCALAR_CONTRACT_TYPES:
        return True
    return bool(MEDIA_TYPE_PATTERN.fullmatch(normalized))


def _validate_output_fields(outputs: list[ContractOutputField]) -> None:
    names_seen: set[str] = set()
    for field in outputs:
        normalized_name = field.name.strip()
        if not normalized_name:
            raise ValidationError("Workflow contract output names must be non-empty")
        if normalized_name in names_seen:
            raise ValidationError(f"Workflow contract outputs must have unique names: {normalized_name}")
        names_seen.add(normalized_name)

        if not _is_supported_output_type(field.type):
            raise ValidationError(f"Unsupported workflow output type: {field.type}")


def _workflow_node_class_map(workflow_payload: dict) -> dict[str, str]:
    node_map: dict[str, str] = {}

    prompt = workflow_payload.get("prompt")
    if isinstance(prompt, dict):
        for node_id, node in prompt.items():
            if not isinstance(node, dict):
                continue
            class_type = node.get("class_type")
            if not class_type:
                continue
            node_map[str(node_id)] = str(class_type)

    nodes = workflow_payload.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            class_type = node.get("class_type") or node.get("type")
            if node_id is None or not class_type:
                continue
            node_map[str(node_id)] = str(class_type)

    return node_map


def _validate_contract_output_nodes(workflow_payload: dict, contract: WorkflowApiContractV1) -> None:
    node_map = _workflow_node_class_map(workflow_payload)
    if not node_map:
        raise ValidationError("Workflow must include nodes or prompt graph for output validation")

    for field in contract.outputs:
        class_type = node_map.get(field.node_id)
        if class_type is None:
            raise ValidationError(f"Workflow output node missing for contract output '{field.name}': {field.node_id}")
        if class_type.strip().lower() != "apioutput":
            raise ValidationError(
                f"Workflow output node '{field.node_id}' for output '{field.name}' must be ApiOutput"
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
    workflow_payload = json.loads(app_spec.workflow_path.read_text(encoding="utf-8"))
    if not isinstance(workflow_payload, dict):
        raise ValidationError(f"Workflow file must contain a JSON object: {app_spec.workflow_path}")
    _validate_contract_output_nodes(workflow_payload, contract)
    return app_spec, contract


__all__ = [
    "ContractError",
    "ValidationError",
    "parse_app_spec",
    "parse_workflow_contract",
    "validate_deployable_spec",
]
