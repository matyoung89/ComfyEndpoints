from __future__ import annotations

import copy
import os

from comfy_endpoints.models import WorkflowApiContractV1


class PromptMappingError(ValueError):
    pass


def _inputs_from_widgets(class_type: str, widgets_values: list[object]) -> dict[str, object]:
    normalized = class_type.strip().lower()
    if normalized == "apiinput":
        return {
            "name": widgets_values[0] if len(widgets_values) > 0 else "prompt",
            "type": widgets_values[1] if len(widgets_values) > 1 else "string",
            "required": widgets_values[2] if len(widgets_values) > 2 else True,
            "value": widgets_values[3] if len(widgets_values) > 3 else "",
        }
    if normalized == "apioutput":
        return {
            "name": widgets_values[0] if len(widgets_values) > 0 else "output",
            "type": widgets_values[1] if len(widgets_values) > 1 else "string",
            "value": widgets_values[2] if len(widgets_values) > 2 else "",
        }
    return {}


def _prompt_from_nodes(workflow_payload: dict) -> dict[str, dict]:
    nodes = workflow_payload.get("nodes")
    if not isinstance(nodes, list):
        return {}

    prompt: dict[str, dict] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        class_type = node.get("class_type") or node.get("type")
        if node_id is None or not class_type:
            continue

        node_inputs = node.get("inputs")
        if not isinstance(node_inputs, dict):
            node_inputs = {}

        widgets_values = node.get("widgets_values")
        if isinstance(widgets_values, list):
            from_widgets = _inputs_from_widgets(str(class_type), widgets_values)
            for key, value in from_widgets.items():
                node_inputs.setdefault(key, value)

        prompt[str(node_id)] = {
            "class_type": str(class_type),
            "inputs": node_inputs,
        }

    return prompt


def parse_prompt_template(workflow_payload: dict) -> dict[str, dict]:
    prompt = workflow_payload.get("prompt")
    if isinstance(prompt, dict):
        return prompt

    from_nodes = _prompt_from_nodes(workflow_payload)
    if from_nodes:
        return from_nodes

    if workflow_payload and all(
        isinstance(key, str) and isinstance(value, dict) for key, value in workflow_payload.items()
    ):
        if any("class_type" in value for value in workflow_payload.values()):
            return workflow_payload

    raise PromptMappingError(
        "Workflow JSON must include a Comfy prompt template object under 'prompt' or as top-level graph."
    )


def _default_value(field_type: str) -> object:
    normalized = field_type.strip().lower()
    if normalized in {"string", "text", "str"}:
        return ""
    if normalized in {"int", "integer", "number"}:
        return 0
    if normalized in {"float", "double"}:
        return 0.0
    if normalized in {"bool", "boolean"}:
        return False
    return ""


def _resolve_input_key(node_inputs: dict, preferred_name: str) -> str:
    if preferred_name in node_inputs:
        return preferred_name
    if "value" in node_inputs:
        return "value"
    if len(node_inputs) == 1:
        return next(iter(node_inputs.keys()))
    return preferred_name


def map_contract_payload_to_prompt(
    workflow_payload: dict,
    contract: WorkflowApiContractV1,
    input_payload: dict[str, object],
    job_id: str | None = None,
) -> dict:
    prompt_template = parse_prompt_template(workflow_payload)
    prompt = {node_id: copy.deepcopy(node) for node_id, node in prompt_template.items()}

    for field in contract.inputs:
        if field.required and field.name not in input_payload:
            raise PromptMappingError(f"missing_required_input:{field.name}")

        node = prompt.get(field.node_id)
        if not isinstance(node, dict):
            raise PromptMappingError(f"missing_contract_node:{field.node_id}")
        node_inputs = node.get("inputs")
        if not isinstance(node_inputs, dict):
            raise PromptMappingError(f"invalid_contract_node_inputs:{field.node_id}")

        key = _resolve_input_key(node_inputs, field.name)
        if field.name in input_payload:
            node_inputs[key] = input_payload[field.name]
        elif field.required:
            raise PromptMappingError(f"missing_required_input:{field.name}")

    artifacts_dir = os.getenv("COMFY_ENDPOINTS_ARTIFACTS_DIR", "/opt/comfy_endpoints/runtime/artifacts")
    state_db_path = os.getenv("COMFY_ENDPOINTS_STATE_DB", "/opt/comfy_endpoints/runtime/jobs.db")
    for node in prompt.values():
        if not isinstance(node, dict):
            continue
        class_type = str(node.get("class_type", "")).strip().lower()
        if class_type != "apioutput":
            continue
        node_inputs = node.get("inputs")
        if not isinstance(node_inputs, dict):
            continue
        node_inputs["ce_job_id"] = job_id or ""
        node_inputs["ce_artifacts_dir"] = artifacts_dir
        node_inputs["ce_state_db"] = state_db_path

    return {"prompt": prompt}


def build_preflight_payload(workflow_payload: dict, contract: WorkflowApiContractV1) -> dict:
    defaults: dict[str, object] = {}
    for field in contract.inputs:
        defaults[field.name] = _default_value(field.type)
    return map_contract_payload_to_prompt(workflow_payload, contract, defaults)
