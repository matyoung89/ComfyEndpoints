from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ApiInputNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "name": ("STRING", {"default": "prompt"}),
                "type": ("STRING", {"default": "string"}),
                "required": ("BOOLEAN", {"default": True}),
                "value": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints"

    def execute(self, name: str, type: str, required: bool, value: str) -> tuple[str]:
        _ = (name, type, required)
        return (value,)


class ApiOutputNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "name": ("STRING", {"default": "image"}),
                "type": ("STRING", {"default": "image/png"}),
                "value": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints"

    def execute(self, name: str, type: str, value: str) -> tuple[str]:
        _ = (name, type)
        return (value,)


NODE_CLASS_MAPPINGS = {
    "ApiInput": ApiInputNode,
    "ApiOutput": ApiOutputNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ApiInput": "ComfyEndpoints API Input",
    "ApiOutput": "ComfyEndpoints API Output",
}


def export_contract_from_workflow(workflow_path: Path, output_path: Path) -> None:
    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
    nodes = workflow.get("nodes", [])

    inputs: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []

    for node in nodes:
        class_type = node.get("type") or node.get("class_type")
        node_id = str(node.get("id", ""))
        widgets = node.get("widgets_values", [])

        if class_type == "ApiInput":
            name = str(widgets[0] if len(widgets) > 0 else "input")
            value_type = str(widgets[1] if len(widgets) > 1 else "string")
            required = bool(widgets[2] if len(widgets) > 2 else True)
            inputs.append(
                {
                    "name": name,
                    "type": value_type,
                    "required": required,
                    "node_id": node_id,
                }
            )

        if class_type == "ApiOutput":
            name = str(widgets[0] if len(widgets) > 0 else "output")
            value_type = str(widgets[1] if len(widgets) > 1 else "string")
            outputs.append(
                {
                    "name": name,
                    "type": value_type,
                    "node_id": node_id,
                }
            )

    if not inputs or not outputs:
        raise ValueError("Workflow must include at least one ApiInput and one ApiOutput node")

    payload = {
        "contract_id": f"{workflow_path.stem}-contract",
        "version": "v1",
        "inputs": inputs,
        "outputs": outputs,
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
