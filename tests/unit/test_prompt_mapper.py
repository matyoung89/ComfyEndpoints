from __future__ import annotations

import unittest

from comfy_endpoints.models import ContractInputField, ContractOutputField, WorkflowApiContractV1
from comfy_endpoints.gateway.prompt_mapper import (
    PromptMappingError,
    build_preflight_payload,
    map_contract_payload_to_prompt,
)


def _contract() -> WorkflowApiContractV1:
    return WorkflowApiContractV1(
        contract_id="demo-contract",
        version="v1",
        inputs=[
            ContractInputField(
                name="prompt",
                type="string",
                required=True,
                node_id="1",
            )
        ],
        outputs=[ContractOutputField(name="image", type="image/png", node_id="9")],
    )


class PromptMapperTest(unittest.TestCase):
    def test_maps_payload_to_prompt_template(self) -> None:
        workflow = {
            "prompt": {
                "1": {
                    "inputs": {"value": ""},
                    "class_type": "ApiInput",
                }
            }
        }
        payload = map_contract_payload_to_prompt(workflow, _contract(), {"prompt": "hello"})
        self.assertEqual(payload["prompt"]["1"]["inputs"]["value"], "hello")

    def test_missing_required_raises(self) -> None:
        workflow = {
            "prompt": {
                "1": {
                    "inputs": {"value": ""},
                    "class_type": "ApiInput",
                }
            }
        }
        with self.assertRaises(PromptMappingError):
            map_contract_payload_to_prompt(workflow, _contract(), {})

    def test_preflight_payload_uses_defaults(self) -> None:
        workflow = {
            "prompt": {
                "1": {
                    "inputs": {"value": "seed"},
                    "class_type": "ApiInput",
                }
            }
        }
        payload = build_preflight_payload(workflow, _contract())
        self.assertEqual(payload["prompt"]["1"]["inputs"]["value"], "")

    def test_maps_ui_nodes_workflow_to_prompt_graph(self) -> None:
        workflow = {
            "nodes": [
                {
                    "id": 1,
                    "type": "ApiInput",
                    "widgets_values": ["prompt", "string", True, ""],
                },
                {
                    "id": 2,
                    "type": "ApiOutput",
                    "widgets_values": ["image", "image/png", ""],
                },
            ]
        }
        payload = map_contract_payload_to_prompt(workflow, _contract(), {"prompt": "hello"})
        self.assertEqual(payload["prompt"]["1"]["class_type"], "ApiInput")
        self.assertEqual(payload["prompt"]["1"]["inputs"]["value"], "hello")
        self.assertIn("ce_state_db", payload["prompt"]["1"]["inputs"])
        self.assertEqual(payload["prompt"]["2"]["inputs"]["ce_job_id"], "")
        self.assertIn("ce_artifacts_dir", payload["prompt"]["2"]["inputs"])
        self.assertIn("ce_state_db", payload["prompt"]["2"]["inputs"])

    def test_injects_job_id_into_api_output_nodes(self) -> None:
        workflow = {
            "prompt": {
                "1": {"class_type": "ApiInput", "inputs": {"value": ""}},
                "2": {"class_type": "ApiOutput", "inputs": {"name": "image", "type": "image/png"}},
            }
        }
        payload = map_contract_payload_to_prompt(workflow, _contract(), {"prompt": "hello"}, job_id="job-123")
        self.assertEqual(payload["prompt"]["2"]["inputs"]["ce_job_id"], "job-123")
        self.assertIn("ce_state_db", payload["prompt"]["1"]["inputs"])


if __name__ == "__main__":
    unittest.main()
