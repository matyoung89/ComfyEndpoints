from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.deploy.bootstrap import ensure_contract_file, ensure_workflow_file


class BootstrapContractFileTest(unittest.TestCase):
    def test_writes_contract_from_env_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            contract_path = Path(tmp_dir) / "runtime" / "workflow.contract.json"
            payload = {
                "contract_id": "demo-contract",
                "version": "v1",
                "inputs": [
                    {
                        "name": "prompt",
                        "type": "string",
                        "required": True,
                        "node_id": "1",
                    }
                ],
                "outputs": [
                    {
                        "name": "image",
                        "type": "image/png",
                        "node_id": "2",
                    }
                ],
            }
            with mock.patch.dict(os.environ, {"COMFY_ENDPOINTS_CONTRACT_JSON": json.dumps(payload)}, clear=False):
                ensure_contract_file(contract_path)

            self.assertTrue(contract_path.exists())
            parsed = json.loads(contract_path.read_text(encoding="utf-8"))
            self.assertEqual(parsed["contract_id"], "demo-contract")

    def test_raises_when_missing_and_no_env_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            contract_path = Path(tmp_dir) / "runtime" / "workflow.contract.json"
            with mock.patch.dict(os.environ, {"COMFY_ENDPOINTS_CONTRACT_JSON": ""}, clear=False):
                with self.assertRaisesRegex(RuntimeError, "Contract path missing"):
                    ensure_contract_file(contract_path)

    def test_writes_workflow_from_env_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workflow_path = Path(tmp_dir) / "runtime" / "workflow.json"
            payload = {"prompt": {"1": {"inputs": {"value": ""}, "class_type": "ApiInput"}}}
            with mock.patch.dict(os.environ, {"COMFY_ENDPOINTS_WORKFLOW_JSON": json.dumps(payload)}, clear=False):
                ensure_workflow_file(workflow_path)

            self.assertTrue(workflow_path.exists())
            parsed = json.loads(workflow_path.read_text(encoding="utf-8"))
            self.assertIn("prompt", parsed)


if __name__ == "__main__":
    unittest.main()
