from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.contracts.validators import ValidationError, validate_deployable_spec


class ValidatorsTest(unittest.TestCase):
    def test_validate_deployable_spec_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            workflow_path = root / "workflow.json"
            contract_path = root / "workflow.contract.json"
            app_path = root / "app.json"

            workflow_path.write_text("{}", encoding="utf-8")
            contract_path.write_text(
                json.dumps(
                    {
                        "contract_id": "demo",
                        "version": "v1",
                        "inputs": [
                            {
                                "name": "prompt",
                                "type": "string",
                                "required": True,
                                "node_id": "1",
                            }
                        ],
                        "outputs": [{"name": "image", "type": "image/png", "node_id": "99"}],
                    }
                ),
                encoding="utf-8",
            )
            app_path.write_text(
                json.dumps(
                    {
                        "app_id": "demo",
                        "version": "v1",
                        "workflow_path": "./workflow.json",
                        "provider": "runpod",
                        "gpu_profile": "A10G",
                        "regions": ["US"],
                        "env": {},
                        "endpoint": {
                            "name": "run",
                            "mode": "async",
                            "auth_mode": "api_key",
                            "timeout_seconds": 300,
                            "max_payload_mb": 10,
                        },
                        "cache_policy": {
                            "watch_paths": ["/tmp/models"],
                            "min_file_size_mb": 100,
                            "symlink_targets": ["/tmp/models"],
                        },
                        "build": {
                            "comfy_version": "0.3.26",
                            "plugins": [{"repo": "https://example.com", "ref": "main"}],
                        },
                    }
                ),
                encoding="utf-8",
            )

            app_spec, contract = validate_deployable_spec(app_path)
            self.assertEqual(app_spec.app_id, "demo")
            self.assertEqual(contract.contract_id, "demo")

    def test_validate_deployable_spec_requires_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            workflow_path = root / "workflow.json"
            app_path = root / "app.json"

            workflow_path.write_text("{}", encoding="utf-8")
            app_path.write_text(
                json.dumps(
                    {
                        "app_id": "demo",
                        "version": "v1",
                        "workflow_path": "./workflow.json",
                        "provider": "runpod",
                        "gpu_profile": "A10G",
                        "regions": ["US"],
                        "env": {},
                        "endpoint": {
                            "name": "run",
                            "mode": "async",
                            "auth_mode": "api_key",
                            "timeout_seconds": 300,
                            "max_payload_mb": 10,
                        },
                        "cache_policy": {
                            "watch_paths": ["/tmp/models"],
                            "min_file_size_mb": 100,
                            "symlink_targets": ["/tmp/models"],
                        },
                        "build": {"comfy_version": "0.3.26", "plugins": []},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValidationError):
                validate_deployable_spec(app_path)


if __name__ == "__main__":
    unittest.main()
