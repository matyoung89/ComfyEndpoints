from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.gateway.server import GatewayApp, GatewayConfig, is_authorized, is_public_route


class GatewayPoliciesIntegrationTest(unittest.TestCase):
    def test_public_route_matrix(self) -> None:
        self.assertTrue(is_public_route("POST", "/run"))
        self.assertTrue(is_public_route("GET", "/jobs/abc"))
        self.assertTrue(is_public_route("GET", "/healthz"))
        self.assertFalse(is_public_route("GET", "/prompt"))
        self.assertFalse(is_public_route("GET", "/api/history"))
        self.assertFalse(is_public_route("POST", "/view"))

    def test_authorization(self) -> None:
        self.assertTrue(is_authorized("secret", "secret"))
        self.assertFalse(is_authorized("", "secret"))
        self.assertFalse(is_authorized("other", "secret"))

    def test_payload_validation_against_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            contract_path = root / "workflow.contract.json"
            contract_path.write_text(
                json.dumps(
                    {
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
                        "outputs": [{"name": "image", "type": "image/png", "node_id": "9"}],
                    }
                ),
                encoding="utf-8",
            )

            app = GatewayApp(
                GatewayConfig(
                    listen_host="127.0.0.1",
                    listen_port=3000,
                    api_key="secret",
                    comfy_url="http://127.0.0.1:8188",
                    contract_path=contract_path,
                    state_db=root / "jobs.db",
                )
            )
            ok, detail = app.validate_payload({"prompt": "hello"})
            self.assertTrue(ok)
            self.assertEqual(detail, "ok")

            bad_ok, bad_detail = app.validate_payload({})
            self.assertFalse(bad_ok)
            self.assertIn("missing_required_input", bad_detail)


if __name__ == "__main__":
    unittest.main()
