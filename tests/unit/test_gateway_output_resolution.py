from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.gateway.server import GatewayApp, GatewayConfig, GatewayHandler, OutputResolutionError


class _FakeComfyClient:
    def __init__(self, history_payload: dict, media_payload: bytes = b"img"):
        self.history_payload = history_payload
        self.media_payload = media_payload

    def queue_prompt(self, _prompt_payload: dict) -> str:
        return "prompt-1"

    def get_history(self, _prompt_id: str) -> dict:
        return self.history_payload

    def get_view_media(self, filename: str, subfolder: str, media_type: str) -> bytes:
        _ = (filename, subfolder, media_type)
        return self.media_payload


class GatewayOutputResolutionTest(unittest.TestCase):
    def _build_app(self, root: Path, contract_outputs: list[dict], app_id: str = "demo") -> GatewayApp:
        contract_path = root / "workflow.contract.json"
        workflow_path = root / "workflow.json"
        contract_path.write_text(
            json.dumps(
                {
                    "contract_id": "demo-contract",
                    "version": "v1",
                    "inputs": [{"name": "prompt", "type": "string", "required": True, "node_id": "1"}],
                    "outputs": contract_outputs,
                }
            ),
            encoding="utf-8",
        )
        workflow_path.write_text(
            json.dumps(
                {
                    "prompt": {
                        "1": {"class_type": "ApiInput", "inputs": {"value": ""}},
                    }
                }
            ),
            encoding="utf-8",
        )
        return GatewayApp(
            GatewayConfig(
                listen_host="127.0.0.1",
                listen_port=3000,
                api_key="secret",
                comfy_url="http://127.0.0.1:8188",
                contract_path=contract_path,
                workflow_path=workflow_path,
                state_db=root / "jobs.db",
                app_id=app_id,
            )
        )

    @staticmethod
    def _handler_for_app(app: GatewayApp) -> GatewayHandler:
        handler = GatewayHandler.__new__(GatewayHandler)
        handler.app = app
        return handler

    def test_resolves_media_and_scalar_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app = self._build_app(
                root,
                contract_outputs=[
                    {"name": "image", "type": "image/png", "node_id": "9"},
                    {"name": "caption", "type": "string", "node_id": "10"},
                    {"name": "score", "type": "number", "node_id": "11"},
                ],
            )
            app.comfy_client = _FakeComfyClient(
                {
                    "prompt-1": {
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "flux.png",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            },
                            "10": {"value": ["hello"]},
                            "11": {"value": ["1.25"]},
                        }
                    }
                }
            )
            handler = self._handler_for_app(app)
            result = handler._resolve_contract_outputs("prompt-1")

            self.assertIn("image", result)
            self.assertTrue(str(result["image"]).startswith("fid_"))
            self.assertEqual(result["caption"], "hello")
            self.assertEqual(result["score"], 1.25)

            record = app.job_store.get_file(str(result["image"]))
            assert record is not None
            self.assertEqual(record.source, "generated")
            self.assertEqual(record.app_id, "demo")

    def test_type_coercion_error_fails_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app = self._build_app(
                root,
                contract_outputs=[
                    {"name": "flag", "type": "boolean", "node_id": "10"},
                ],
            )
            app.comfy_client = _FakeComfyClient({"prompt-1": {"outputs": {"10": {"value": ["nope"]}}}})
            handler = self._handler_for_app(app)

            with self.assertRaisesRegex(OutputResolutionError, "OUTPUT_TYPE_ERROR"):
                handler._resolve_contract_outputs("prompt-1")


if __name__ == "__main__":
    unittest.main()
