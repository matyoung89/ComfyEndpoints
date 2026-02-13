from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.gateway.server import GatewayApp, GatewayConfig, GatewayHandler, OutputResolutionError


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
            job_id = "job-1"
            app.job_store.write_output_artifacts(
                job_id,
                {
                    "image": "fid_generated",
                    "caption": "hello",
                    "score": 1.25,
                },
            )
            handler = self._handler_for_app(app)
            result = handler._resolve_contract_outputs(job_id, timeout_seconds=1)

            self.assertIn("image", result)
            self.assertEqual(result["image"], "fid_generated")
            self.assertEqual(result["caption"], "hello")
            self.assertEqual(result["score"], 1.25)

    def test_type_coercion_error_fails_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app = self._build_app(
                root,
                contract_outputs=[
                    {"name": "flag", "type": "boolean", "node_id": "10"},
                ],
            )
            job_id = "job-2"
            app.job_store.write_output_artifacts(job_id, {"flag": "nope"})
            handler = self._handler_for_app(app)

            with self.assertRaisesRegex(OutputResolutionError, "OUTPUT_TYPE_ERROR"):
                handler._resolve_contract_outputs(job_id, timeout_seconds=1)

    def test_missing_artifacts_reports_explicit_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app = self._build_app(
                root,
                contract_outputs=[
                    {"name": "image", "type": "image/png", "node_id": "9"},
                ],
            )
            handler = self._handler_for_app(app)
            with self.assertRaisesRegex(OutputResolutionError, "^MISSING_ARTIFACTS:image$"):
                handler._resolve_contract_outputs("job-3", timeout_seconds=0.01)


if __name__ == "__main__":
    unittest.main()
