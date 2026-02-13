from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from comfy_endpoints.cli.main import main
from comfy_endpoints.models import DeploymentRecord, DeploymentState, ProviderName
from comfy_endpoints.runtime.state_store import DeploymentStore


class CliInvokeDynamicIntegrationTest(unittest.TestCase):
    def _state_dir_with_demo(self, root: Path) -> Path:
        state_dir = root / ".comfy_endpoints"
        store = DeploymentStore(state_dir=state_dir)
        store.put(
            DeploymentRecord(
                app_id="demo",
                deployment_id="dep-demo",
                provider=ProviderName.RUNPOD,
                state=DeploymentState.READY,
                endpoint_url="https://demo.example.com",
                api_key_ref="secret://demo/api_key",
                metadata={},
            )
        )
        return state_dir

    def test_shorthand_dynamic_invoke_uses_contract_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = self._state_dir_with_demo(root)
            contract = {
                "inputs": [
                    {"name": "prompt", "type": "string", "required": True, "node_id": "1"},
                ],
                "outputs": [],
            }
            with mock.patch("comfy_endpoints.cli.main._discover_contract", return_value=contract):
                with mock.patch(
                    "comfy_endpoints.cli.main._request_json_post",
                    return_value={"job_id": "job-1", "state": "queued"},
                ) as mocked_post:
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        self.assertEqual(
                            main(
                                [
                                    "--state-dir",
                                    str(state_dir),
                                    "demo",
                                    "--input-prompt",
                                    "hello world",
                                ]
                            ),
                            0,
                        )
                    payload = json.loads(stdout.getvalue())
                    self.assertEqual(payload["request"]["prompt"], "hello world")
                    self.assertEqual(payload["response"]["job_id"], "job-1")
                    mocked_post.assert_called_once()

    def test_media_file_flag_uploads_and_invokes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = self._state_dir_with_demo(root)
            contract = {
                "inputs": [
                    {"name": "image", "type": "image/png", "required": True, "node_id": "2"},
                ],
                "outputs": [],
            }
            media_path = root / "input.png"
            media_path.write_bytes(b"img-bytes")

            with mock.patch("comfy_endpoints.cli.main._discover_contract", return_value=contract):
                with mock.patch(
                    "comfy_endpoints.cli.main._request_upload",
                    return_value={"file_id": "fid_uploaded"},
                ) as mocked_upload:
                    with mock.patch(
                        "comfy_endpoints.cli.main._request_json_post",
                        return_value={"job_id": "job-2", "state": "queued"},
                    ) as mocked_post:
                        stdout = io.StringIO()
                        with redirect_stdout(stdout):
                            self.assertEqual(
                                main(
                                    [
                                        "--state-dir",
                                        str(state_dir),
                                        "invoke",
                                        "demo",
                                        "--input-image-file",
                                        str(media_path),
                                    ]
                                ),
                                0,
                            )
                        payload = json.loads(stdout.getvalue())
                        self.assertEqual(payload["request"]["image"], "fid_uploaded")
                        mocked_upload.assert_called_once()
                        mocked_post.assert_called_once()

    def test_complete_returns_dynamic_input_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = self._state_dir_with_demo(root)
            contract = {
                "inputs": [
                    {"name": "prompt", "type": "string", "required": True, "node_id": "1"},
                    {"name": "image", "type": "image/png", "required": False, "node_id": "2"},
                ],
                "outputs": [],
            }
            with mock.patch("comfy_endpoints.cli.main._discover_contract", return_value=contract):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    self.assertEqual(
                        main(
                            [
                                "--state-dir",
                                str(state_dir),
                                "_complete",
                                "--index",
                                "2",
                                "--words",
                                "comfy-endpoints",
                                "demo",
                                "--in",
                            ]
                        ),
                        0,
                    )
                values = set(stdout.getvalue().splitlines())
                self.assertIn("--input-prompt", values)
                self.assertIn("--input-image-file", values)
                self.assertIn("--input-image-id", values)


if __name__ == "__main__":
    unittest.main()
