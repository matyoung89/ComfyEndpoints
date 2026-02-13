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


class CliFilesCommandsIntegrationTest(unittest.TestCase):
    def test_files_list_get_and_download_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = root / ".comfy_endpoints"
            record_file_id = "fid_test_123"
            endpoint_url = "https://demo.example.com"
            store = DeploymentStore(state_dir=state_dir)
            store.put(
                DeploymentRecord(
                    app_id="demo",
                    deployment_id="dep-demo",
                    provider=ProviderName.RUNPOD,
                    state=DeploymentState.READY,
                    endpoint_url=endpoint_url,
                    api_key_ref="secret://demo/api_key",
                    metadata={},
                )
            )

            with mock.patch(
                "comfy_endpoints.cli.main._request_json",
                side_effect=[
                    {"items": [{"file_id": record_file_id, "media_type": "text/plain"}], "next_cursor": None},
                    {"file_id": record_file_id, "media_type": "text/plain"},
                ],
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    with mock.patch("sys.argv", ["comfy-endpoints", "--state-dir", str(state_dir), "files", "list"]):
                        self.assertEqual(main(), 0)
                list_payload = json.loads(stdout.getvalue())
                self.assertTrue(any(item["file_id"] == record_file_id for item in list_payload["items"]))

                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    with mock.patch(
                        "sys.argv",
                        [
                            "comfy-endpoints",
                            "--state-dir",
                            str(state_dir),
                            "files",
                            "get",
                            record_file_id,
                        ],
                    ):
                        self.assertEqual(main(), 0)
                get_payload = json.loads(stdout.getvalue())
                self.assertEqual(get_payload["file_id"], record_file_id)

            out_file = root / "downloaded.txt"
            def _download_stub(endpoint_url: str, app_id: str, path: str, out_path: Path) -> dict[str, str]:
                _ = (endpoint_url, app_id, path)
                out_path.write_bytes(b"cli-file")
                return {"content_type": "text/plain", "content_length": "8"}

            with mock.patch(
                "comfy_endpoints.cli.main._request_download",
                side_effect=_download_stub,
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    with mock.patch(
                        "sys.argv",
                        [
                            "comfy-endpoints",
                            "--state-dir",
                            str(state_dir),
                            "files",
                            "download",
                            record_file_id,
                            "--out",
                            str(out_file),
                        ],
                    ):
                        self.assertEqual(main(), 0)
            self.assertEqual(out_file.read_bytes(), b"cli-file")

    def test_files_upload_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = root / ".comfy_endpoints"
            endpoint_url = "https://demo.example.com"
            store = DeploymentStore(state_dir=state_dir)
            store.put(
                DeploymentRecord(
                    app_id="demo",
                    deployment_id="dep-demo",
                    provider=ProviderName.RUNPOD,
                    state=DeploymentState.READY,
                    endpoint_url=endpoint_url,
                    api_key_ref="secret://demo/api_key",
                    metadata={},
                )
            )

            upload_in = root / "upload.bin"
            upload_in.write_bytes(b"payload")
            with mock.patch(
                "comfy_endpoints.cli.main._request_upload",
                return_value={"file_id": "fid_uploaded", "size_bytes": 7},
            ) as mocked_upload:
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    with mock.patch(
                        "sys.argv",
                        [
                            "comfy-endpoints",
                            "--state-dir",
                            str(state_dir),
                            "files",
                            "upload",
                            "--in",
                            str(upload_in),
                            "--media-type",
                            "application/octet-stream",
                        ],
                    ):
                        self.assertEqual(main(), 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["file_id"], "fid_uploaded")
                mocked_upload.assert_called_once()


if __name__ == "__main__":
    unittest.main()
