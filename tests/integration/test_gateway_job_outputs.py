from __future__ import annotations

import json
import socket
import tempfile
import time
import unittest
from pathlib import Path

from comfy_endpoints.gateway.server import GatewayApp, GatewayConfig, GatewayHandler


class _FakeComfyClient:
    def __init__(self, outputs: dict):
        self.outputs = outputs

    def queue_prompt(self, _prompt_payload: dict) -> str:
        return "prompt-1"

    def get_history(self, _prompt_id: str) -> dict:
        return {"prompt-1": {"outputs": self.outputs}}

    def get_view_media(self, filename: str, subfolder: str, media_type: str) -> bytes:
        _ = (filename, subfolder, media_type)
        return b"generated-image"


class GatewayJobOutputsIntegrationTest(unittest.TestCase):
    @staticmethod
    def _round_trip(app: GatewayApp, raw_request: bytes) -> tuple[int, dict[str, str], bytes]:
        class BoundHandler(GatewayHandler):
            pass

        BoundHandler.app = app
        client_sock, server_sock = socket.socketpair()
        try:
            client_sock.settimeout(0.3)
            client_sock.sendall(raw_request)
            client_sock.shutdown(socket.SHUT_WR)
            BoundHandler(server_sock, ("127.0.0.1", 0), object())
            try:
                server_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                server_sock.close()
            except OSError:
                pass

            response = b""
            while True:
                try:
                    chunk = client_sock.recv(65536)
                except TimeoutError:
                    break
                if not chunk:
                    break
                response += chunk
        finally:
            client_sock.close()
            try:
                server_sock.close()
            except OSError:
                pass

        header_blob, body = response.split(b"\r\n\r\n", 1)
        header_lines = header_blob.decode("iso-8859-1").split("\r\n")
        status_code = int(header_lines[0].split(" ", 2)[1])
        headers: dict[str, str] = {}
        for line in header_lines[1:]:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()
        return status_code, headers, body

    def _build_app(self, root: Path, contract_outputs: list[dict], comfy_outputs: dict) -> GatewayApp:
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
            json.dumps({"prompt": {"1": {"class_type": "ApiInput", "inputs": {"value": ""}}}}),
            encoding="utf-8",
        )
        app = GatewayApp(
            GatewayConfig(
                listen_host="127.0.0.1",
                listen_port=3000,
                api_key="secret",
                comfy_url="http://127.0.0.1:8188",
                contract_path=contract_path,
                workflow_path=workflow_path,
                state_db=root / "jobs.db",
                app_id="demo",
            )
        )
        app.comfy_client = _FakeComfyClient(comfy_outputs)
        return app

    def test_run_and_job_include_generated_file_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            app = self._build_app(
                Path(tmp_dir),
                contract_outputs=[{"name": "image", "type": "image/png", "node_id": "9"}],
                comfy_outputs={
                    "9": {
                        "images": [
                            {"filename": "img.png", "subfolder": "", "type": "output"},
                        ]
                    }
                },
            )

            body = json.dumps({"prompt": "hello"}).encode("utf-8")
            run_request = (
                b"POST /run HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"content-type: application/json\r\n"
                b"connection: close\r\n"
                + f"content-length: {len(body)}\r\n\r\n".encode("ascii")
                + body
            )
            status, _headers, payload_body = self._round_trip(app, run_request)
            self.assertEqual(status, 202)
            run_payload = json.loads(payload_body.decode("utf-8"))
            job_id = run_payload["job_id"]

            terminal_payload = None
            for _ in range(20):
                job_request = (
                    f"GET /jobs/{job_id} HTTP/1.1\r\n".encode("ascii")
                    + b"Host: localhost\r\n"
                    + b"x-api-key: secret\r\n"
                    + b"connection: close\r\n\r\n"
                )
                job_status, _job_headers, job_body = self._round_trip(app, job_request)
                self.assertEqual(job_status, 200)
                candidate = json.loads(job_body.decode("utf-8"))
                if candidate.get("state") == "completed":
                    terminal_payload = candidate
                    break
                time.sleep(0.02)

            assert terminal_payload is not None
            image_file_id = terminal_payload["output"]["result"]["image"]
            self.assertTrue(str(image_file_id).startswith("fid_"))

    def test_multi_output_mixed_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            app = self._build_app(
                Path(tmp_dir),
                contract_outputs=[
                    {"name": "image", "type": "image/png", "node_id": "9"},
                    {"name": "caption", "type": "string", "node_id": "10"},
                ],
                comfy_outputs={
                    "9": {
                        "images": [
                            {"filename": "img.png", "subfolder": "", "type": "output"},
                        ]
                    },
                    "10": {"value": ["done"]},
                },
            )

            body = json.dumps({"prompt": "hello"}).encode("utf-8")
            run_request = (
                b"POST /run HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"content-type: application/json\r\n"
                b"connection: close\r\n"
                + f"content-length: {len(body)}\r\n\r\n".encode("ascii")
                + body
            )
            _status, _headers, payload_body = self._round_trip(app, run_request)
            run_payload = json.loads(payload_body.decode("utf-8"))
            job_id = run_payload["job_id"]

            terminal_payload = None
            for _ in range(20):
                job_request = (
                    f"GET /jobs/{job_id} HTTP/1.1\r\n".encode("ascii")
                    + b"Host: localhost\r\n"
                    + b"x-api-key: secret\r\n"
                    + b"connection: close\r\n\r\n"
                )
                _job_status, _job_headers, job_body = self._round_trip(app, job_request)
                candidate = json.loads(job_body.decode("utf-8"))
                if candidate.get("state") == "completed":
                    terminal_payload = candidate
                    break
                time.sleep(0.02)

            assert terminal_payload is not None
            result = terminal_payload["output"]["result"]
            self.assertTrue(str(result["image"]).startswith("fid_"))
            self.assertEqual(result["caption"], "done")


if __name__ == "__main__":
    unittest.main()
