from __future__ import annotations

import json
import socket
import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.gateway.server import GatewayApp, GatewayConfig, GatewayHandler


class GatewayFileApiIntegrationTest(unittest.TestCase):
    def _build_app(self, root: Path) -> GatewayApp:
        contract_path = root / "workflow.contract.json"
        workflow_path = root / "workflow.json"
        contract_path.write_text(
            json.dumps(
                {
                    "contract_id": "demo-contract",
                    "version": "v1",
                    "inputs": [{"name": "prompt", "type": "string", "required": True, "node_id": "1"}],
                    "outputs": [{"name": "image", "type": "image/png", "node_id": "9"}],
                }
            ),
            encoding="utf-8",
        )
        workflow_path.write_text(
            json.dumps({"prompt": {"1": {"inputs": {"value": ""}, "class_type": "ApiInput"}}}),
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
            )
        )

    @staticmethod
    def _round_trip(app: GatewayApp, raw_request: bytes) -> tuple[int, dict[str, str], bytes]:
        class BoundHandler(GatewayHandler):
            pass

        BoundHandler.app = app
        client_sock, server_sock = socket.socketpair()
        try:
            client_sock.settimeout(0.2)
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

    def test_upload_list_get_and_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            app = self._build_app(Path(tmp_dir))
            upload_body = b"hello world"
            upload_request = (
                b"POST /files HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"content-type: text/plain\r\n"
                b"x-file-name: hello.txt\r\n"
                b"x-app-id: demo\r\n"
                b"connection: close\r\n"
                + f"content-length: {len(upload_body)}\r\n\r\n".encode("ascii")
                + upload_body
            )
            status, _headers, body = self._round_trip(app, upload_request)
            self.assertEqual(status, 201)
            upload_payload = json.loads(body.decode("utf-8"))
            file_id = upload_payload["file_id"]

            list_request = (
                b"GET /files?limit=10 HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"connection: close\r\n\r\n"
            )
            list_status, _list_headers, list_body = self._round_trip(app, list_request)
            self.assertEqual(list_status, 200)
            list_payload = json.loads(list_body.decode("utf-8"))
            self.assertTrue(any(item["file_id"] == file_id for item in list_payload["items"]))

            get_request = (
                f"GET /files/{file_id} HTTP/1.1\r\n".encode("ascii")
                + b"Host: localhost\r\n"
                + b"x-api-key: secret\r\n"
                + b"connection: close\r\n\r\n"
            )
            get_status, _get_headers, get_body = self._round_trip(app, get_request)
            self.assertEqual(get_status, 200)
            get_payload = json.loads(get_body.decode("utf-8"))
            self.assertEqual(get_payload["media_type"], "text/plain")

            download_request = (
                f"GET /files/{file_id}/download HTTP/1.1\r\n".encode("ascii")
                + b"Host: localhost\r\n"
                + b"x-api-key: secret\r\n"
                + b"connection: close\r\n\r\n"
            )
            download_status, download_headers, download_body = self._round_trip(app, download_request)
            self.assertEqual(download_status, 200)
            self.assertEqual(download_body, b"hello world")
            self.assertEqual(download_headers.get("content-type"), "text/plain")
            self.assertEqual(download_headers.get("content-disposition"), 'attachment; filename="hello.txt"')

    def test_generated_files_are_listed_and_downloadable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            app = self._build_app(Path(tmp_dir))
            record = app.job_store.create_file(
                content=b"generated-media",
                media_type="image/png",
                source="generated",
                app_id="demo-generated",
                original_name="gen.png",
            )
            list_request = (
                b"GET /files?source=generated HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"connection: close\r\n\r\n"
            )
            list_status, _headers, list_body = self._round_trip(app, list_request)
            self.assertEqual(list_status, 200)
            payload = json.loads(list_body.decode("utf-8"))
            self.assertTrue(any(item["file_id"] == record.file_id for item in payload["items"]))

            download_request = (
                f"GET /files/{record.file_id}/download HTTP/1.1\r\n".encode("ascii")
                + b"Host: localhost\r\n"
                + b"x-api-key: secret\r\n"
                + b"connection: close\r\n\r\n"
            )
            status, _download_headers, body = self._round_trip(app, download_request)
            self.assertEqual(status, 200)
            self.assertEqual(body, b"generated-media")

    def test_missing_file_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            app = self._build_app(Path(tmp_dir))
            request = (
                b"GET /files/fid_missing/download HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"connection: close\r\n\r\n"
            )
            status, _headers, body = self._round_trip(app, request)
            self.assertEqual(status, 404)
            payload = json.loads(body.decode("utf-8"))
            self.assertEqual(payload["error"], "file_not_found")


if __name__ == "__main__":
    unittest.main()
