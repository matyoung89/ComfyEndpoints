from __future__ import annotations

import json
import os
import socket
import tempfile
import time
import unittest
from pathlib import Path

from comfy_endpoints.gateway.server import GatewayApp, GatewayConfig, GatewayHandler


class _FakeComfyClient:
    def __init__(self, outputs: dict, skip_artifact_names: set[str] | None = None):
        self.outputs = outputs
        self.skip_artifact_names = skip_artifact_names or set()
        self.last_prompt_payload: dict | None = None

    def queue_prompt(self, prompt_payload: dict) -> str:
        self.last_prompt_payload = prompt_payload
        prompt = prompt_payload.get("prompt", {})
        if isinstance(prompt, dict):
            for node_id, node in prompt.items():
                if not isinstance(node, dict):
                    continue
                if str(node.get("class_type", "")).strip().lower() != "apioutput":
                    continue
                node_inputs = node.get("inputs")
                if not isinstance(node_inputs, dict):
                    continue
                output_name = str(node_inputs.get("name", "")).strip()
                job_id = str(node_inputs.get("ce_job_id", "")).strip()
                artifacts_dir = str(node_inputs.get("ce_artifacts_dir", "")).strip()
                if not output_name or not job_id or not artifacts_dir:
                    continue
                if output_name in self.skip_artifact_names:
                    continue

                node_output = self.outputs.get(str(node_id), {})
                value = ""
                if isinstance(node_output, dict):
                    values = node_output.get("value")
                    if isinstance(values, list) and values:
                        value = values[0]
                    elif "value" in node_output and not isinstance(node_output["value"], list):
                        value = node_output["value"]
                    elif "images" in node_output:
                        value = f"fid_mock_{output_name}"
                artifact_path = Path(artifacts_dir) / job_id / output_name
                artifact_path.parent.mkdir(parents=True, exist_ok=True)
                artifact_path.write_text(str(value), encoding="utf-8")
        return "prompt-1"

    def get_history(self, _prompt_id: str) -> dict:
        return {"prompt-1": {"outputs": self.outputs}}

    def interrupt(self) -> None:
        return None

    def delete_prompt_from_queue(self, _prompt_id: str) -> None:
        return None

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
        os.environ["COMFY_ENDPOINTS_ARTIFACTS_DIR"] = str(root / "artifacts")
        os.environ["COMFY_ENDPOINTS_STATE_DB"] = str(root / "jobs.db")
        contract_path = root / "workflow.contract.json"
        workflow_path = root / "workflow.json"
        prompt_nodes = {
            "1": {"class_type": "ApiInput", "inputs": {"value": ""}},
        }
        for item in contract_outputs:
            node_id = str(item.get("node_id", ""))
            prompt_nodes[node_id] = {
                "class_type": "ApiOutput",
                "inputs": {
                    "name": str(item.get("name", "")),
                    "type": str(item.get("type", "")),
                    "value": "",
                },
            }
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
            json.dumps({"prompt": prompt_nodes}),
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
            artifact_path = Path(tmp_dir) / "artifacts" / job_id / "image"
            self.assertTrue(artifact_path.exists())
            self.assertEqual(artifact_path.read_text(encoding="utf-8"), str(image_file_id))

    def test_job_fails_when_prompt_done_but_artifacts_missing(self) -> None:
        previous_artifact_grace = os.environ.get("COMFY_ENDPOINTS_ARTIFACT_GRACE_SECONDS")
        previous_output_poll = os.environ.get("COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS")
        os.environ["COMFY_ENDPOINTS_ARTIFACT_GRACE_SECONDS"] = "1"
        os.environ["COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS"] = "0.1"
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                comfy_outputs = {
                    "9": {
                        "images": [
                            {"filename": "img.png", "subfolder": "", "type": "output"},
                        ]
                    }
                }
                app = self._build_app(
                    Path(tmp_dir),
                    contract_outputs=[{"name": "image", "type": "image/png", "node_id": "9"}],
                    comfy_outputs=comfy_outputs,
                )
                app.comfy_client = _FakeComfyClient(
                    comfy_outputs,
                    skip_artifact_names={"image"},
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
                for _ in range(80):
                    job_request = (
                        f"GET /jobs/{job_id} HTTP/1.1\r\n".encode("ascii")
                        + b"Host: localhost\r\n"
                        + b"x-api-key: secret\r\n"
                        + b"connection: close\r\n\r\n"
                    )
                    _job_status, _job_headers, job_body = self._round_trip(app, job_request)
                    candidate = json.loads(job_body.decode("utf-8"))
                    if candidate.get("state") in {"failed", "completed"}:
                        terminal_payload = candidate
                        break
                    time.sleep(0.05)

                assert terminal_payload is not None
                self.assertEqual(terminal_payload["state"], "failed")
                self.assertIn("MISSING_ARTIFACTS:image", str(terminal_payload.get("error", "")))
        finally:
            if previous_artifact_grace is None:
                os.environ.pop("COMFY_ENDPOINTS_ARTIFACT_GRACE_SECONDS", None)
            else:
                os.environ["COMFY_ENDPOINTS_ARTIFACT_GRACE_SECONDS"] = previous_artifact_grace
            if previous_output_poll is None:
                os.environ.pop("COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS", None)
            else:
                os.environ["COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS"] = previous_output_poll

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
            image_artifact = Path(tmp_dir) / "artifacts" / job_id / "image"
            caption_artifact = Path(tmp_dir) / "artifacts" / job_id / "caption"
            self.assertTrue(image_artifact.exists())
            self.assertTrue(caption_artifact.exists())
            self.assertEqual(image_artifact.read_text(encoding="utf-8"), str(result["image"]))
            self.assertEqual(caption_artifact.read_text(encoding="utf-8"), "done")

    def test_cancel_running_job_transitions_to_canceled(self) -> None:
        class _SlowComfyClient(_FakeComfyClient):
            def get_history(self, _prompt_id: str) -> dict:
                return {}

        previous_prompt_timeout = os.environ.get("COMFY_ENDPOINTS_PROMPT_TIMEOUT_SECONDS")
        previous_poll = os.environ.get("COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS")
        os.environ["COMFY_ENDPOINTS_PROMPT_TIMEOUT_SECONDS"] = "none"
        os.environ["COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS"] = "0.05"
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                app = self._build_app(
                    Path(tmp_dir),
                    contract_outputs=[{"name": "image", "type": "image/png", "node_id": "9"}],
                    comfy_outputs={"9": {"images": [{"filename": "x.png", "subfolder": "", "type": "output"}]}},
                )
                app.comfy_client = _SlowComfyClient(outputs={})

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
                job_id = json.loads(payload_body.decode("utf-8"))["job_id"]

                cancel_request = (
                    f"POST /jobs/{job_id}/cancel HTTP/1.1\r\n".encode("ascii")
                    + b"Host: localhost\r\n"
                    + b"x-api-key: secret\r\n"
                    + b"content-type: application/json\r\n"
                    + b"content-length: 2\r\n"
                    + b"connection: close\r\n\r\n{}"
                )
                cancel_status, _cancel_headers, cancel_body = self._round_trip(app, cancel_request)
                self.assertEqual(cancel_status, 202)
                cancel_payload = json.loads(cancel_body.decode("utf-8"))
                self.assertTrue(cancel_payload.get("cancel_requested"))

                terminal_payload = None
                for _ in range(120):
                    job_request = (
                        f"GET /jobs/{job_id} HTTP/1.1\r\n".encode("ascii")
                        + b"Host: localhost\r\n"
                        + b"x-api-key: secret\r\n"
                        + b"connection: close\r\n\r\n"
                    )
                    _job_status, _job_headers, job_body = self._round_trip(app, job_request)
                    candidate = json.loads(job_body.decode("utf-8"))
                    if candidate.get("state") in {"canceled", "cancelled", "failed", "completed"}:
                        terminal_payload = candidate
                        break
                    time.sleep(0.02)

                assert terminal_payload is not None
                self.assertEqual(terminal_payload["state"], "canceled")
        finally:
            if previous_prompt_timeout is None:
                os.environ.pop("COMFY_ENDPOINTS_PROMPT_TIMEOUT_SECONDS", None)
            else:
                os.environ["COMFY_ENDPOINTS_PROMPT_TIMEOUT_SECONDS"] = previous_prompt_timeout
            if previous_poll is None:
                os.environ.pop("COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS", None)
            else:
                os.environ["COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS"] = previous_poll

    def test_run_accepts_media_file_ids_for_two_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            os.environ["COMFY_ENDPOINTS_ARTIFACTS_DIR"] = str(root / "artifacts")
            os.environ["COMFY_ENDPOINTS_STATE_DB"] = str(root / "jobs.db")

            contract_path = root / "workflow.contract.json"
            workflow_path = root / "workflow.json"
            contract_path.write_text(
                json.dumps(
                    {
                        "contract_id": "wanimate-contract",
                        "version": "v1",
                        "inputs": [
                            {"name": "reference_image", "type": "image/png", "required": True, "node_id": "1"},
                            {"name": "driving_video", "type": "video/mp4", "required": True, "node_id": "2"},
                        ],
                        "outputs": [{"name": "output_video", "type": "video/mp4", "node_id": "9"}],
                    }
                ),
                encoding="utf-8",
            )
            workflow_path.write_text(
                json.dumps(
                    {
                        "prompt": {
                            "1": {"class_type": "ApiInput", "inputs": {"value": ""}},
                            "2": {"class_type": "ApiInput", "inputs": {"value": ""}},
                            "9": {
                                "class_type": "ApiOutput",
                                "inputs": {"name": "output_video", "type": "video/mp4", "value": ""},
                            },
                        }
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
                    workflow_path=workflow_path,
                    state_db=root / "jobs.db",
                    app_id="wanimate",
                )
            )
            reference = app.job_store.create_file(
                content=b"png-bytes",
                media_type="image/png",
                source="uploaded",
                app_id="wanimate",
                original_name="reference.png",
            )
            driving = app.job_store.create_file(
                content=b"mp4-bytes",
                media_type="video/mp4",
                source="uploaded",
                app_id="wanimate",
                original_name="driving.mp4",
            )
            app.comfy_client = _FakeComfyClient(
                outputs={"9": {"value": ["fid_mock_output_video"]}},
            )

            body = json.dumps(
                {
                    "reference_image": reference.file_id,
                    "driving_video": driving.file_id,
                }
            ).encode("utf-8")
            run_request = (
                b"POST /run HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"x-api-key: secret\r\n"
                b"content-type: application/json\r\n"
                b"connection: close\r\n"
                + f"content-length: {len(body)}\r\n\r\n".encode("ascii")
                + body
            )
            status, _headers, _payload_body = self._round_trip(app, run_request)
            self.assertEqual(status, 202)

            for _ in range(20):
                if app.comfy_client.last_prompt_payload is not None:
                    break
                time.sleep(0.02)
            assert app.comfy_client.last_prompt_payload is not None
            prompt = app.comfy_client.last_prompt_payload["prompt"]
            self.assertEqual(prompt["1"]["inputs"]["value"], reference.file_id)
            self.assertEqual(prompt["2"]["inputs"]["value"], driving.file_id)


if __name__ == "__main__":
    unittest.main()
