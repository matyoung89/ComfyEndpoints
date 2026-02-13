from __future__ import annotations

import argparse
import json
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from comfy_endpoints.contracts.validators import parse_workflow_contract
from comfy_endpoints.gateway.comfy_client import ComfyClient, ComfyClientError
from comfy_endpoints.gateway.job_store import JobStore
from comfy_endpoints.gateway.prompt_mapper import (
    PromptMappingError,
    map_contract_payload_to_prompt,
)


@dataclass(slots=True)
class GatewayConfig:
    listen_host: str
    listen_port: int
    api_key: str
    comfy_url: str
    contract_path: Path
    workflow_path: Path
    state_db: Path


def is_public_route(method: str, path: str) -> bool:
    if method == "GET" and path == "/healthz":
        return True

    if method == "POST" and path == "/run":
        return True

    if method == "GET" and path.startswith("/jobs/"):
        return True

    return False


def is_authorized(provided_key: str, expected_key: str) -> bool:
    return bool(provided_key) and provided_key == expected_key


class GatewayApp:
    def __init__(self, config: GatewayConfig):
        self.config = config
        self.contract = parse_workflow_contract(config.contract_path)
        self.workflow = json.loads(config.workflow_path.read_text(encoding="utf-8"))
        self.job_store = JobStore(config.state_db)
        self.comfy_client = ComfyClient(config.comfy_url)

    def validate_payload(self, payload: dict) -> tuple[bool, str]:
        names = {field.name for field in self.contract.inputs}

        for field in self.contract.inputs:
            if field.required and field.name not in payload:
                return False, f"missing_required_input:{field.name}"

        extra = sorted(set(payload.keys()) - names)
        if extra:
            return False, f"unexpected_inputs:{','.join(extra)}"

        return True, "ok"


class GatewayHandler(BaseHTTPRequestHandler):
    app: GatewayApp

    def _json_response(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        provided = self.headers.get("x-api-key", "")
        return is_authorized(provided, self.app.config.api_key)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._json_response(HTTPStatus.OK, {"status": "ok"})
            return

        if self.path.startswith("/jobs/"):
            if not self._authorized():
                self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            job_id = self.path.split("/jobs/", 1)[1]
            record = self.app.job_store.get(job_id)
            if not record:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job_not_found"})
                return

            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": record.job_id,
                    "state": record.state,
                    "output": record.output_payload,
                    "error": record.error,
                },
            )
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/run":
            self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        if not self._authorized():
            self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return

        content_length = int(self.headers.get("content-length", "0"))
        raw_body = self.rfile.read(content_length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": "VALIDATION_ERROR", "detail": "invalid_json"},
            )
            return

        ok, detail = self.app.validate_payload(payload)
        if not ok:
            self._json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": "VALIDATION_ERROR", "detail": detail},
            )
            return

        job_id = self.app.job_store.create(payload)
        worker = threading.Thread(target=self._execute_job, args=(job_id, payload), daemon=True)
        worker.start()

        self._json_response(HTTPStatus.ACCEPTED, {"job_id": job_id, "state": "queued"})

    def _execute_job(self, job_id: str, payload: dict) -> None:
        try:
            self.app.job_store.mark_running(job_id)
            mapped_prompt = map_contract_payload_to_prompt(
                workflow_payload=self.app.workflow,
                contract=self.app.contract,
                input_payload=payload,
            )
            prompt_id = self.app.comfy_client.queue_prompt(mapped_prompt)
            self.app.job_store.mark_completed(
                job_id,
                {
                    "prompt_id": prompt_id,
                    "status": "submitted",
                },
            )
        except PromptMappingError as exc:
            self.app.job_store.mark_failed(job_id, f"VALIDATION_ERROR:{exc}")
        except ComfyClientError as exc:
            self.app.job_store.mark_failed(job_id, f"QUEUE_ERROR:{exc}")
        except Exception as exc:  # noqa: BLE001
            self.app.job_store.mark_failed(job_id, f"SYSTEM_ERROR:{exc}")


def run_gateway(config: GatewayConfig) -> None:
    app = GatewayApp(config)

    class BoundHandler(GatewayHandler):
        pass

    BoundHandler.app = app

    server = ThreadingHTTPServer((config.listen_host, config.listen_port), BoundHandler)
    server.serve_forever()


def main() -> int:
    parser = argparse.ArgumentParser(prog="comfy-endpoints-gateway")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=3000)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--comfy-url", default="http://127.0.0.1:8188")
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--workflow-path", required=True)
    parser.add_argument("--state-db", default="/var/lib/comfy_endpoints/jobs.db")
    args = parser.parse_args()

    run_gateway(
        GatewayConfig(
            listen_host=args.listen_host,
            listen_port=args.listen_port,
            api_key=args.api_key,
            comfy_url=args.comfy_url,
            contract_path=Path(args.contract_path),
            workflow_path=Path(args.workflow_path),
            state_db=Path(args.state_db),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
