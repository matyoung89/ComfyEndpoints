from __future__ import annotations

import argparse
import json
import os
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from comfy_endpoints.contracts.validators import parse_workflow_contract
from comfy_endpoints.gateway.comfy_client import ComfyClient, ComfyClientError
from comfy_endpoints.gateway.job_store import FileRecord, JobStore
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
    app_id: str | None = None


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
        self.app_id = config.app_id or os.getenv("COMFY_ENDPOINTS_APP_ID", "").strip() or None
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


MEDIA_PREFIXES = ("image/", "video/", "audio/", "file/")
SCALAR_TYPES = {"string", "integer", "number", "boolean", "object", "array"}


class OutputResolutionError(ValueError):
    pass


def _is_media_contract_type(type_name: str) -> bool:
    normalized = type_name.strip().lower()
    return any(normalized.startswith(prefix) for prefix in MEDIA_PREFIXES)


def _coerce_scalar_output(type_name: str, raw_value: object) -> object:
    normalized = type_name.strip().lower()
    if normalized == "string":
        if isinstance(raw_value, str):
            return raw_value
        return str(raw_value)
    if normalized == "integer":
        if isinstance(raw_value, bool):
            raise OutputResolutionError("OUTPUT_TYPE_ERROR:cannot_coerce_bool_to_integer")
        return int(raw_value)
    if normalized == "number":
        if isinstance(raw_value, bool):
            raise OutputResolutionError("OUTPUT_TYPE_ERROR:cannot_coerce_bool_to_number")
        return float(raw_value)
    if normalized == "boolean":
        if isinstance(raw_value, bool):
            return raw_value
        if isinstance(raw_value, str):
            lowered = raw_value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        if isinstance(raw_value, (int, float)):
            return bool(raw_value)
        raise OutputResolutionError("OUTPUT_TYPE_ERROR:cannot_coerce_to_boolean")
    if normalized == "object":
        if isinstance(raw_value, dict):
            return raw_value
        raise OutputResolutionError("OUTPUT_TYPE_ERROR:expected_object")
    if normalized == "array":
        if isinstance(raw_value, list):
            return raw_value
        raise OutputResolutionError("OUTPUT_TYPE_ERROR:expected_array")
    raise OutputResolutionError(f"OUTPUT_TYPE_ERROR:unsupported_type:{type_name}")


def _extract_prompt_history(history_payload: dict, prompt_id: str) -> dict | None:
    if prompt_id in history_payload and isinstance(history_payload[prompt_id], dict):
        return history_payload[prompt_id]

    for value in history_payload.values():
        if isinstance(value, dict) and isinstance(value.get("prompt_id"), str):
            if str(value.get("prompt_id")) == prompt_id:
                return value

    return None


def _is_prompt_history_terminal(prompt_history: dict) -> bool:
    status = prompt_history.get("status")
    if isinstance(status, dict):
        completed = status.get("completed")
        if isinstance(completed, bool) and completed:
            return True

        status_str = str(status.get("status_str", "")).strip().lower()
        if status_str in {"success", "succeeded", "completed", "failed", "error"}:
            return True

    return False


def _first_list_item(value: object) -> object | None:
    if isinstance(value, list) and value:
        return value[0]
    return None


def _node_scalar_value(node_output: dict) -> object:
    api_output = node_output.get("api_output")
    if isinstance(api_output, dict) and "value" in api_output:
        return api_output["value"]

    candidates: tuple[str, ...] = ("value", "values", "text", "result")
    for key in candidates:
        if key not in node_output:
            continue
        raw = node_output[key]
        first = _first_list_item(raw)
        if first is not None:
            return first
        return raw

    raise OutputResolutionError("OUTPUT_RESOLUTION_ERROR:missing_scalar_value")


def _node_media_descriptor(node_output: dict) -> dict[str, str]:
    media_lists: tuple[str, ...] = ("images", "videos", "audios", "files")
    for key in media_lists:
        raw = node_output.get(key)
        first = _first_list_item(raw)
        if not isinstance(first, dict):
            continue

        filename = str(first.get("filename", "")).strip()
        subfolder = str(first.get("subfolder", "")).strip()
        media_type = str(first.get("type", "")).strip()
        if not filename:
            continue
        return {
            "filename": filename,
            "subfolder": subfolder,
            "type": media_type,
        }

    raise OutputResolutionError("OUTPUT_RESOLUTION_ERROR:missing_media_descriptor")


def _prompt_graph_from_workflow(workflow_payload: dict) -> dict[str, dict]:
    prompt = workflow_payload.get("prompt")
    if isinstance(prompt, dict):
        normalized: dict[str, dict] = {}
        for node_id, node in prompt.items():
            if isinstance(node, dict):
                normalized[str(node_id)] = node
        return normalized

    if all(isinstance(key, str) and isinstance(value, dict) for key, value in workflow_payload.items()):
        if any("class_type" in value for value in workflow_payload.values()):
            return workflow_payload

    return {}


def _api_output_linked_source_node_id(workflow_payload: dict, node_id: str) -> str | None:
    prompt = _prompt_graph_from_workflow(workflow_payload)
    node = prompt.get(node_id)
    if not isinstance(node, dict):
        return None

    inputs = node.get("inputs")
    if not isinstance(inputs, dict):
        return None

    for key in ("image", "value"):
        link = inputs.get(key)
        if isinstance(link, list) and link:
            source_node_id = link[0]
            if isinstance(source_node_id, (str, int)):
                return str(source_node_id)
    return None

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

    @staticmethod
    def _file_payload(record: FileRecord) -> dict:
        return {
            "file_id": record.file_id,
            "media_type": record.media_type,
            "size_bytes": record.size_bytes,
            "sha256": record.sha256_hex,
            "source": record.source,
            "app_id": record.app_id,
            "original_name": record.original_name,
            "created_at": record.created_at,
            "download_path": f"/files/{record.file_id}/download",
        }

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/healthz":
            self._json_response(HTTPStatus.OK, {"status": "ok"})
            return

        if path == "/contract":
            if not self._authorized():
                self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            self._json_response(
                HTTPStatus.OK,
                {
                    "contract_id": self.app.contract.contract_id,
                    "version": self.app.contract.version,
                    "inputs": [
                        {
                            "name": field.name,
                            "type": field.type,
                            "required": field.required,
                            "node_id": field.node_id,
                        }
                        for field in self.app.contract.inputs
                    ],
                    "outputs": [
                        {
                            "name": field.name,
                            "type": field.type,
                            "node_id": field.node_id,
                        }
                        for field in self.app.contract.outputs
                    ],
                },
            )
            return

        if path == "/files":
            if not self._authorized():
                self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            try:
                limit = int(query.get("limit", ["50"])[0])
            except ValueError:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid_limit"})
                return

            cursor = query.get("cursor", [None])[0]
            media_type = query.get("media_type", [None])[0]
            source = query.get("source", [None])[0]
            app_id = query.get("app_id", [None])[0]
            try:
                files, next_cursor = self.app.job_store.list_files(
                    limit=limit,
                    cursor=cursor,
                    media_type=media_type,
                    source=source,
                    app_id=app_id,
                )
            except ValueError:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid_cursor"})
                return

            self._json_response(
                HTTPStatus.OK,
                {
                    "items": [self._file_payload(item) for item in files],
                    "next_cursor": next_cursor,
                },
            )
            return

        if path.startswith("/files/"):
            if not self._authorized():
                self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            suffix = path.split("/files/", 1)[1]
            if suffix.endswith("/download"):
                file_id = suffix[: -len("/download")]
                record = self.app.job_store.get_file(file_id)
                if not record or not record.storage_path.exists():
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "file_not_found"})
                    return

                content = record.storage_path.read_bytes()
                filename = record.original_name or record.storage_path.name
                self.send_response(HTTPStatus.OK)
                self.send_header("content-type", record.media_type or "application/octet-stream")
                self.send_header("content-length", str(len(content)))
                self.send_header(
                    "content-disposition",
                    f'attachment; filename="{filename}"',
                )
                self.end_headers()
                self.wfile.write(content)
                return

            file_id = suffix
            record = self.app.job_store.get_file(file_id)
            if not record:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "file_not_found"})
                return
            self._json_response(HTTPStatus.OK, self._file_payload(record))
            return

        if path.startswith("/jobs/"):
            if not self._authorized():
                self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            job_id = path.split("/jobs/", 1)[1]
            record = self.app.job_store.get(job_id)
            if not record:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job_not_found"})
                return

            output_payload = record.output_payload
            if record.state == "completed":
                artifact_outputs = self.app.job_store.read_output_artifacts(job_id)
                if artifact_outputs:
                    if isinstance(output_payload, dict):
                        output_payload = dict(output_payload)
                    else:
                        output_payload = {}

                    existing_result = output_payload.get("result")
                    if isinstance(existing_result, dict):
                        output_payload["result"] = artifact_outputs
                    else:
                        output_payload = {"result": artifact_outputs}

            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": record.job_id,
                    "state": record.state,
                    "output": output_payload,
                    "error": record.error,
                },
            )
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/files":
            if not self._authorized():
                self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return

            content_length = int(self.headers.get("content-length", "0"))
            if content_length <= 0:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "empty_file"})
                return
            payload = self.rfile.read(content_length)
            media_type = self.headers.get("content-type", "application/octet-stream")
            original_name = self.headers.get("x-file-name", "")
            app_id = self.headers.get("x-app-id", "").strip() or None
            try:
                record = self.app.job_store.create_file(
                    content=payload,
                    media_type=media_type,
                    source="uploaded",
                    app_id=app_id,
                    original_name=original_name,
                )
            except ValueError as exc:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self._json_response(HTTPStatus.CREATED, self._file_payload(record))
            return

        if path != "/run":
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
                job_id=job_id,
            )
            prompt_id = self.app.comfy_client.queue_prompt(mapped_prompt)
            result = self._resolve_contract_outputs(job_id)
            self.app.job_store.mark_completed(
                job_id,
                {
                    "prompt_id": prompt_id,
                    "status": "completed",
                    "result": result,
                },
            )
        except PromptMappingError as exc:
            self.app.job_store.mark_failed(job_id, f"VALIDATION_ERROR:{exc}")
        except OutputResolutionError as exc:
            self.app.job_store.mark_failed(job_id, str(exc))
        except ComfyClientError as exc:
            self.app.job_store.mark_failed(job_id, f"QUEUE_ERROR:{exc}")
        except Exception as exc:  # noqa: BLE001
            self.app.job_store.mark_failed(job_id, f"SYSTEM_ERROR:{exc}")

    def _resolve_contract_outputs(self, job_id: str) -> dict[str, object]:
        timeout_seconds = float(os.getenv("COMFY_ENDPOINTS_OUTPUT_TIMEOUT_SECONDS", "180"))
        poll_seconds = float(os.getenv("COMFY_ENDPOINTS_OUTPUT_POLL_SECONDS", "1.5"))
        deadline = time.time() + timeout_seconds
        expected_output_names = [field.name for field in self.app.contract.outputs]

        while time.time() < deadline:
            artifacts = self.app.job_store.read_output_artifacts(job_id)
            missing = [name for name in expected_output_names if name not in artifacts]
            if missing:
                time.sleep(max(0.2, poll_seconds))
                continue

            result: dict[str, object] = {}
            for field in self.app.contract.outputs:
                raw_value = artifacts[field.name]
                if _is_media_contract_type(field.type):
                    if not isinstance(raw_value, str) or not raw_value.strip():
                        raise OutputResolutionError(
                            f"OUTPUT_TYPE_ERROR:media_output_must_be_file_id:{field.name}"
                        )
                    result[field.name] = raw_value.strip()
                    continue
                result[field.name] = _coerce_scalar_output(field.type, raw_value)

            return result

        raise OutputResolutionError(
            "OUTPUT_TIMEOUT:missing_artifacts:" + ",".join(expected_output_names)
        )


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
    parser.add_argument("--app-id", default=None)
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
            app_id=str(args.app_id).strip() if args.app_id else None,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
