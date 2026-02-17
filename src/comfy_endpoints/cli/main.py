from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from comfy_endpoints.runtime import DeploymentService
from comfy_endpoints.runtime.state_store import DeploymentStore
from comfy_endpoints.utils.env_loader import load_local_env


STATIC_ROOT_COMMANDS = {
    "init",
    "validate",
    "deploy",
    "status",
    "logs",
    "destroy",
    "files",
    "jobs",
    "endpoints",
    "invoke",
    "completion",
    "_complete",
}


def _default_state_dir() -> Path:
    return Path.cwd() / ".comfy_endpoints"


def _cmd_init(args: argparse.Namespace) -> int:
    app_dir = Path(args.app_dir).resolve()
    app_dir.mkdir(parents=True, exist_ok=True)

    workflow_file = app_dir / "workflow.json"
    contract_file = app_dir / "workflow.contract.json"
    app_spec_file = app_dir / "app.json"

    if not workflow_file.exists():
        workflow_file.write_text(
            json.dumps(
                {
                    "prompt": {
                        "1": {
                            "class_type": "ApiInput",
                            "inputs": {
                                "name": "prompt",
                                "type": "string",
                                "required": True,
                                "value": "",
                            },
                        },
                        "2": {
                            "class_type": "ApiOutput",
                            "inputs": {
                                "name": "image",
                                "type": "image/png",
                                "value": "",
                            },
                        },
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    if not contract_file.exists():
        contract_payload = {
            "contract_id": f"{app_dir.name}-contract",
            "version": "v1",
            "inputs": [
                {
                    "name": "prompt",
                    "type": "string",
                    "required": True,
                    "node_id": "1",
                }
            ],
            "outputs": [
                {
                    "name": "image",
                    "type": "image/png",
                    "node_id": "2",
                }
            ],
        }
        contract_file.write_text(json.dumps(contract_payload, indent=2), encoding="utf-8")

    if not app_spec_file.exists():
        app_spec_file.write_text(
            json.dumps(
                {
                    "app_id": app_dir.name,
                    "version": "v1",
                    "workflow_path": "./workflow.json",
                    "provider": "runpod",
                    "gpu_profile": "A10G",
                    "regions": ["US"],
                    "env": {
                        "COMFY_HEADLESS": "1",
                        "COMFY_ENDPOINTS_API_KEY": "demo-change-me",
                    },
                    "endpoint": {
                        "name": "run",
                        "mode": "async",
                        "auth_mode": "api_key",
                        "timeout_seconds": 300,
                        "max_payload_mb": 10,
                    },
                    "cache_policy": {
                        "watch_paths": ["/opt/comfy/models"],
                        "min_file_size_mb": 100,
                        "symlink_targets": ["/opt/comfy/models"],
                    },
                    "compute_policy": {
                        "min_vram_gb": 24,
                        "min_ram_per_gpu_gb": 64,
                        "gpu_count": 1,
                    },
                    "build": {
                        "comfy_version": "0.3.26",
                        "image_repository": "ghcr.io/matyoung89/comfy-endpoints-golden",
                        "base_image_repository": "ghcr.io/matyoung89/comfy-endpoints-comfybase",
                        "base_dockerfile_path": "docker/Dockerfile.comfybase",
                        "dockerfile_path": "docker/Dockerfile.golden",
                        "build_context": ".",
                        "base_build_context": ".",
                        "container_registry_auth_id": "",
                        "plugins": [
                            {
                                "repo": "https://github.com/comfyanonymous/ComfyUI",
                                "ref": "master",
                            }
                        ],
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    print(f"Initialized app scaffold at {app_dir}")
    return 0


def _service(state_dir: str | None) -> DeploymentService:
    state_root = Path(state_dir).resolve() if state_dir else _default_state_dir()
    return DeploymentService(state_dir=state_root)


def _store(state_dir: str | None) -> DeploymentStore:
    state_root = Path(state_dir).resolve() if state_dir else _default_state_dir()
    return DeploymentStore(state_dir=state_root)


def _app_api_key(app_id: str) -> str:
    normalized = app_id.upper().replace("-", "_")
    app_key = os.getenv(f"COMFY_ENDPOINTS_API_KEY_{normalized}", "").strip()
    if app_key:
        return app_key
    return os.getenv("COMFY_ENDPOINTS_API_KEY", "").strip()


def _request_json(
    endpoint_url: str,
    app_id: str,
    path: str,
    query: dict[str, str] | None = None,
) -> dict:
    url = f"{endpoint_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    headers = {
        "accept": "application/json",
        "user-agent": "comfy-endpoints/0.1 cli",
    }
    api_key = _app_api_key(app_id)
    if api_key:
        headers["x-api-key"] = api_key
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed request to {url}: {exc.reason}") from exc


def _request_json_post(
    endpoint_url: str,
    app_id: str,
    path: str,
    payload: dict[str, Any],
) -> dict:
    url = f"{endpoint_url.rstrip('/')}{path}"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": "comfy-endpoints/0.1 cli",
    }
    api_key = _app_api_key(app_id)
    if api_key:
        headers["x-api-key"] = api_key

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed request to {url}: {exc.reason}") from exc


def _request_download(
    endpoint_url: str,
    app_id: str,
    path: str,
    out_path: Path,
) -> dict[str, str]:
    url = f"{endpoint_url.rstrip('/')}{path}"
    headers = {
        "accept": "*/*",
        "user-agent": "comfy-endpoints/0.1 cli",
    }
    api_key = _app_api_key(app_id)
    if api_key:
        headers["x-api-key"] = api_key

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            payload = response.read()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(payload)
            return {
                "content_type": response.headers.get("content-type", "application/octet-stream"),
                "content_length": str(len(payload)),
            }
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed request to {url}: {exc.reason}") from exc


def _request_upload(
    endpoint_url: str,
    app_id: str,
    in_path: Path,
    media_type: str | None = None,
    file_name: str | None = None,
) -> dict:
    if not in_path.exists() or not in_path.is_file():
        raise RuntimeError(f"Upload file not found: {in_path}")

    payload = in_path.read_bytes()
    if not payload:
        raise RuntimeError(f"Upload file is empty: {in_path}")

    url = f"{endpoint_url.rstrip('/')}/files"
    headers = {
        "content-type": media_type or "application/octet-stream",
        "x-file-name": file_name or in_path.name,
        "x-app-id": app_id,
        "user-agent": "comfy-endpoints/0.1 cli",
    }
    api_key = _app_api_key(app_id)
    if api_key:
        headers["x-api-key"] = api_key

    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed request to {url}: {exc.reason}") from exc


def _resolve_targets(state_dir: str | None, app_id: str | None = None) -> list[tuple[str, str]]:
    store = _store(state_dir)
    if app_id:
        record = store.get(app_id)
        if not record:
            raise RuntimeError(f"No deployment record found for app_id={app_id}")
        if not record.endpoint_url:
            raise RuntimeError(f"Missing endpoint_url for app_id={app_id}")
        return [(record.app_id, record.endpoint_url)]

    records = [item for item in store.list_records() if item.endpoint_url]
    if not records:
        raise RuntimeError("No deployment records with endpoint_url found")
    return [(item.app_id, item.endpoint_url or "") for item in records]


def _resolve_one_target(state_dir: str | None, app_id: str) -> tuple[str, str]:
    targets = _resolve_targets(state_dir, app_id)
    if len(targets) != 1:
        raise RuntimeError(f"Expected exactly one deployment for app_id={app_id}")
    return targets[0]


def _discover_contract(app_id: str, endpoint_url: str) -> dict[str, Any]:
    contract = _request_json(endpoint_url=endpoint_url, app_id=app_id, path="/contract")
    if not isinstance(contract, dict) or "inputs" not in contract:
        raise RuntimeError("Invalid contract payload from endpoint")
    inputs = contract.get("inputs")
    if not isinstance(inputs, list):
        raise RuntimeError("Contract inputs must be an array")
    return contract


def _is_media_contract_type(type_name: str) -> bool:
    normalized = type_name.strip().lower()
    if normalized.startswith("image/"):
        return True
    if normalized.startswith("video/"):
        return True
    if normalized.startswith("audio/"):
        return True
    if normalized.startswith("file/"):
        return True
    return False


def _coerce_scalar(type_name: str, raw_value: str) -> Any:
    normalized = type_name.strip().lower()
    if normalized == "string":
        return raw_value
    if normalized == "integer":
        return int(raw_value)
    if normalized == "number":
        return float(raw_value)
    if normalized == "boolean":
        lowered = raw_value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"Invalid boolean: {raw_value}")
    if normalized in {"object", "array"}:
        return json.loads(raw_value)
    return raw_value


def _parse_dynamic_inputs(
    dynamic_args: list[str],
    contract: dict[str, Any],
    app_id: str,
    endpoint_url: str,
    input_json: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if input_json:
        parsed = json.loads(input_json)
        if not isinstance(parsed, dict):
            raise RuntimeError("--input-json must decode to an object")
        payload.update(parsed)

    contract_inputs = contract.get("inputs", [])
    inputs_by_name: dict[str, dict[str, Any]] = {}
    for item in contract_inputs:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        inputs_by_name[name] = item

    idx = 0
    seen_media_inputs: dict[str, str] = {}
    while idx < len(dynamic_args):
        token = dynamic_args[idx]
        if token in {"-h", "--help"}:
            raise RuntimeError("Dynamic help: run `comfy-endpoints endpoints describe <app_id>`")
        if not token.startswith("--input-"):
            raise RuntimeError(f"Unsupported invoke argument: {token}")

        key = token[2:]
        value: str
        if "=" in key:
            key, value = key.split("=", 1)
        else:
            if idx + 1 >= len(dynamic_args):
                raise RuntimeError(f"Missing value for argument: {token}")
            value = dynamic_args[idx + 1]
            idx += 1

        if key.endswith("-file"):
            input_name = key[len("input-") : -len("-file")]
            field = inputs_by_name.get(input_name)
            if not field:
                raise RuntimeError(f"Unknown input field: {input_name}")
            field_type = str(field.get("type", "string"))
            if not _is_media_contract_type(field_type):
                raise RuntimeError(f"Input '{input_name}' is not a media type and does not support -file")
            prior = seen_media_inputs.get(input_name)
            if prior and prior != "file":
                raise RuntimeError(f"Input '{input_name}' accepts either -file or -id, not both")
            upload_response = _request_upload(
                endpoint_url=endpoint_url,
                app_id=app_id,
                in_path=Path(value).resolve(),
                media_type=field_type,
            )
            file_id = str(upload_response.get("file_id", "")).strip()
            if not file_id:
                raise RuntimeError(f"Upload did not return file_id for input '{input_name}'")
            payload[input_name] = file_id
            seen_media_inputs[input_name] = "file"
        elif key.endswith("-id"):
            input_name = key[len("input-") : -len("-id")]
            field = inputs_by_name.get(input_name)
            if not field:
                raise RuntimeError(f"Unknown input field: {input_name}")
            field_type = str(field.get("type", "string"))
            if not _is_media_contract_type(field_type):
                raise RuntimeError(f"Input '{input_name}' is not a media type and does not support -id")
            prior = seen_media_inputs.get(input_name)
            if prior and prior != "id":
                raise RuntimeError(f"Input '{input_name}' accepts either -file or -id, not both")
            payload[input_name] = value
            seen_media_inputs[input_name] = "id"
        else:
            input_name = key[len("input-") :]
            field = inputs_by_name.get(input_name)
            if not field:
                raise RuntimeError(f"Unknown input field: {input_name}")
            field_type = str(field.get("type", "string"))
            if _is_media_contract_type(field_type):
                raise RuntimeError(
                    f"Input '{input_name}' is media and requires --input-{input_name}-file or --input-{input_name}-id"
                )
            try:
                payload[input_name] = _coerce_scalar(field_type, value)
            except (ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"Invalid value for input '{input_name}' ({field_type}): {value}") from exc

        idx += 1

    missing_required: list[str] = []
    for item in contract_inputs:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        required = bool(item.get("required", False))
        if required and name not in payload:
            missing_required.append(name)
    if missing_required:
        raise RuntimeError(f"Missing required inputs: {', '.join(sorted(missing_required))}")

    extras = sorted(set(payload.keys()) - set(inputs_by_name.keys()))
    if extras:
        raise RuntimeError(f"Unknown inputs in payload: {', '.join(extras)}")
    return payload


def _poll_job_until_terminal(
    endpoint_url: str,
    app_id: str,
    job_id: str,
    timeout_seconds: int,
    poll_seconds: float,
) -> dict[str, Any]:
    terminal_states = {
        "completed",
        "succeeded",
        "failed",
        "error",
        "canceled",
        "cancelled",
        "timed_out",
        "timeout",
    }
    deadline = time.time() + timeout_seconds
    last_response: dict[str, Any] | None = None
    last_error: str | None = None
    while True:
        now = time.time()
        if now > deadline:
            state_hint = ""
            if last_response is not None:
                state_hint = f", last_state={last_response.get('state', '')}"
            error_hint = f", last_error={last_error}" if last_error else ""
            raise RuntimeError(f"Timed out waiting for job_id={job_id}{state_hint}{error_hint}")

        try:
            response = _request_json(endpoint_url=endpoint_url, app_id=app_id, path=f"/jobs/{job_id}")
            last_response = response
            last_error = None
        except RuntimeError as exc:
            last_error = str(exc)
            time.sleep(max(0.2, poll_seconds))
            continue

        state = str(response.get("state", "")).strip().lower()
        if state in terminal_states:
            return response
        time.sleep(max(0.2, poll_seconds))


def _cmd_validate(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    app_id, contract_id = svc.validate(Path(args.app_spec).resolve())
    print(json.dumps({"app_id": app_id, "contract_id": contract_id, "result": "ok"}, indent=2))
    return 0


def _cmd_deploy(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    record = svc.deploy(
        Path(args.app_spec).resolve(),
        keep_existing=bool(args.keep_existing),
        progress_callback=lambda msg: print(f"[deploy] {msg}", file=sys.stderr),
    )
    print(
        json.dumps(
            {
                "app_id": record.app_id,
                "deployment_id": record.deployment_id,
                "state": record.state.value,
                "endpoint_url": record.endpoint_url,
                "api_key_ref": record.api_key_ref,
            },
            indent=2,
        )
    )
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    record = svc.status(args.app_id)
    print(
        json.dumps(
            {
                "app_id": record.app_id,
                "deployment_id": record.deployment_id,
                "state": record.state.value,
                "endpoint_url": record.endpoint_url,
                "metadata": record.metadata,
            },
            indent=2,
        )
    )
    return 0


def _cmd_logs(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    print(svc.logs(args.app_id))
    return 0


def _cmd_destroy(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    svc.destroy(Path(args.app_spec).resolve())
    print(json.dumps({"result": "destroyed"}, indent=2))
    return 0


def _cmd_files_list(args: argparse.Namespace) -> int:
    targets = _resolve_targets(args.state_dir, args.app_id)
    if len(targets) > 1 and args.cursor:
        raise RuntimeError("--cursor requires --app-id when multiple endpoints are queried")

    merged: dict[str, dict] = {}
    next_cursor = None
    for current_app_id, endpoint_url in targets:
        response = _request_json(
            endpoint_url=endpoint_url,
            app_id=current_app_id,
            path="/files",
            query={
                key: value
                for key, value in {
                    "limit": str(args.limit),
                    "cursor": args.cursor or "",
                    "media_type": args.media_type or "",
                    "source": args.source or "",
                    "app_id": args.app_id_filter or "",
                }.items()
                if value
            },
        )
        for item in response.get("items", []):
            if isinstance(item, dict) and item.get("file_id"):
                merged[str(item["file_id"])] = item
        if len(targets) == 1:
            next_cursor = response.get("next_cursor")

    items = list(merged.values())
    items.sort(key=lambda row: str(row.get("created_at", "")), reverse=True)
    payload: dict[str, object] = {"items": items[: args.limit]}
    if next_cursor:
        payload["next_cursor"] = next_cursor
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_files_get(args: argparse.Namespace) -> int:
    targets = _resolve_targets(args.state_dir, args.app_id)
    errors: list[str] = []
    for current_app_id, endpoint_url in targets:
        try:
            response = _request_json(
                endpoint_url=endpoint_url,
                app_id=current_app_id,
                path=f"/files/{args.file_id}",
            )
            print(json.dumps(response, indent=2))
            return 0
        except RuntimeError as exc:
            errors.append(f"{current_app_id}: {exc}")
    raise RuntimeError(f"Unable to resolve file_id={args.file_id}. " + " | ".join(errors))


def _cmd_files_download(args: argparse.Namespace) -> int:
    targets = _resolve_targets(args.state_dir, args.app_id)
    errors: list[str] = []
    out_path = Path(args.out).resolve()
    for current_app_id, endpoint_url in targets:
        try:
            info = _request_download(
                endpoint_url=endpoint_url,
                app_id=current_app_id,
                path=f"/files/{args.file_id}/download",
                out_path=out_path,
            )
            print(
                json.dumps(
                    {
                        "file_id": args.file_id,
                        "out": str(out_path),
                        "content_type": info["content_type"],
                        "content_length": info["content_length"],
                    },
                    indent=2,
                )
            )
            return 0
        except RuntimeError as exc:
            errors.append(f"{current_app_id}: {exc}")
    raise RuntimeError(f"Unable to download file_id={args.file_id}. " + " | ".join(errors))


def _cmd_files_upload(args: argparse.Namespace) -> int:
    targets = _resolve_targets(args.state_dir, args.app_id)
    if len(targets) != 1:
        raise RuntimeError("files upload requires exactly one target endpoint; use --app-id")
    current_app_id, endpoint_url = targets[0]
    response = _request_upload(
        endpoint_url=endpoint_url,
        app_id=current_app_id,
        in_path=Path(args.in_path).resolve(),
        media_type=args.media_type,
        file_name=args.file_name,
    )
    print(json.dumps(response, indent=2))
    return 0


def _cmd_jobs_get(args: argparse.Namespace) -> int:
    app_id, endpoint_url = _resolve_one_target(args.state_dir, args.app_id)
    response = _request_json(
        endpoint_url=endpoint_url,
        app_id=app_id,
        path=f"/jobs/{args.job_id}",
    )
    print(json.dumps(response, indent=2))
    return 0


def _cmd_endpoints_list(args: argparse.Namespace) -> int:
    records = _store(args.state_dir).list_records()
    payload_items: list[dict[str, Any]] = []
    for record in records:
        if not record.endpoint_url:
            continue
        health_status = "unknown"
        detail = ""
        try:
            health = _request_json(endpoint_url=record.endpoint_url, app_id=record.app_id, path="/healthz")
            if str(health.get("status", "")).lower() == "ok":
                health_status = "healthy"
            else:
                health_status = "unhealthy"
                detail = str(health)
        except RuntimeError as exc:
            health_status = "unreachable"
            detail = str(exc)

        payload_items.append(
            {
                "app_id": record.app_id,
                "deployment_id": record.deployment_id,
                "endpoint_url": record.endpoint_url,
                "state": record.state.value,
                "health": health_status,
                "detail": detail,
            }
        )

    payload_items.sort(key=lambda row: row["app_id"])
    print(json.dumps({"items": payload_items}, indent=2))
    return 0


def _cmd_endpoints_describe(args: argparse.Namespace) -> int:
    app_id, endpoint_url = _resolve_one_target(args.state_dir, args.app_id)
    contract = _discover_contract(app_id, endpoint_url)
    print(
        json.dumps(
            {
                "app_id": app_id,
                "endpoint_url": endpoint_url,
                "contract": contract,
            },
            indent=2,
        )
    )
    return 0


def _cmd_invoke(args: argparse.Namespace) -> int:
    app_id, endpoint_url = _resolve_one_target(args.state_dir, args.app_id)
    contract = _discover_contract(app_id, endpoint_url)
    payload = _parse_dynamic_inputs(
        dynamic_args=args.dynamic_args,
        contract=contract,
        app_id=app_id,
        endpoint_url=endpoint_url,
        input_json=args.input_json,
    )
    run_response = _request_json_post(endpoint_url=endpoint_url, app_id=app_id, path="/run", payload=payload)

    if not args.wait:
        print(
            json.dumps(
                {
                    "app_id": app_id,
                    "endpoint_url": endpoint_url,
                    "request": payload,
                    "response": run_response,
                },
                indent=2,
            )
        )
        return 0

    job_id = str(run_response.get("job_id", "")).strip()
    if not job_id:
        raise RuntimeError(f"Expected job_id in /run response: {run_response}")
    terminal = _poll_job_until_terminal(
        endpoint_url=endpoint_url,
        app_id=app_id,
        job_id=job_id,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
    )
    print(
        json.dumps(
            {
                "app_id": app_id,
                "endpoint_url": endpoint_url,
                "request": payload,
                "run": run_response,
                "job": terminal,
            },
            indent=2,
        )
    )
    return 0


def _app_ids(state_dir: str | None) -> list[str]:
    records = _store(state_dir).list_records()
    values = sorted({record.app_id for record in records})
    return values


def _invoke_flags_for_app(state_dir: str | None, app_id: str) -> list[str]:
    try:
        current_app_id, endpoint_url = _resolve_one_target(state_dir, app_id)
        contract = _discover_contract(current_app_id, endpoint_url)
    except RuntimeError:
        return []

    flags: list[str] = ["--wait", "--timeout-seconds", "--poll-seconds", "--input-json"]
    for item in contract.get("inputs", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        type_name = str(item.get("type", "string"))
        if not name:
            continue
        if _is_media_contract_type(type_name):
            flags.append(f"--input-{name}-file")
            flags.append(f"--input-{name}-id")
        else:
            flags.append(f"--input-{name}")
    return sorted(set(flags))


def _complete_candidates(state_dir: str | None, words: list[str], index: int) -> list[str]:
    if index <= 1:
        return sorted(STATIC_ROOT_COMMANDS - {"_complete"}) + _app_ids(state_dir)

    if not words:
        return []

    root = words[1] if len(words) > 1 else ""
    app_ids = set(_app_ids(state_dir))
    if root in app_ids:
        return _invoke_flags_for_app(state_dir, root)

    if root == "invoke" and len(words) >= 3:
        app_id = words[2]
        if app_id in app_ids:
            return _invoke_flags_for_app(state_dir, app_id)

    if root == "files":
        if index == 2:
            return ["list", "get", "download", "upload"]
        if len(words) >= 3 and words[2] == "list":
            return ["--limit", "--cursor", "--media-type", "--source", "--app-id", "--app-id-filter"]
        if len(words) >= 3 and words[2] == "download":
            return ["--out", "--app-id"]
        if len(words) >= 3 and words[2] == "upload":
            return ["--in", "--media-type", "--file-name", "--app-id"]

    if root == "jobs":
        if index == 2:
            return ["get"]
        if index == 3:
            return sorted(app_ids)

    if root == "endpoints" and index == 2:
        return ["list", "describe"]

    return []


def _cmd_complete(args: argparse.Namespace) -> int:
    candidates = _complete_candidates(args.state_dir, args.words, args.index)
    prefix = ""
    if 0 <= args.index < len(args.words):
        prefix = args.words[args.index]
    for candidate in candidates:
        if prefix and not candidate.startswith(prefix):
            continue
        print(candidate)
    return 0


def _cmd_completion(args: argparse.Namespace) -> int:
    if args.shell == "bash":
        print(
            """
_comfy_endpoints_complete() {
  local cur prev cword
  cur="${COMP_WORDS[COMP_CWORD]}"
  COMPREPLY=( $(comfy-endpoints _complete --index "${COMP_CWORD}" --words "${COMP_WORDS[@]}") )
}
complete -F _comfy_endpoints_complete comfy-endpoints
""".strip()
        )
        return 0

    if args.shell == "zsh":
        print(
            """
#compdef comfy-endpoints

_comfy_endpoints_complete() {
  local -a suggestions
  suggestions=(${(@f)$(comfy-endpoints _complete --index ${CURRENT} --words ${words[@]})})
  _describe 'values' suggestions
}

compdef _comfy_endpoints_complete comfy-endpoints
""".strip()
        )
        return 0

    raise RuntimeError(f"Unsupported shell: {args.shell}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="comfy-endpoints")
    parser.add_argument("--state-dir", default=None)

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_cmd = subparsers.add_parser("init", help="Initialize a new ComfyEndpoints app scaffold")
    init_cmd.add_argument("app_dir")
    init_cmd.set_defaults(func=_cmd_init)

    validate_cmd = subparsers.add_parser("validate", help="Validate app spec and workflow contract")
    validate_cmd.add_argument("app_spec")
    validate_cmd.set_defaults(func=_cmd_validate)

    deploy_cmd = subparsers.add_parser("deploy", help="Deploy the app to configured provider")
    deploy_cmd.add_argument("app_spec")
    deploy_cmd.add_argument(
        "--keep-existing",
        action="store_true",
        default=False,
        help="Skip automatic destroy of existing deployments for the app before deploying",
    )
    deploy_cmd.set_defaults(func=_cmd_deploy)

    status_cmd = subparsers.add_parser("status", help="Check deployment status")
    status_cmd.add_argument("app_id")
    status_cmd.set_defaults(func=_cmd_status)

    logs_cmd = subparsers.add_parser("logs", help="Fetch deployment logs summary")
    logs_cmd.add_argument("app_id")
    logs_cmd.set_defaults(func=_cmd_logs)

    destroy_cmd = subparsers.add_parser("destroy", help="Destroy deployment")
    destroy_cmd.add_argument("app_spec")
    destroy_cmd.set_defaults(func=_cmd_destroy)

    endpoints_cmd = subparsers.add_parser("endpoints", help="Discover and inspect deployed endpoints")
    endpoints_subparsers = endpoints_cmd.add_subparsers(dest="endpoints_command", required=True)

    endpoints_list_cmd = endpoints_subparsers.add_parser("list", help="List known endpoints")
    endpoints_list_cmd.set_defaults(func=_cmd_endpoints_list)

    endpoints_describe_cmd = endpoints_subparsers.add_parser(
        "describe", help="Show endpoint contract and typed inputs"
    )
    endpoints_describe_cmd.add_argument("app_id")
    endpoints_describe_cmd.set_defaults(func=_cmd_endpoints_describe)

    invoke_cmd = subparsers.add_parser("invoke", help="Invoke one endpoint by app_id")
    invoke_cmd.add_argument("app_id")
    invoke_cmd.add_argument("--input-json", default=None)
    invoke_cmd.add_argument("--wait", action="store_true", default=False)
    invoke_cmd.add_argument("--timeout-seconds", type=int, default=180)
    invoke_cmd.add_argument("--poll-seconds", type=float, default=2.0)
    invoke_cmd.set_defaults(func=_cmd_invoke)

    files_cmd = subparsers.add_parser("files", help="List, inspect, and download remote files")
    files_subparsers = files_cmd.add_subparsers(dest="files_command", required=True)

    files_list_cmd = files_subparsers.add_parser("list", help="List remote files")
    files_list_cmd.add_argument("--limit", type=int, default=50)
    files_list_cmd.add_argument("--cursor", default=None)
    files_list_cmd.add_argument("--media-type", default=None)
    files_list_cmd.add_argument("--source", default=None, choices=["uploaded", "generated"])
    files_list_cmd.add_argument("--app-id", default=None, help="Target one deployed app endpoint")
    files_list_cmd.add_argument(
        "--app-id-filter",
        default=None,
        help="Filter files by owning app_id in the remote file registry",
    )
    files_list_cmd.set_defaults(func=_cmd_files_list)

    files_get_cmd = files_subparsers.add_parser("get", help="Fetch metadata for one file_id")
    files_get_cmd.add_argument("file_id")
    files_get_cmd.add_argument("--app-id", default=None, help="Optional app endpoint hint")
    files_get_cmd.set_defaults(func=_cmd_files_get)

    files_download_cmd = files_subparsers.add_parser("download", help="Download a file by file_id")
    files_download_cmd.add_argument("file_id")
    files_download_cmd.add_argument("--out", required=True)
    files_download_cmd.add_argument("--app-id", default=None, help="Optional app endpoint hint")
    files_download_cmd.set_defaults(func=_cmd_files_download)

    files_upload_cmd = files_subparsers.add_parser("upload", help="Upload a local file to a deployed endpoint")
    files_upload_cmd.add_argument("--in", dest="in_path", required=True)
    files_upload_cmd.add_argument("--media-type", default=None)
    files_upload_cmd.add_argument("--file-name", default=None)
    files_upload_cmd.add_argument("--app-id", default=None, help="Required when multiple endpoints are deployed")
    files_upload_cmd.set_defaults(func=_cmd_files_upload)

    jobs_cmd = subparsers.add_parser("jobs", help="Query jobs for a deployed endpoint")
    jobs_subparsers = jobs_cmd.add_subparsers(dest="jobs_command", required=True)

    jobs_get_cmd = jobs_subparsers.add_parser("get", help="Fetch status/details for one job_id")
    jobs_get_cmd.add_argument("app_id")
    jobs_get_cmd.add_argument("job_id")
    jobs_get_cmd.set_defaults(func=_cmd_jobs_get)

    completion_cmd = subparsers.add_parser("completion", help="Generate shell completion script")
    completion_cmd.add_argument("shell", choices=["bash", "zsh"])
    completion_cmd.set_defaults(func=_cmd_completion)

    complete_cmd = subparsers.add_parser("_complete", help=argparse.SUPPRESS)
    complete_cmd.add_argument("--index", required=True, type=int)
    complete_cmd.add_argument("--words", nargs=argparse.REMAINDER, default=[])
    complete_cmd.set_defaults(func=_cmd_complete)

    return parser


def _expand_dynamic_shorthand(argv: list[str]) -> list[str]:
    if not argv:
        return argv

    args = list(argv)
    command_index = 0
    while command_index < len(args):
        token = args[command_index]
        if token == "--state-dir":
            command_index += 2
            continue
        if token.startswith("-"):
            command_index += 1
            continue
        break

    if command_index >= len(args):
        return args

    candidate = args[command_index]
    if candidate in STATIC_ROOT_COMMANDS:
        return args
    if candidate.startswith("-"):
        return args
    return [*args[:command_index], "invoke", *args[command_index:]]


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    effective_argv = _expand_dynamic_shorthand(raw_argv)

    parser = build_parser()
    args, unknown = parser.parse_known_args(effective_argv)
    if unknown and args.command != "invoke":
        parser.error(f"unrecognized arguments: {' '.join(unknown)}")
    if not hasattr(args, "dynamic_args"):
        args.dynamic_args = unknown
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
