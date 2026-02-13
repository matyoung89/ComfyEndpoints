from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from comfy_endpoints.contracts.validators import parse_workflow_contract
from comfy_endpoints.deploy.cache_manager import CacheManager
from comfy_endpoints.gateway.comfy_client import ComfyClient, ComfyClientError
from comfy_endpoints.gateway.prompt_mapper import build_preflight_payload


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def ensure_contract_file(contract_path: Path) -> None:
    if contract_path.exists():
        return

    contract_json = os.getenv("COMFY_ENDPOINTS_CONTRACT_JSON", "").strip()
    if not contract_json:
        raise RuntimeError(f"Contract path missing: {contract_path}")

    try:
        parsed = json.loads(contract_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid COMFY_ENDPOINTS_CONTRACT_JSON payload") from exc

    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(json.dumps(parsed, indent=2), encoding="utf-8")


def ensure_workflow_file(workflow_path: Path) -> None:
    if workflow_path.exists():
        return

    workflow_json = os.getenv("COMFY_ENDPOINTS_WORKFLOW_JSON", "").strip()
    if not workflow_json:
        raise RuntimeError(f"Workflow path missing: {workflow_path}")

    try:
        parsed = json.loads(workflow_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid COMFY_ENDPOINTS_WORKFLOW_JSON payload") from exc

    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    workflow_path.write_text(json.dumps(parsed, indent=2), encoding="utf-8")


def wait_for_comfy_ready(comfy_url: str, timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    last_error = "unknown"
    while time.time() < deadline:
        request = urllib.request.Request(f"{comfy_url.rstrip('/')}/system_stats", method="GET")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    return
                last_error = f"unexpected_status:{response.status}"
        except urllib.error.HTTPError as exc:
            last_error = f"http_error:{exc.code}"
        except urllib.error.URLError as exc:
            last_error = f"url_error:{exc.reason}"
        time.sleep(3)
    raise RuntimeError(f"Comfy startup timeout waiting for readiness: {last_error}")


def run_bootstrap(
    cache_root: Path,
    watch_paths: list[Path],
    min_file_size_mb: int,
    contract_path: Path,
    workflow_path: Path,
    api_key: str,
    gateway_port: int,
) -> int:
    ensure_contract_file(contract_path)
    ensure_workflow_file(workflow_path)

    contract = parse_workflow_contract(contract_path)
    workflow_payload = json.loads(workflow_path.read_text(encoding="utf-8"))

    manager = CacheManager(
        cache_root=cache_root,
        watch_paths=watch_paths,
        min_file_size_mb=min_file_size_mb,
    )
    manager.reconcile()

    comfy_command = os.getenv(
        "COMFY_START_COMMAND",
        "python /opt/comfy/main.py --listen 127.0.0.1 --port 8188 --disable-auto-launch",
    )

    gateway_command = (
        "python -m comfy_endpoints.gateway.server "
        f"--listen-host 0.0.0.0 --listen-port {gateway_port} "
        f"--api-key {shlex.quote(api_key)} "
        f"--contract-path {shlex.quote(str(contract_path))} "
        f"--workflow-path {shlex.quote(str(workflow_path))} "
        "--comfy-url http://127.0.0.1:8188"
    )

    comfy_process = subprocess.Popen(shlex.split(comfy_command))
    gateway_process = None

    def shutdown(_sig: int, _frame: object) -> None:
        for process in (gateway_process, comfy_process):
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        wait_for_comfy_ready("http://127.0.0.1:8188")
        preflight_payload = build_preflight_payload(workflow_payload, contract)
        preflight_prompt_id = ComfyClient("http://127.0.0.1:8188").queue_prompt(preflight_payload)
        print(
            f"[bootstrap] comfy preflight queue passed prompt_id={preflight_prompt_id}",
            file=sys.stderr,
        )
    except ComfyClientError as exc:
        if comfy_process.poll() is None:
            comfy_process.terminate()
        raise RuntimeError(f"Comfy preflight queue failed: {exc}") from exc
    except Exception:
        if comfy_process.poll() is None:
            comfy_process.terminate()
        raise

    gateway_process = subprocess.Popen(shlex.split(gateway_command))

    while True:
        comfy_status = comfy_process.poll()
        gateway_status = gateway_process.poll() if gateway_process else None

        if comfy_status is not None:
            if gateway_process and gateway_process.poll() is None:
                gateway_process.terminate()
            return comfy_status

        if gateway_status is not None:
            if comfy_process.poll() is None:
                comfy_process.terminate()
            return gateway_status


def main() -> int:
    parser = argparse.ArgumentParser(prog="comfy-endpoints-bootstrap")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--watch-paths", required=True)
    parser.add_argument("--min-file-size-mb", type=int, default=100)
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--workflow-path", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--gateway-port", type=int, default=3000)
    args = parser.parse_args()

    return run_bootstrap(
        cache_root=Path(args.cache_root),
        watch_paths=[Path(item) for item in _split_csv(args.watch_paths)],
        min_file_size_mb=args.min_file_size_mb,
        contract_path=Path(args.contract_path),
        workflow_path=Path(args.workflow_path),
        api_key=args.api_key,
        gateway_port=args.gateway_port,
    )


if __name__ == "__main__":
    raise SystemExit(main())
