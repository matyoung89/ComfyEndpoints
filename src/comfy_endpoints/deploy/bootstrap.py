from __future__ import annotations

import argparse
import os
import shlex
import signal
import subprocess
import sys
from pathlib import Path

from comfy_endpoints.contracts.validators import parse_workflow_contract
from comfy_endpoints.deploy.cache_manager import CacheManager


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def run_bootstrap(
    cache_root: Path,
    watch_paths: list[Path],
    min_file_size_mb: int,
    contract_path: Path,
    api_key: str,
    gateway_port: int,
) -> int:
    if not contract_path.exists():
        raise RuntimeError(f"Contract path missing: {contract_path}")

    parse_workflow_contract(contract_path)

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
        "--comfy-url http://127.0.0.1:8188"
    )

    comfy_process = subprocess.Popen(shlex.split(comfy_command))
    gateway_process = subprocess.Popen(shlex.split(gateway_command))

    def shutdown(_sig: int, _frame: object) -> None:
        for process in (gateway_process, comfy_process):
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        comfy_status = comfy_process.poll()
        gateway_status = gateway_process.poll()

        if comfy_status is not None:
            if gateway_process.poll() is None:
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
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--gateway-port", type=int, default=3000)
    args = parser.parse_args()

    return run_bootstrap(
        cache_root=Path(args.cache_root),
        watch_paths=[Path(item) for item in _split_csv(args.watch_paths)],
        min_file_size_mb=args.min_file_size_mb,
        contract_path=Path(args.contract_path),
        api_key=args.api_key,
        gateway_port=args.gateway_port,
    )


if __name__ == "__main__":
    raise SystemExit(main())
