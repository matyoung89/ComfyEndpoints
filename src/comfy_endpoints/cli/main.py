from __future__ import annotations

import argparse
import json
from pathlib import Path

from comfy_endpoints.runtime import DeploymentService
from comfy_endpoints.utils.env_loader import load_local_env


def _default_state_dir() -> Path:
    return Path.cwd() / ".comfy_endpoints"


def _cmd_init(args: argparse.Namespace) -> int:
    app_dir = Path(args.app_dir).resolve()
    app_dir.mkdir(parents=True, exist_ok=True)

    workflow_file = app_dir / "workflow.json"
    contract_file = app_dir / "workflow.contract.json"
    app_spec_file = app_dir / "app.yaml"

    if not workflow_file.exists():
        workflow_file.write_text("{}\n", encoding="utf-8")

    if not contract_file.exists():
        contract_payload = {
            "contract_id": f"{app_dir.name}-contract",
            "version": "v1",
            "inputs": [
                {
                    "name": "prompt",
                    "type": "string",
                    "required": True,
                    "node_id": "api_input_prompt",
                }
            ],
            "outputs": [
                {
                    "name": "image",
                    "type": "image/png",
                    "node_id": "api_output_image",
                }
            ],
        }
        contract_file.write_text(json.dumps(contract_payload, indent=2), encoding="utf-8")

    if not app_spec_file.exists():
        app_spec_file.write_text(
            f"""app_id: {app_dir.name}
version: v1
workflow_path: ./workflow.json
provider: runpod
gpu_profile: A10G
regions:
  - US
env:
  COMFY_HEADLESS: "1"
endpoint:
  name: run
  mode: async
  auth_mode: api_key
  timeout_seconds: 300
  max_payload_mb: 10
cache_policy:
  watch_paths:
    - /opt/comfy/models
  min_file_size_mb: 100
  symlink_targets:
    - /opt/comfy/models
build:
  comfy_version: 0.3.26
  image_repository: ghcr.io/comfy-endpoints/golden
  dockerfile_path: docker/Dockerfile.golden
  build_context: .
  container_registry_auth_id: ""
  plugins:
    - repo: https://github.com/comfyanonymous/ComfyUI
      ref: master
""",
            encoding="utf-8",
        )

    print(f"Initialized app scaffold at {app_dir}")
    return 0


def _service(state_dir: str | None) -> DeploymentService:
    state_root = Path(state_dir).resolve() if state_dir else _default_state_dir()
    return DeploymentService(state_dir=state_root)


def _cmd_validate(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    app_id, contract_id = svc.validate(Path(args.app_spec).resolve())
    print(json.dumps({"app_id": app_id, "contract_id": contract_id, "result": "ok"}, indent=2))
    return 0


def _cmd_deploy(args: argparse.Namespace) -> int:
    svc = _service(args.state_dir)
    record = svc.deploy(Path(args.app_spec).resolve())
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

    return parser


def main() -> int:
    load_local_env()
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
