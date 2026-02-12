from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.models import DeploymentState
from comfy_endpoints.providers.base import DeploymentStatus
from comfy_endpoints.runtime.deployment_service import DeploymentService
from comfy_endpoints.runtime.image_manager import ImageResolution


class FakeProvider:
    def __init__(self):
        self.last_env = None

    def create_deployment(self, app_spec):
        _ = app_spec
        return "dep-123"

    def ensure_volume(self, deployment_id, size_gb):
        _ = (deployment_id, size_gb)
        return "vol-123"

    def deploy_image(self, deployment_id, image_ref, env, mounts, container_registry_auth_id=None):
        _ = (deployment_id, image_ref, mounts, container_registry_auth_id)
        self.last_env = env

    def get_status(self, deployment_id):
        _ = deployment_id
        return DeploymentStatus(state=DeploymentState.READY, detail="RUNNING")

    def get_endpoint(self, deployment_id):
        return f"https://{deployment_id}.example.com"

    def destroy(self, deployment_id):
        _ = deployment_id


class OutbidThenReadyProvider(FakeProvider):
    def __init__(self):
        super().__init__()
        self.create_calls = 0
        self.destroy_calls = 0

    def create_deployment(self, app_spec):
        _ = app_spec
        self.create_calls += 1
        return f"dep-{self.create_calls}"

    def get_status(self, deployment_id):
        if deployment_id == "dep-1":
            return DeploymentStatus(state=DeploymentState.DEGRADED, detail="EXITED: Outbid")
        return DeploymentStatus(state=DeploymentState.READY, detail="RUNNING")

    def destroy(self, deployment_id):
        _ = deployment_id
        self.destroy_calls += 1


class DeploymentServiceTest(unittest.TestCase):
    def test_deploy_records_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app_path = root / "app.json"
            workflow_path = root / "workflow.json"
            contract_path = root / "workflow.contract.json"

            workflow_path.write_text("{}", encoding="utf-8")
            contract_path.write_text(
                json.dumps(
                    {
                        "contract_id": "demo-contract",
                        "version": "v1",
                        "inputs": [
                            {
                                "name": "prompt",
                                "type": "string",
                                "required": True,
                                "node_id": "1",
                            }
                        ],
                        "outputs": [{"name": "image", "type": "image/png", "node_id": "9"}],
                    }
                ),
                encoding="utf-8",
            )

            app_path.write_text(
                json.dumps(
                    {
                        "app_id": "demo",
                        "version": "v1",
                        "workflow_path": "./workflow.json",
                        "provider": "runpod",
                        "gpu_profile": "A10G",
                        "regions": ["US"],
                        "env": {},
                        "endpoint": {
                            "name": "run",
                            "mode": "async",
                            "auth_mode": "api_key",
                            "timeout_seconds": 300,
                            "max_payload_mb": 10,
                        },
                        "cache_policy": {
                            "watch_paths": ["/tmp/models"],
                            "min_file_size_mb": 100,
                            "symlink_targets": ["/tmp/models"],
                        },
                        "build": {"comfy_version": "0.3.26", "plugins": []},
                    }
                ),
                encoding="utf-8",
            )

            service = DeploymentService(state_dir=root / "state")
            fake_provider = FakeProvider()
            with mock.patch("comfy_endpoints.runtime.deployment_service.build_provider", return_value=fake_provider):
                service.image_manager = mock.Mock(
                    ensure_image=mock.Mock(
                        return_value=ImageResolution(
                            image_ref="ghcr.io/comfy-endpoints/golden:test",
                            image_exists=True,
                            built=False,
                        )
                    )
                )
                service._is_endpoint_ready = mock.Mock(return_value=True)
                record = service.deploy(app_path)

            self.assertEqual(record.state, DeploymentState.READY)
            self.assertEqual(record.app_id, "demo")
            assert fake_provider.last_env is not None
            self.assertIn("COMFY_ENDPOINTS_CONTRACT_JSON", fake_provider.last_env)
            self.assertEqual(
                fake_provider.last_env.get("COMFY_ENDPOINTS_CONTRACT_PATH"),
                "/opt/comfy_endpoints/runtime/workflow.contract.json",
            )

    def test_retries_on_outbid_until_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app_path = root / "app.json"
            workflow_path = root / "workflow.json"
            contract_path = root / "workflow.contract.json"

            workflow_path.write_text("{}", encoding="utf-8")
            contract_path.write_text(
                json.dumps(
                    {
                        "contract_id": "demo-contract",
                        "version": "v1",
                        "inputs": [
                            {
                                "name": "prompt",
                                "type": "string",
                                "required": True,
                                "node_id": "1",
                            }
                        ],
                        "outputs": [{"name": "image", "type": "image/png", "node_id": "9"}],
                    }
                ),
                encoding="utf-8",
            )

            app_path.write_text(
                json.dumps(
                    {
                        "app_id": "demo",
                        "version": "v1",
                        "workflow_path": "./workflow.json",
                        "provider": "runpod",
                        "gpu_profile": "A10G",
                        "regions": ["US"],
                        "env": {},
                        "endpoint": {
                            "name": "run",
                            "mode": "async",
                            "auth_mode": "api_key",
                            "timeout_seconds": 300,
                            "max_payload_mb": 10,
                        },
                        "cache_policy": {
                            "watch_paths": ["/tmp/models"],
                            "min_file_size_mb": 100,
                            "symlink_targets": ["/tmp/models"],
                        },
                        "build": {"comfy_version": "0.3.26", "plugins": []},
                    }
                ),
                encoding="utf-8",
            )

            service = DeploymentService(state_dir=root / "state")
            provider = OutbidThenReadyProvider()
            with mock.patch.dict("os.environ", {"COMFY_ENDPOINTS_RUNPOD_MAX_DEPLOY_ATTEMPTS": "3"}, clear=False):
                with mock.patch("comfy_endpoints.runtime.deployment_service.build_provider", return_value=provider):
                    service.image_manager = mock.Mock(
                        ensure_image=mock.Mock(
                            return_value=ImageResolution(
                                image_ref="ghcr.io/comfy-endpoints/golden:test",
                                image_exists=True,
                                built=False,
                            )
                        )
                    )
                    service._is_endpoint_ready = mock.Mock(return_value=True)
                    record = service.deploy(app_path)

            self.assertEqual(record.state, DeploymentState.READY)
            self.assertEqual(provider.create_calls, 2)
            self.assertEqual(provider.destroy_calls, 1)


if __name__ == "__main__":
    unittest.main()
