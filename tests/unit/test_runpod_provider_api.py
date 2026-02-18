from __future__ import annotations

import unittest
from unittest import mock

from comfy_endpoints.models import (
    AppSpecV1,
    AuthMode,
    BuildSpec,
    CachePolicy,
    ComputePolicy,
    DeploymentState,
    EndpointSpec,
    ProviderName,
    WorkflowApiContractV1,
)
from comfy_endpoints.providers.runpod_provider import RunpodProvider


def _app_spec() -> AppSpecV1:
    from pathlib import Path

    return AppSpecV1(
        app_id="demo",
        version="v1",
        workflow_path=Path("/tmp/workflow.json"),
        provider=ProviderName.RUNPOD,
        gpu_profile="A10G",
        regions=["US"],
        env={"COMFY_ENDPOINTS_APP_ID": "demo"},
        endpoint=EndpointSpec(
            name="run",
            mode="async",
            auth_mode=AuthMode.API_KEY,
            timeout_seconds=300,
            max_payload_mb=10,
        ),
        cache_policy=CachePolicy(
            watch_paths=["/opt/comfy/models"],
            min_file_size_mb=100,
            symlink_targets=["/opt/comfy/models"],
        ),
        build=BuildSpec(comfy_version="0.3.26", plugins=[]),
    )


class RunpodProviderApiTest(unittest.TestCase):
    def test_create_deployment_sends_expected_input(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request", return_value={"id": "pod-1"}) as mocked_rest:
            deployment_id = provider.create_deployment(_app_spec())

        self.assertEqual(deployment_id, "pod-1")
        args, _kwargs = mocked_rest.call_args
        self.assertEqual(args[0], "POST")
        self.assertEqual(args[1], "/pods")
        payload = args[2]
        self.assertEqual(payload["name"], "comfy-endpoints-demo")
        self.assertEqual(payload["volumeMountPath"], "/cache")
        self.assertIn("8080/http", payload["ports"][0])
        self.assertEqual(payload["cloudType"], "COMMUNITY")
        self.assertTrue(payload["interruptible"])

    def test_create_deployment_includes_registry_auth_when_configured(self) -> None:
        provider = RunpodProvider()
        app_spec = _app_spec()
        app_spec.build.image_ref = "ghcr.io/private/repo:tag"
        app_spec.build.container_registry_auth_id = "reg-auth-1"
        with mock.patch.object(provider, "_rest_request", return_value={"id": "pod-1"}) as mocked_rest:
            provider.create_deployment(app_spec)
        payload = mocked_rest.call_args.args[2]
        self.assertEqual(payload["containerRegistryAuthId"], "reg-auth-1")
        self.assertEqual(payload["imageName"], "ghcr.io/private/repo:tag")

    def test_create_deployment_applies_min_ram_per_gpu_when_configured(self) -> None:
        provider = RunpodProvider()
        app_spec = _app_spec()
        app_spec.compute_policy = ComputePolicy(min_ram_per_gpu_gb=64, gpu_count=1)
        with mock.patch.object(provider, "_rest_request", return_value={"id": "pod-1"}) as mocked_rest:
            provider.create_deployment(app_spec)
        payload = mocked_rest.call_args.args[2]
        self.assertEqual(payload["minRAMPerGPU"], 64)

    def test_create_deployment_applies_gpu_type_ids_for_min_vram(self) -> None:
        provider = RunpodProvider()
        app_spec = _app_spec()
        app_spec.compute_policy = ComputePolicy(min_vram_gb=24, gpu_count=1)
        with mock.patch.object(provider, "_gpu_type_ids_with_min_vram", return_value=["gpu-1", "gpu-2"]):
            with mock.patch.object(provider, "_rest_request", return_value={"id": "pod-1"}) as mocked_rest:
                provider.create_deployment(app_spec)
        payload = mocked_rest.call_args.args[2]
        self.assertEqual(payload["gpuTypeIds"], ["gpu-1", "gpu-2"])

    def test_create_deployment_uses_preferred_gpu_types_strictly(self) -> None:
        provider = RunpodProvider()
        app_spec = _app_spec()
        app_spec.preferred_gpu_types = ["NVIDIA RTX PRO 6000"]
        app_spec.compute_policy = ComputePolicy(min_vram_gb=24, gpu_count=1)
        with mock.patch.object(provider, "_gpu_type_ids_for_preferred", return_value=["gpu-preferred-1"]) as mocked_pref:
            with mock.patch.object(provider, "_gpu_type_ids_with_min_vram", return_value=["gpu-vram-1"]) as mocked_vram:
                with mock.patch.object(provider, "_rest_request", return_value={"id": "pod-1"}) as mocked_rest:
                    provider.create_deployment(app_spec)
        payload = mocked_rest.call_args.args[2]
        self.assertEqual(payload["gpuTypeIds"], ["gpu-preferred-1"])
        mocked_pref.assert_called_once()
        mocked_vram.assert_not_called()

    def test_create_deployment_fails_when_preferred_gpu_not_available(self) -> None:
        provider = RunpodProvider()
        app_spec = _app_spec()
        app_spec.preferred_gpu_types = ["NVIDIA RTX PRO 6000"]
        with mock.patch.object(
            provider,
            "_gpu_type_ids_for_preferred",
            side_effect=RuntimeError("preferred_gpu_types not available"),
        ):
            with self.assertRaisesRegex(RuntimeError, "preferred_gpu_types not available"):
                provider.create_deployment(app_spec)

    def test_create_deployment_fails_when_no_gpu_type_meets_min_vram(self) -> None:
        provider = RunpodProvider()
        app_spec = _app_spec()
        app_spec.compute_policy = ComputePolicy(min_vram_gb=80, gpu_count=1)
        with mock.patch.object(
            provider,
            "_gpu_type_ids_with_min_vram",
            side_effect=RuntimeError("No RunPod GPU types satisfy min_vram_gb=80"),
        ):
            with self.assertRaisesRegex(RuntimeError, "min_vram_gb=80"):
                provider.create_deployment(app_spec)

    def test_gpu_type_ids_with_min_vram_filters_to_rest_openapi_enum(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(
            provider,
            "_graphql_request",
            return_value={
                "gpuTypes": [
                    {"id": "NVIDIA H100 PCIe", "displayName": "H100 PCIe", "memoryInGb": 80},
                    {"id": "NVIDIA B300 SXM6 AC", "displayName": "B300", "memoryInGb": 288},
                ]
            },
        ):
            with mock.patch.object(provider, "_rest_gpu_type_enum", return_value={"NVIDIA H100 PCIe", "NVIDIA H200"}):
                gpu_type_ids = provider._gpu_type_ids_with_min_vram(64)

        self.assertEqual(gpu_type_ids, ["NVIDIA H100 PCIe"])

    def test_gpu_type_ids_for_preferred_resolves_display_name_priority(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(
            provider,
            "_graphql_request",
            return_value={
                "gpuTypes": [
                    {"id": "NVIDIA RTX PRO 6000", "displayName": "NVIDIA RTX PRO 6000", "memoryInGb": 96},
                    {"id": "NVIDIA H100 PCIe", "displayName": "H100 PCIe", "memoryInGb": 80},
                ]
            },
        ):
            with mock.patch.object(
                provider,
                "_rest_gpu_type_enum",
                return_value={"NVIDIA RTX PRO 6000", "NVIDIA H100 PCIe"},
            ):
                gpu_type_ids = provider._gpu_type_ids_for_preferred(["NVIDIA RTX PRO 6000", "NVIDIA H100 PCIe"])
        self.assertEqual(gpu_type_ids, ["NVIDIA RTX PRO 6000", "NVIDIA H100 PCIe"])

    def test_gpu_type_ids_with_min_vram_fails_when_no_rest_enum_overlap(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(
            provider,
            "_graphql_request",
            return_value={
                "gpuTypes": [
                    {"id": "NVIDIA B300 SXM6 AC", "displayName": "B300", "memoryInGb": 288},
                ]
            },
        ):
            with mock.patch.object(provider, "_rest_gpu_type_enum", return_value={"NVIDIA H100 PCIe", "NVIDIA H200"}):
                with self.assertRaisesRegex(RuntimeError, "no overlap"):
                    provider._gpu_type_ids_with_min_vram(64)

    def test_ensure_volume_patches_when_too_small(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request") as mocked_rest:
            mocked_rest.side_effect = [
                {"volumeInGb": 20},
                {"id": "pod-1"},
            ]
            volume_ref = provider.ensure_volume("pod-1", 100)

        self.assertEqual(volume_ref, "pod-volume:pod-1:100")
        self.assertEqual(mocked_rest.call_args_list[1].args[0], "PATCH")

    def test_deploy_image_patches_pod_and_resumes(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request", return_value={"id": "pod-1"}) as mocked_rest:
            provider.deploy_image(
                deployment_id="pod-1",
                image_ref="ghcr.io/comfy-endpoints/golden:tag",
                env={"A": "B"},
                mounts=[{"source": "cache", "target": "/cache"}],
                container_registry_auth_id="reg-auth-1",
            )

        self.assertEqual(mocked_rest.call_args_list[0].args[0], "PATCH")
        self.assertEqual(mocked_rest.call_args_list[1].args[0], "POST")
        self.assertEqual(mocked_rest.call_args_list[1].args[1], "/pods/pod-1/start")
        self.assertEqual(mocked_rest.call_args_list[0].args[2]["containerRegistryAuthId"], "reg-auth-1")

    def test_get_status_maps_states(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request", return_value={"desiredStatus": "RUNNING", "lastStatusChange": "Rented by User"}):
            status = provider.get_status("pod-1")
        self.assertEqual(status.state, DeploymentState.READY)

    def test_get_status_stays_bootstrapping_when_image_fetching(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(
            provider,
            "_rest_request",
            return_value={"desiredStatus": "RUNNING", "lastStatusChange": "create container: still fetching image"},
        ):
            status = provider.get_status("pod-1")
        self.assertEqual(status.state, DeploymentState.BOOTSTRAPPING)

    def test_get_endpoint_prefers_gateway_port(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(
            provider,
            "_rest_request",
            return_value={"id": "pod-1", "ports": ["8080/http", "3000/http"]},
        ):
            endpoint = provider.get_endpoint("pod-1")
        self.assertEqual(endpoint, "https://pod-1-3000.proxy.runpod.net")

    def test_get_logs_uses_logs_endpoint_when_available(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(
            provider,
            "_rest_request",
            return_value=[
                {"message": "line 1"},
                {"message": "line 2"},
            ],
        ):
            logs = provider.get_logs("pod-1", tail_lines=100)
        self.assertEqual(logs, "line 1\nline 2")

    def test_get_logs_falls_back_to_pod_payload_when_logs_endpoint_missing(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request") as mocked_rest:
            mocked_rest.side_effect = [
                {"_suppressed_http_error": 404, "_detail": "not found"},
                {"_suppressed_http_error": 404, "_detail": "not found"},
                {
                    "desiredStatus": "EXITED",
                    "lastStatusChange": "python: can't open file '/opt/comfy/main.py': [Errno 2] No such file or directory",
                },
            ]
            logs = provider.get_logs("pod-1", tail_lines=100)
        self.assertIn("/opt/comfy/main.py", logs)

    def test_destroy_ignores_missing_pod_404(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request") as mocked_rest:
            mocked_rest.side_effect = [
                {},
                {"_suppressed_http_error": 404, "_detail": "pod not found"},
            ]
            provider.destroy("pod-1")

        self.assertEqual(mocked_rest.call_args_list[0].args[0], "POST")
        self.assertEqual(mocked_rest.call_args_list[1].args[0], "DELETE")
        self.assertEqual(mocked_rest.call_args_list[1].kwargs["suppress_http_errors"], (404,))

    def test_destroy_raises_when_delete_fails_non_404(self) -> None:
        provider = RunpodProvider()
        with mock.patch.object(provider, "_rest_request") as mocked_rest:
            mocked_rest.side_effect = [None, RuntimeError("boom")]
            with self.assertRaisesRegex(RuntimeError, "boom"):
                provider.destroy("pod-1")


if __name__ == "__main__":
    unittest.main()
