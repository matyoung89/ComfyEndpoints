from __future__ import annotations

import unittest
from unittest import mock

from comfy_endpoints.deploy.predeploy_validator import validate_preflight_artifacts
from comfy_endpoints.models import ArtifactSourceSpec


class _StatusResponse:
    def __init__(self, status: int):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        _ = (exc_type, exc, tb)
        return False


class PredeployValidatorTest(unittest.TestCase):
    def test_validate_preflight_artifacts_passes_for_wan_requirements(self) -> None:
        preflight_payload = {
            "prompt": {
                "5": {
                    "class_type": "WanVideoVAELoader",
                    "inputs": {"model_name": "wanvideo/Wan2_1_VAE_bf16.safetensors"},
                },
                "6": {
                    "class_type": "WanVideoModelLoader",
                    "inputs": {"model": "WanVideo/2_2/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors"},
                },
            }
        }
        artifact_specs = [
            ArtifactSourceSpec(
                match="wanvideo/Wan2_1_VAE_bf16.safetensors",
                source_url="https://example.com/Wan2_1_VAE_bf16.safetensors",
                target_subdir="vae",
                target_path="wanvideo/Wan2_1_VAE_bf16.safetensors",
            ),
            ArtifactSourceSpec(
                match="WanVideo/2_2/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
                source_url="https://example.com/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
                target_subdir="diffusion_models",
                target_path="WanVideo/2_2/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
            ),
        ]

        with mock.patch("urllib.request.urlopen", return_value=_StatusResponse(200)):
            result = validate_preflight_artifacts(preflight_payload, artifact_specs)
        self.assertTrue(result.ok)
        self.assertEqual(result.errors, [])
        self.assertGreaterEqual(len(result.matched_models), 2)

    def test_validate_preflight_artifacts_fails_when_required_model_not_declared(self) -> None:
        preflight_payload = {
            "prompt": {
                "6": {
                    "class_type": "WanVideoModelLoader",
                    "inputs": {"model": "WanVideo/2_2/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors"},
                },
            }
        }
        artifact_specs = [
            ArtifactSourceSpec(
                match="wanvideo/Wan2_1_VAE_bf16.safetensors",
                source_url="https://example.com/Wan2_1_VAE_bf16.safetensors",
                target_subdir="vae",
                target_path="wanvideo/Wan2_1_VAE_bf16.safetensors",
            )
        ]
        with mock.patch("urllib.request.urlopen", return_value=_StatusResponse(200)):
            result = validate_preflight_artifacts(preflight_payload, artifact_specs)
        self.assertFalse(result.ok)
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.errors[0]["type"], "required_model_not_declared")

    def test_validate_preflight_artifacts_fails_when_source_unreachable(self) -> None:
        preflight_payload = {"prompt": {}}
        artifact_specs = [
            ArtifactSourceSpec(
                match="x.safetensors",
                source_url="https://example.com/x.safetensors",
                target_subdir="checkpoints",
                target_path="x.safetensors",
            )
        ]
        with mock.patch(
            "urllib.request.urlopen",
            side_effect=OSError("network down"),
        ):
            result = validate_preflight_artifacts(preflight_payload, artifact_specs)
        self.assertFalse(result.ok)
        self.assertEqual(result.errors[0]["type"], "artifact_source_unreachable")


if __name__ == "__main__":
    unittest.main()
