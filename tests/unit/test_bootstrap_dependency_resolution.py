from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.deploy.bootstrap import (
    MissingModelRequirement,
    _fetch_manager_default_model_list,
    _install_missing_models,
    _iter_model_entries,
    _missing_models_from_object_info,
    _missing_models_from_preflight_error,
)
from comfy_endpoints.gateway.comfy_client import ComfyClientError


class BootstrapDependencyResolutionTest(unittest.TestCase):
    def test_missing_models_parse_from_error_text(self) -> None:
        exc = ComfyClientError(
            "error",
            status_code=400,
            response_text=(
                "Value not in list: clip_name1: 'clip_l.safetensors' not in []\\n"
                "Value not in list: unet_name: 'flux1-schnell.safetensors' not in []"
            ),
        )
        parsed = _missing_models_from_preflight_error(exc)
        self.assertEqual(
            parsed,
            [
                MissingModelRequirement(input_name="clip_name1", filename="clip_l.safetensors"),
                MissingModelRequirement(input_name="unet_name", filename="flux1-schnell.safetensors"),
            ],
        )

    def test_iter_model_entries_walks_nested_payload(self) -> None:
        payload = {
            "models": [
                {
                    "name": "clip_l.safetensors",
                    "url": "https://example.com/clip_l.safetensors",
                    "type": "text_encoders",
                }
            ],
            "other": {
                "entries": [
                    {
                        "filename": "ae.safetensors",
                        "download_url": "https://example.com/ae.safetensors",
                        "type": "vae",
                    }
                ]
            },
        }
        entries = _iter_model_entries(payload)
        names = sorted(item["filename"] for item in entries)
        self.assertEqual(names, ["ae.safetensors", "clip_l.safetensors"])

    def test_install_missing_models_downloads_matches(self) -> None:
        class FakeComfyClient:
            def get_external_models(self):
                return {
                    "models": [
                        {
                            "filename": "clip_l.safetensors",
                            "url": "https://example.com/clip_l.safetensors",
                            "type": "text_encoders",
                        }
                    ]
                }

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            requirements = [MissingModelRequirement(input_name="clip_name1", filename="clip_l.safetensors")]
            with mock.patch("comfy_endpoints.deploy.bootstrap._download_file") as mocked_download:
                count = _install_missing_models(FakeComfyClient(), requirements, root)

            self.assertEqual(count, 1)
            mocked_download.assert_called_once()
            target_path = mocked_download.call_args.args[1]
            self.assertEqual(str(target_path), str(root / "models" / "text_encoders" / "clip_l.safetensors"))

    def test_install_missing_models_falls_back_to_default_catalog(self) -> None:
        class EmptyManagerClient:
            def get_external_models(self):
                raise ComfyClientError("unavailable")

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            requirements = [MissingModelRequirement(input_name="unet_name", filename="flux1-schnell.safetensors")]
            fallback_payload = {
                "models": [
                    {
                        "filename": "flux1-schnell.safetensors",
                        "url": "https://example.com/flux1-schnell.safetensors",
                        "type": "unet",
                    }
                ]
            }
            with mock.patch(
                "comfy_endpoints.deploy.bootstrap._fetch_manager_default_model_list",
                return_value=fallback_payload,
            ):
                with mock.patch("comfy_endpoints.deploy.bootstrap._download_file") as mocked_download:
                    count = _install_missing_models(EmptyManagerClient(), requirements, root)

            self.assertEqual(count, 1)
            mocked_download.assert_called_once()

    def test_missing_models_parse_from_object_info_dropdown_mismatch(self) -> None:
        prompt_payload = {
            "prompt": {
                "2": {
                    "class_type": "UNETLoader",
                    "inputs": {"unet_name": "flux1-schnell.safetensors"},
                },
                "3": {
                    "class_type": "DualCLIPLoader",
                    "inputs": {
                        "clip_name1": "clip_l.safetensors",
                        "clip_name2": "t5xxl_fp8_e4m3fn.safetensors",
                    },
                },
                "4": {
                    "class_type": "VAELoader",
                    "inputs": {"vae_name": "ae.safetensors"},
                },
            }
        }
        object_info_payload = {
            "UNETLoader": {"input": {"required": {"unet_name": [["other_unet.safetensors"]]}}},
            "DualCLIPLoader": {
                "input": {
                    "required": {
                        "clip_name1": [["other_clip.safetensors"]],
                        "clip_name2": [["other_t5.safetensors"]],
                    }
                }
            },
            "VAELoader": {"input": {"required": {"vae_name": [["pixel_space"]]}}},
        }

        parsed = _missing_models_from_object_info(prompt_payload, object_info_payload)
        self.assertEqual(
            parsed,
            [
                MissingModelRequirement(input_name="unet_name", filename="flux1-schnell.safetensors"),
                MissingModelRequirement(input_name="clip_name1", filename="clip_l.safetensors"),
                MissingModelRequirement(input_name="clip_name2", filename="t5xxl_fp8_e4m3fn.safetensors"),
                MissingModelRequirement(input_name="vae_name", filename="ae.safetensors"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
