from __future__ import annotations

import tempfile
import urllib.error
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.deploy.bootstrap import (
    MissingNodeRequirement,
    MissingModelRequirement,
    _ensure_model_roots_on_cache,
    _fetch_manager_default_model_list,
    _find_package_ids_for_node_class,
    _find_repo_urls_for_package_ids,
    _find_repo_urls_for_node_class,
    _install_missing_custom_nodes,
    _install_missing_custom_nodes_from_catalog,
    _install_missing_models,
    _install_missing_models_from_entries,
    _iter_model_entries,
    _missing_nodes_from_object_info,
    _missing_nodes_from_preflight_error,
    _known_model_requirements_from_prompt,
    _log_manager_endpoint_probes,
    _missing_models_from_object_info,
    _missing_models_from_preflight_error,
    _probe_manager_endpoint_status,
    _prestart_resolve_artifacts_or_error,
    _required_nodes_from_prompt,
    _resolve_missing_custom_nodes_from_catalog,
    _resolve_missing_models_from_entries,
)
from comfy_endpoints.gateway.comfy_client import ComfyClientError


class BootstrapDependencyResolutionTest(unittest.TestCase):
    def test_probe_manager_endpoint_status_reports_http_code(self) -> None:
        class _StatusResponse:
            def __init__(self, status: int):
                self.status = status

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                _ = (exc_type, exc, tb)
                return False

        with mock.patch("urllib.request.urlopen", return_value=_StatusResponse(200)):
            status = _probe_manager_endpoint_status("http://127.0.0.1:8188", "/customnode/getlist")
        self.assertEqual(status, "HTTP 200")

    def test_log_manager_endpoint_probes_logs_each_path(self) -> None:
        def _raise_404(_request, timeout=10):  # noqa: ARG001
            raise urllib.error.HTTPError(
                url="http://127.0.0.1:8188/customnode/getlist",
                code=404,
                msg="Not Found",
                hdrs=None,
                fp=None,
            )

        with mock.patch("urllib.request.urlopen", side_effect=_raise_404):
            with mock.patch("sys.stderr") as mocked_stderr:
                _log_manager_endpoint_probes("http://127.0.0.1:8188")

        joined = "".join(str(call.args[0]) for call in mocked_stderr.write.call_args_list)
        self.assertIn("/customnode/getmappings?mode=default", joined)
        self.assertIn("/customnode/getlist?mode=default&skip_update=true", joined)
        self.assertIn("/externalmodel/getlist?mode=default", joined)

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
                count = _install_missing_models(FakeComfyClient(), requirements, root / "cache_models")

            self.assertEqual(count, 1)
            mocked_download.assert_called_once()
            target_path = mocked_download.call_args.args[1]
            self.assertEqual(
                str(target_path),
                str(root / "cache_models" / "text_encoders" / "clip_l.safetensors"),
            )

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
                    count = _install_missing_models(EmptyManagerClient(), requirements, root / "cache_models")

            self.assertEqual(count, 1)
            mocked_download.assert_called_once()

    def test_install_missing_models_matches_by_basename_and_keeps_prompt_subpath(self) -> None:
        class FakeComfyClient:
            def get_external_models(self):
                return {
                    "models": [
                        {
                            "filename": "Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
                            "url": "https://example.com/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
                            "type": "diffusion_models",
                        }
                    ]
                }

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            requirements = [
                MissingModelRequirement(
                    input_name="model",
                    filename="WanVideo/2_2/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
                )
            ]
            with mock.patch("comfy_endpoints.deploy.bootstrap._download_file") as mocked_download:
                count = _install_missing_models(FakeComfyClient(), requirements, root / "cache_models")

            self.assertEqual(count, 1)
            mocked_download.assert_called_once()
            target_path = mocked_download.call_args.args[1]
            self.assertEqual(
                str(target_path),
                str(
                    root
                    / "cache_models"
                    / "diffusion_models"
                    / "WanVideo"
                    / "2_2"
                    / "Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors"
                ),
            )

    def test_ensure_model_roots_on_cache_replaces_local_dirs_with_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            comfy_models_root = root / "opt_comfy_models"
            cache_models_root = root / "cache_models"
            checkpoints_dir = comfy_models_root / "checkpoints"
            checkpoints_dir.mkdir(parents=True)
            local_model = checkpoints_dir / "demo.safetensors"
            local_model.write_bytes(b"model")

            _ensure_model_roots_on_cache(
                comfy_models_root=comfy_models_root,
                cache_models_root=cache_models_root,
            )

            symlinked_checkpoints = comfy_models_root / "checkpoints"
            self.assertTrue(symlinked_checkpoints.is_symlink())
            self.assertEqual(
                symlinked_checkpoints.resolve(),
                (cache_models_root / "checkpoints").resolve(),
            )
            self.assertTrue((cache_models_root / "checkpoints" / "demo.safetensors").exists())

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
                MissingModelRequirement(
                    input_name="unet_name",
                    filename="flux1-schnell.safetensors",
                    class_type="UNETLoader",
                ),
                MissingModelRequirement(
                    input_name="clip_name1",
                    filename="clip_l.safetensors",
                    class_type="DualCLIPLoader",
                ),
                MissingModelRequirement(
                    input_name="clip_name2",
                    filename="t5xxl_fp8_e4m3fn.safetensors",
                    class_type="DualCLIPLoader",
                ),
                MissingModelRequirement(
                    input_name="vae_name",
                    filename="ae.safetensors",
                    class_type="VAELoader",
                ),
            ],
        )

    def test_known_model_requirements_from_prompt_includes_wan_loader_inputs(self) -> None:
        prompt_payload = {
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
        parsed = _known_model_requirements_from_prompt(prompt_payload)
        self.assertEqual(
            parsed,
            [
                MissingModelRequirement(
                    input_name="model_name",
                    filename="wanvideo/Wan2_1_VAE_bf16.safetensors",
                    class_type="WanVideoVAELoader",
                ),
                MissingModelRequirement(
                    input_name="model",
                    filename="WanVideo/2_2/Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors",
                    class_type="WanVideoModelLoader",
                ),
            ],
        )

    def test_known_model_requirements_from_prompt(self) -> None:
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
                "4": {"class_type": "VAELoader", "inputs": {"vae_name": "ae.safetensors"}},
            }
        }
        parsed = _known_model_requirements_from_prompt(prompt_payload)
        self.assertEqual(
            parsed,
            [
                MissingModelRequirement(
                    input_name="unet_name",
                    filename="flux1-schnell.safetensors",
                    class_type="UNETLoader",
                ),
                MissingModelRequirement(
                    input_name="clip_name1",
                    filename="clip_l.safetensors",
                    class_type="DualCLIPLoader",
                ),
                MissingModelRequirement(
                    input_name="clip_name2",
                    filename="t5xxl_fp8_e4m3fn.safetensors",
                    class_type="DualCLIPLoader",
                ),
                MissingModelRequirement(
                    input_name="vae_name",
                    filename="ae.safetensors",
                    class_type="VAELoader",
                ),
            ],
        )

    def test_missing_nodes_parse_from_error_text(self) -> None:
        exc = ComfyClientError(
            "error",
            status_code=400,
            response_text="invalid prompt: Node 'Wan22Animate' not found. The custom node may not be installed.",
        )
        parsed = _missing_nodes_from_preflight_error(exc)
        self.assertEqual(parsed, [MissingNodeRequirement(class_type="Wan22Animate")])

    def test_missing_nodes_parse_from_object_info(self) -> None:
        prompt_payload = {
            "prompt": {
                "1": {"class_type": "ApiInput", "inputs": {}},
                "2": {"class_type": "Wan22Animate", "inputs": {}},
                "3": {"class_type": "ApiOutput", "inputs": {}},
            }
        }
        object_info_payload = {
            "ApiInput": {"input": {"required": {}}},
            "ApiOutput": {"input": {"required": {}}},
        }
        parsed = _missing_nodes_from_object_info(prompt_payload, object_info_payload)
        self.assertEqual(parsed, [MissingNodeRequirement(class_type="Wan22Animate")])

    def test_required_nodes_from_prompt_deduplicates_class_types(self) -> None:
        prompt_payload = {
            "prompt": {
                "1": {"class_type": "WanVideoVAELoader", "inputs": {}},
                "2": {"class_type": "WanVideoVAELoader", "inputs": {}},
                "3": {"class_type": "WanVideoModelLoader", "inputs": {}},
            }
        }
        parsed = _required_nodes_from_prompt(prompt_payload)
        self.assertEqual(
            parsed,
            [
                MissingNodeRequirement(class_type="WanVideoVAELoader"),
                MissingNodeRequirement(class_type="WanVideoModelLoader"),
            ],
        )

    def test_find_repo_urls_for_node_class_from_mapping(self) -> None:
        payload = {
            "https://github.com/example/custom-wan-node": [["Wan22Animate"]],
            "https://github.com/example/other": [["OtherNode"]],
        }
        urls = _find_repo_urls_for_node_class("Wan22Animate", payload)
        self.assertEqual(urls, {"https://github.com/example/custom-wan-node"})

    def test_install_missing_custom_nodes_installs_by_git_url(self) -> None:
        class FakeComfyClient:
            def __init__(self):
                self.installs = []

            def get_custom_node_mappings(self):
                return {
                    "https://github.com/example/custom-wan-node": [["Wan22Animate"]],
                }

            def get_custom_node_list(self):
                return {}

            def install_custom_node_by_git_url(self, git_url: str):
                self.installs.append(git_url)
                return "ok"

        client = FakeComfyClient()
        count = _install_missing_custom_nodes(
            comfy_client=client,
            requirements=[MissingNodeRequirement(class_type="Wan22Animate")],
        )
        self.assertEqual(count, 1)
        self.assertEqual(client.installs, ["https://github.com/kijai/ComfyUI-WanVideoWrapper"])

    def test_install_missing_custom_nodes_uses_override_when_catalog_missing(self) -> None:
        class FakeComfyClient:
            def __init__(self):
                self.installs = []

            def get_custom_node_mappings(self):
                return {}

            def get_custom_node_list(self):
                return {}

            def install_custom_node_by_git_url(self, git_url: str):
                self.installs.append(git_url)
                return "ok"

        client = FakeComfyClient()
        count = _install_missing_custom_nodes(
            comfy_client=client,
            requirements=[MissingNodeRequirement(class_type="Wan22Animate")],
        )
        self.assertEqual(count, 1)
        self.assertEqual(client.installs, ["https://github.com/kijai/ComfyUI-WanVideoWrapper"])

    def test_install_missing_custom_nodes_prefers_override_over_catalog_match(self) -> None:
        class FakeComfyClient:
            def __init__(self):
                self.installs = []

            def get_custom_node_mappings(self):
                return {
                    "https://github.com/LeonQ8/ComfyUI-Dynamic-Lora-Scheduler": [["WanVideoVAELoader"]],
                }

            def get_custom_node_list(self):
                return {}

            def install_custom_node_by_git_url(self, git_url: str):
                self.installs.append(git_url)
                return "ok"

        client = FakeComfyClient()
        count = _install_missing_custom_nodes(
            comfy_client=client,
            requirements=[MissingNodeRequirement(class_type="WanVideoVAELoader")],
        )
        self.assertEqual(count, 1)
        self.assertEqual(client.installs, ["https://github.com/kijai/ComfyUI-WanVideoWrapper"])

    def test_install_missing_custom_nodes_falls_back_to_git_clone(self) -> None:
        class FakeComfyClient:
            def get_custom_node_mappings(self):
                return {
                    "https://github.com/example/custom-wan-node": [["Wan22Animate"]],
                }

            def get_custom_node_list(self):
                return {}

            def install_custom_node_by_git_url(self, git_url: str):
                _ = git_url
                raise ComfyClientError("manager unavailable")

        with mock.patch(
            "comfy_endpoints.deploy.bootstrap._install_custom_node_by_git_clone",
            return_value=True,
        ) as mocked_clone:
            with mock.patch(
                "comfy_endpoints.deploy.bootstrap._install_custom_node_python_dependencies",
            ) as mocked_deps:
                with mock.patch(
                    "comfy_endpoints.deploy.bootstrap._install_custom_node_override_packages",
                ) as mocked_override_pkgs:
                    count = _install_missing_custom_nodes(
                        comfy_client=FakeComfyClient(),
                        requirements=[MissingNodeRequirement(class_type="Wan22Animate")],
                    )
        self.assertEqual(count, 1)
        mocked_clone.assert_called_once_with("https://github.com/kijai/ComfyUI-WanVideoWrapper")
        mocked_deps.assert_called_once_with("https://github.com/kijai/ComfyUI-WanVideoWrapper")
        mocked_override_pkgs.assert_called_once_with("Wan22Animate")

    def test_package_id_mapping_resolves_repo_urls(self) -> None:
        mappings_payload = {
            "example-wan-pack": ["Wan22Animate"],
            "other-pack": ["OtherNode"],
        }
        node_list_payload = {
            "node_packs": [
                {"id": "example-wan-pack", "files": ["https://github.com/example/custom-wan-node"]},
                {"id": "other-pack", "files": ["https://github.com/example/other-node"]},
            ]
        }
        package_ids = _find_package_ids_for_node_class("Wan22Animate", mappings_payload)
        urls = _find_repo_urls_for_package_ids(package_ids, node_list_payload)
        self.assertEqual(package_ids, {"example-wan-pack"})
        self.assertEqual(urls, {"https://github.com/example/custom-wan-node"})

    def test_install_missing_custom_nodes_from_catalog_uses_override(self) -> None:
        with mock.patch(
            "comfy_endpoints.deploy.bootstrap._install_custom_node_by_git_clone",
            return_value=True,
        ) as mocked_clone:
            with mock.patch(
                "comfy_endpoints.deploy.bootstrap._install_custom_node_python_dependencies",
            ) as mocked_deps:
                with mock.patch(
                    "comfy_endpoints.deploy.bootstrap._install_custom_node_override_packages",
                ) as mocked_override_pkgs:
                    count = _install_missing_custom_nodes_from_catalog(
                        requirements=[MissingNodeRequirement(class_type="WanVideoVAELoader")],
                        mapping_payload={
                            "https://github.com/LeonQ8/ComfyUI-Dynamic-Lora-Scheduler": [["WanVideoVAELoader"]]
                        },
                        list_payload={},
                    )
        self.assertEqual(count, 1)
        mocked_clone.assert_called_once_with("https://github.com/kijai/ComfyUI-WanVideoWrapper")
        mocked_deps.assert_called_once_with("https://github.com/kijai/ComfyUI-WanVideoWrapper")
        mocked_override_pkgs.assert_called_once_with("WanVideoVAELoader")

    def test_install_missing_models_from_entries(self) -> None:
        requirements = [
            MissingModelRequirement(
                input_name="model_name",
                filename="wanvideo/Wan2_1_VAE_bf16.safetensors",
                class_type="WanVideoVAELoader",
            )
        ]
        entries = [
            {
                "filename": "Wan2_1_VAE_bf16.safetensors",
                "url": "https://example.com/Wan2_1_VAE_bf16.safetensors",
                "type": "vae",
            }
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            with mock.patch("comfy_endpoints.deploy.bootstrap._download_file") as mocked_download:
                count = _install_missing_models_from_entries(
                    requirements=requirements,
                    entries=entries,
                    cache_models_root=root / "cache_models",
                )

            self.assertEqual(count, 1)
            mocked_download.assert_called_once()
            target_path = mocked_download.call_args.args[1]
            self.assertEqual(
                str(target_path),
                str(root / "cache_models" / "vae" / "wanvideo" / "Wan2_1_VAE_bf16.safetensors"),
            )

    def test_resolve_missing_custom_nodes_from_catalog_reports_unresolved(self) -> None:
        with mock.patch(
            "comfy_endpoints.deploy.bootstrap._install_custom_node_by_git_clone",
            return_value=False,
        ):
            _, unresolved = _resolve_missing_custom_nodes_from_catalog(
                requirements=[MissingNodeRequirement(class_type="UnknownNodeClass")],
                mapping_payload={},
                list_payload={},
            )
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(unresolved[0]["reason"], "no_catalog_match")

    def test_resolve_missing_models_from_entries_reports_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, unresolved = _resolve_missing_models_from_entries(
                requirements=[
                    MissingModelRequirement(
                        input_name="model",
                        filename="WanVideo/2_2/missing.safetensors",
                        class_type="WanVideoModelLoader",
                    )
                ],
                entries=[],
                cache_models_root=Path(tmp_dir) / "cache_models",
            )
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(unresolved[0]["reason"], "no_model_catalog_entries")

    def test_prestart_resolver_returns_error_payload_on_config_parse_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch(
                "comfy_endpoints.deploy.bootstrap._load_app_artifact_specs_from_env",
                side_effect=RuntimeError("bad config"),
            ):
                error_payload = _prestart_resolve_artifacts_or_error(
                    preflight_payload={"prompt": {}},
                    cache_models_root=Path(tmp_dir) / "cache_models",
                )
        self.assertIsNotNone(error_payload)
        assert error_payload is not None
        self.assertEqual(error_payload["status"], "artifact_resolver_failed")
        self.assertEqual(error_payload["stage"], "artifact_config")


if __name__ == "__main__":
    unittest.main()
