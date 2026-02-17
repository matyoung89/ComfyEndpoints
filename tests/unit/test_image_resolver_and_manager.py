from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.models import (
    AppSpecV1,
    AuthMode,
    BuildPluginSpec,
    BuildSpec,
    CachePolicy,
    EndpointSpec,
    ProviderName,
)
from comfy_endpoints.runtime.image_manager import ImageManager
from comfy_endpoints.runtime.image_resolver import resolve_comfybase_image, resolve_golden_image


def _app_spec(tmp_root: Path) -> AppSpecV1:
    return AppSpecV1(
        app_id="demo",
        version="v1",
        workflow_path=tmp_root / "workflow.json",
        provider=ProviderName.RUNPOD,
        gpu_profile="A10G",
        regions=["US"],
        env={},
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
        build=BuildSpec(
            comfy_version="0.3.26",
            plugins=[BuildPluginSpec(repo="https://example.com/plugin", ref="main")],
            image_repository="ghcr.io/comfy-endpoints/golden",
            base_image_repository="ghcr.io/comfy-endpoints/comfybase",
        ),
    )


class ImageResolverAndManagerTest(unittest.TestCase):
    def test_resolver_uses_explicit_image_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            app_spec = _app_spec(Path(tmp_dir))
            app_spec.build.image_ref = "ghcr.io/custom/golden:fixed"
            self.assertEqual(
                resolve_golden_image(app_spec, comfybase_image_ref="ghcr.io/example/base:tag"),
                "ghcr.io/custom/golden:fixed",
            )

    def test_resolver_generates_fingerprint_tag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "docker").mkdir(parents=True)
            (root / "docker" / "Dockerfile.comfybase").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            (root / "docker" / "Dockerfile.golden").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            app_spec = _app_spec(root)
            app_spec.build.base_dockerfile_path = str(root / "docker" / "Dockerfile.comfybase")
            app_spec.build.dockerfile_path = str(root / "docker" / "Dockerfile.golden")
            comfybase_image = resolve_comfybase_image(app_spec)
            image = resolve_golden_image(app_spec, comfybase_image_ref=comfybase_image)
            self.assertRegex(comfybase_image, r"^ghcr.io/comfy-endpoints/comfybase:0\.3\.26-base-[0-9a-f]{12}$")
            self.assertRegex(image, r"^ghcr.io/comfy-endpoints/golden:0\.3\.26-v1-[0-9a-f]{12}$")

    def test_image_manager_builds_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "docker").mkdir(parents=True)
            (root / "docker" / "Dockerfile.comfybase").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            (root / "docker" / "Dockerfile.golden").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            app_spec = _app_spec(root)
            app_spec.build.base_dockerfile_path = str(root / "docker" / "Dockerfile.comfybase")
            app_spec.build.dockerfile_path = str(root / "docker" / "Dockerfile.golden")
            app_spec.build.build_context = str(root)
            app_spec.build.base_build_context = str(root)

            manager = ImageManager(project_root=root)

            with mock.patch("comfy_endpoints.runtime.image_fingerprint._hash_paths", return_value="src-hash"):
                with mock.patch("comfy_endpoints.runtime.image_fingerprint._git_fingerprint", return_value="git-hash"):
                    with mock.patch.object(manager, "_docker_available", return_value=True):
                        with mock.patch("subprocess.run") as mocked_run:
                            mocked_run.side_effect = [
                                mock.Mock(returncode=1, stdout="", stderr="not found"),
                                mock.Mock(returncode=0, stdout="built", stderr=""),
                                mock.Mock(returncode=0, stdout="exists", stderr=""),
                                mock.Mock(returncode=1, stdout="", stderr="not found"),
                                mock.Mock(returncode=0, stdout="built", stderr=""),
                                mock.Mock(returncode=0, stdout="exists", stderr=""),
                            ]
                            result = manager.ensure_image(app_spec)

            self.assertTrue(result.built)
            self.assertTrue(result.image_exists)

    def test_image_manager_dispatches_github_actions_without_docker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "docker").mkdir(parents=True)
            (root / "docker" / "Dockerfile.comfybase").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            (root / "docker" / "Dockerfile.golden").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            app_spec = _app_spec(root)
            app_spec.build.base_dockerfile_path = str(root / "docker" / "Dockerfile.comfybase")
            app_spec.build.dockerfile_path = str(root / "docker" / "Dockerfile.golden")

            manager = ImageManager(project_root=root)
            with mock.patch.dict(
                "os.environ",
                {
                    "COMFY_ENDPOINTS_IMAGE_BUILD_BACKEND": "github_actions",
                    "GITHUB_TOKEN": "ghp_test",
                    "GITHUB_REPOSITORY": "owner/repo",
                },
                clear=False,
            ):
                    with mock.patch.object(manager, "_docker_available", return_value=False):
                        with mock.patch.object(manager, "_registry_manifest_exists") as mocked_exists:
                            mocked_exists.side_effect = [False, True, False, True]
                            with mock.patch.object(manager, "_wait_for_github_workflow_run", return_value=None):
                                with mock.patch("urllib.request.urlopen") as mocked_urlopen:
                                    mocked_urlopen.return_value.__enter__.return_value.status = 204
                                    result = manager.ensure_image(app_spec)

            self.assertTrue(result.built)

    def test_image_manager_passes_plugin_build_arg_for_golden_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "docker").mkdir(parents=True)
            (root / "docker" / "Dockerfile.comfybase").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            (root / "docker" / "Dockerfile.golden").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            app_spec = _app_spec(root)
            app_spec.build.base_dockerfile_path = str(root / "docker" / "Dockerfile.comfybase")
            app_spec.build.dockerfile_path = str(root / "docker" / "Dockerfile.golden")

            manager = ImageManager(project_root=root)
            with mock.patch.object(manager, "_image_exists", side_effect=[True, False]):
                with mock.patch.object(manager, "_build_and_push") as mocked_build:
                    with mock.patch.object(manager, "_wait_for_image", return_value=None):
                        with mock.patch(
                            "comfy_endpoints.runtime.image_fingerprint._hash_paths",
                            return_value="src-hash",
                        ):
                            with mock.patch(
                                "comfy_endpoints.runtime.image_fingerprint._git_fingerprint",
                                return_value="git-hash",
                            ):
                                manager.ensure_image(app_spec)

            _, kwargs = mocked_build.call_args
            self.assertIn("build_args", kwargs)
            build_args = kwargs["build_args"]
            self.assertIn("BASE_IMAGE", build_args)
            self.assertIn("COMFY_PLUGINS_JSON", build_args)
            self.assertIn("https://example.com/plugin", build_args["COMFY_PLUGINS_JSON"])

    def test_image_manager_includes_manager_plugin_and_excludes_core_comfyui(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "docker").mkdir(parents=True)
            (root / "docker" / "Dockerfile.comfybase").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            (root / "docker" / "Dockerfile.golden").write_text("FROM python:3.11-slim\n", encoding="utf-8")
            app_spec = _app_spec(root)
            app_spec.build.base_dockerfile_path = str(root / "docker" / "Dockerfile.comfybase")
            app_spec.build.dockerfile_path = str(root / "docker" / "Dockerfile.golden")
            app_spec.build.plugins = [
                BuildPluginSpec(repo="https://github.com/comfyanonymous/ComfyUI", ref="master"),
                BuildPluginSpec(repo="https://github.com/Comfy-Org/ComfyUI-Manager", ref="main"),
                BuildPluginSpec(repo="https://example.com/plugin", ref="main"),
            ]

            manager = ImageManager(project_root=root)
            with mock.patch.object(manager, "_image_exists", side_effect=[True, False]):
                with mock.patch.object(manager, "_build_and_push") as mocked_build:
                    with mock.patch.object(manager, "_wait_for_image", return_value=None):
                        with mock.patch(
                            "comfy_endpoints.runtime.image_fingerprint._hash_paths",
                            return_value="src-hash",
                        ):
                            with mock.patch(
                                "comfy_endpoints.runtime.image_fingerprint._git_fingerprint",
                                return_value="git-hash",
                            ):
                                manager.ensure_image(app_spec)

            _, kwargs = mocked_build.call_args
            build_args = kwargs["build_args"]
            plugin_payload = json.loads(build_args["COMFY_PLUGINS_JSON"])
            plugin_repos = {entry["repo"] for entry in plugin_payload}
            self.assertNotIn("https://github.com/comfyanonymous/ComfyUI", plugin_repos)
            self.assertIn("https://github.com/Comfy-Org/ComfyUI-Manager", plugin_repos)
            self.assertIn("https://example.com/plugin", plugin_repos)


if __name__ == "__main__":
    unittest.main()
