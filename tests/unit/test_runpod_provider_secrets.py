from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.providers.runpod_provider import RunpodProvider


class RunpodProviderSecretsTest(unittest.TestCase):
    def test_uses_env_key_when_present(self) -> None:
        provider = RunpodProvider()
        with mock.patch.dict(os.environ, {"RUNPOD_API_KEY": "env-key"}, clear=False):
            self.assertEqual(provider._api_key(), "env-key")

    def test_uses_keychain_when_env_missing(self) -> None:
        provider = RunpodProvider()
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_env_file = str(Path(tmp_dir) / "missing.env")
            with mock.patch.dict(
                os.environ,
                {
                    "RUNPOD_API_KEY": "",
                    "USER": "mat",
                    "COMFY_ENDPOINTS_ENV_FILE": missing_env_file,
                },
                clear=False,
            ):
                with mock.patch("subprocess.run") as mocked_run:
                    mocked_run.return_value = mock.Mock(stdout="keychain-key\n")
                    self.assertEqual(provider._api_key(), "keychain-key")

    def test_errors_when_no_env_and_no_keychain(self) -> None:
        provider = RunpodProvider()
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_env_file = str(Path(tmp_dir) / "missing.env")
            with mock.patch.dict(
                os.environ,
                {
                    "RUNPOD_API_KEY": "",
                    "USER": "mat",
                    "COMFY_ENDPOINTS_ENV_FILE": missing_env_file,
                },
                clear=False,
            ):
                with mock.patch("subprocess.run", side_effect=FileNotFoundError):
                    with self.assertRaisesRegex(RuntimeError, "Missing RunPod API key"):
                        provider._api_key()

    def test_uses_dotenv_local_when_env_missing(self) -> None:
        provider = RunpodProvider()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / ".env.local").write_text("RUNPOD_API_KEY=dotenv-key\n", encoding="utf-8")
            with mock.patch("subprocess.run") as mocked_run:
                mocked_run.side_effect = FileNotFoundError
                with mock.patch.dict(
                    os.environ,
                    {
                        "RUNPOD_API_KEY": "",
                        "USER": "mat",
                    },
                    clear=False,
                ):
                    with mock.patch("pathlib.Path.cwd", return_value=root):
                        self.assertEqual(provider._api_key(), "dotenv-key")


if __name__ == "__main__":
    unittest.main()
