from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from comfy_endpoints.utils.env_loader import load_local_env


class EnvLoaderTest(unittest.TestCase):
    def test_loads_dotenv_local_from_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            env_file = root / ".env.local"
            env_file.write_text("RUNPOD_API_KEY=from-env-local\n", encoding="utf-8")

            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch("pathlib.Path.cwd", return_value=root):
                    loaded = load_local_env()

                self.assertEqual(loaded, env_file.resolve())
                self.assertEqual(os.environ.get("RUNPOD_API_KEY"), "from-env-local")


if __name__ == "__main__":
    unittest.main()
