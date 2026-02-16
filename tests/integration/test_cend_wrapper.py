from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


class CendWrapperTest(unittest.TestCase):
    def test_cend_help_runs(self) -> None:
        project_root = Path(__file__).resolve().parents[2]
        wrapper = project_root / "cend"
        result = subprocess.run(
            [str(wrapper), "--help"],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root),
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("usage:", result.stdout.lower())
        self.assertIn("comfy-endpoints", result.stdout.lower())


if __name__ == "__main__":
    unittest.main()
