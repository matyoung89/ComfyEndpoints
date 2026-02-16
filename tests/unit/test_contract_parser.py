from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.contracts.parser import ContractError, load_structured_file


class ContractParserTest(unittest.TestCase):
    def test_load_structured_file_supports_json_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            json_path = root / "app.json"
            json_path.write_text(json.dumps({"app_id": "demo"}), encoding="utf-8")
            payload = load_structured_file(json_path)
            self.assertEqual(payload["app_id"], "demo")

    def test_load_structured_file_rejects_yaml_extension(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            yaml_path = root / "app.yaml"
            yaml_path.write_text('{"app_id": "demo"}', encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "Only \\.json is supported"):
                load_structured_file(yaml_path)


if __name__ == "__main__":
    unittest.main()
