from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


class ComfyPluginContractTest(unittest.TestCase):
    @staticmethod
    def _load_module():
        plugin_path = (
            Path(__file__).resolve().parents[2]
            / "comfy_plugin"
            / "comfy_endpoints_contract"
            / "api_contract.py"
        )
        spec = importlib.util.spec_from_file_location("comfy_endpoints_contract_api_contract", plugin_path)
        assert spec is not None
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module

    def test_api_output_is_marked_output_node(self) -> None:
        module = self._load_module()
        api_output_cls = module.NODE_CLASS_MAPPINGS["ApiOutput"]
        self.assertTrue(getattr(api_output_cls, "OUTPUT_NODE", False))

    def test_api_output_accepts_generic_value_input(self) -> None:
        module = self._load_module()
        inputs = module.NODE_CLASS_MAPPINGS["ApiOutput"].INPUT_TYPES()["required"]
        self.assertEqual(inputs["value"][0], "*")

    def test_export_contract_rejects_duplicate_output_names(self) -> None:
        module = self._load_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            workflow_path = root / "workflow.json"
            output_path = root / "workflow.contract.json"
            workflow_path.write_text(
                """{
  "nodes": [
    {"id": 1, "type": "ApiInput", "widgets_values": ["prompt", "string", true, ""]},
    {"id": 2, "type": "ApiOutput", "widgets_values": ["image", "image/png", ""]},
    {"id": 3, "type": "ApiOutput", "widgets_values": ["image", "image/png", ""]}
  ]
}
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Duplicate ApiOutput name"):
                module.export_contract_from_workflow(workflow_path, output_path)


if __name__ == "__main__":
    unittest.main()
