from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


class ComfyPluginContractTest(unittest.TestCase):
    def test_api_output_is_marked_output_node(self) -> None:
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

        api_output_cls = module.NODE_CLASS_MAPPINGS["ApiOutput"]
        self.assertTrue(getattr(api_output_cls, "OUTPUT_NODE", False))


if __name__ == "__main__":
    unittest.main()
