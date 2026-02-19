from __future__ import annotations

import importlib.util
import os
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
        input_types = module.NODE_CLASS_MAPPINGS["ApiOutput"].INPUT_TYPES()
        self.assertIn("value", input_types["optional"])
        self.assertEqual(input_types["optional"]["value"][0], "STRING")
        self.assertIn("image", input_types["optional"])
        self.assertEqual(input_types["optional"]["image"][0], "IMAGE")

    def test_path_tensor_nodes_are_registered(self) -> None:
        module = self._load_module()
        self.assertIn("PathToTensor", module.NODE_CLASS_MAPPINGS)
        self.assertIn("PathToImageTensor", module.NODE_CLASS_MAPPINGS)
        self.assertIn("PathToVideoTensor", module.NODE_CLASS_MAPPINGS)
        self.assertIn("ImageTensorToPath", module.NODE_CLASS_MAPPINGS)
        self.assertIn("VideoTensorToPath", module.NODE_CLASS_MAPPINGS)

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

    def test_api_input_resolves_media_file_id_to_storage_path(self) -> None:
        module = self._load_module()
        from comfy_endpoints.gateway.job_store import JobStore

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            store = JobStore(root / "jobs.db")
            record = store.create_file(
                content=b"image-bytes",
                media_type="image/png",
                source="uploaded",
                app_id="demo",
                original_name="reference.png",
            )

            node = module.NODE_CLASS_MAPPINGS["ApiInput"]()
            resolved_value = node.execute(
                name="reference_image",
                type="image/png",
                required=True,
                value=record.file_id,
                ce_state_db=str(root / "jobs.db"),
            )[0]
            self.assertEqual(resolved_value, str(record.storage_path))

    def test_api_input_rejects_media_file_id_without_state_db(self) -> None:
        module = self._load_module()
        node = module.NODE_CLASS_MAPPINGS["ApiInput"]()
        previous_state_db = os.environ.get("COMFY_ENDPOINTS_STATE_DB")
        if "COMFY_ENDPOINTS_STATE_DB" in os.environ:
            os.environ.pop("COMFY_ENDPOINTS_STATE_DB")
        try:
            with self.assertRaisesRegex(ValueError, "Missing ce_state_db"):
                node.execute(
                    name="reference_image",
                    type="image/png",
                    required=True,
                    value="fid_missing",
                    ce_state_db="",
                )
        finally:
            if previous_state_db is not None:
                os.environ["COMFY_ENDPOINTS_STATE_DB"] = previous_state_db

    def test_api_input_rejects_unknown_media_file_id(self) -> None:
        module = self._load_module()
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            node = module.NODE_CLASS_MAPPINGS["ApiInput"]()
            with self.assertRaisesRegex(ValueError, "Unknown media file_id"):
                node.execute(
                    name="driving_video",
                    type="video/mp4",
                    required=True,
                    value="fid_unknown",
                    ce_state_db=str(root / "jobs.db"),
                )

    def test_api_output_registers_video_file_and_returns_file_id(self) -> None:
        module = self._load_module()
        from comfy_endpoints.gateway.job_store import JobStore

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_db = root / "jobs.db"
            video_path = root / "clip.mp4"
            video_path.write_bytes(b"video-bytes")
            previous_app_id = os.environ.get("COMFY_ENDPOINTS_APP_ID")
            os.environ["COMFY_ENDPOINTS_APP_ID"] = "demo"
            try:
                node = module.NODE_CLASS_MAPPINGS["ApiOutput"]()
                output_json = node.execute(
                    name="output_video",
                    type="video/mp4",
                    value=str(video_path),
                    ce_job_id="",
                    ce_artifacts_dir="",
                    ce_state_db=str(state_db),
                )[0]
            finally:
                if previous_app_id is None:
                    os.environ.pop("COMFY_ENDPOINTS_APP_ID", None)
                else:
                    os.environ["COMFY_ENDPOINTS_APP_ID"] = previous_app_id

            payload = module.json.loads(output_json)
            file_id = str(payload["value"])
            self.assertTrue(file_id.startswith("fid_"))

            store = JobStore(state_db)
            record = store.get_file(file_id)
            assert record is not None
            self.assertEqual(record.media_type, "video/mp4")
            self.assertEqual(record.source, "generated")
            self.assertEqual(record.app_id, "demo")

    def test_path_to_image_tensor_and_back_roundtrip(self) -> None:
        module = self._load_module()
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            input_image_path = root / "in.png"
            output_image_path = root / "out.png"

            image = Image.new("RGB", (8, 8), color=(10, 120, 200))
            image.save(input_image_path)

            to_tensor = module.NODE_CLASS_MAPPINGS["PathToImageTensor"]()
            tensor_value = to_tensor.execute(path=str(input_image_path))[0]
            self.assertEqual(int(tensor_value.shape[0]), 1)
            self.assertEqual(int(tensor_value.shape[1]), 8)
            self.assertEqual(int(tensor_value.shape[2]), 8)
            self.assertEqual(int(tensor_value.shape[3]), 3)

            to_path = module.NODE_CLASS_MAPPINGS["ImageTensorToPath"]()
            saved_path = to_path.execute(image=tensor_value, path=str(output_image_path), format="png")[0]
            self.assertTrue(Path(saved_path).exists())
            saved_image = Image.open(saved_path)
            self.assertEqual(saved_image.size, (8, 8))

    def test_path_to_video_tensor_resize_dimension_resolution(self) -> None:
        module = self._load_module()
        node = module.NODE_CLASS_MAPPINGS["PathToVideoTensor"]()

        width_value, height_value = node._resolve_resize_dims(832, 480, 416, 240)
        self.assertEqual((width_value, height_value), (416, 240))

        width_value, height_value = node._resolve_resize_dims(832, 480, 416, 0)
        self.assertEqual((width_value, height_value), (416, 240))

        width_value, height_value = node._resolve_resize_dims(832, 480, 0, 240)
        self.assertEqual((width_value, height_value), (416, 240))


if __name__ == "__main__":
    unittest.main()
