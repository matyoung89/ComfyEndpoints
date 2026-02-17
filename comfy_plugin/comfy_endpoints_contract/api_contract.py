from __future__ import annotations

import io
import json
import mimetypes
import os
import re
import tempfile
from pathlib import Path
from typing import Any


SCALAR_CONTRACT_TYPES = {"string", "integer", "number", "boolean", "object", "array"}
MEDIA_TYPE_PATTERN = re.compile(r"^(image|video|audio|file)/[A-Za-z0-9][A-Za-z0-9.+-]*$")
MEDIA_PREFIXES = ("image/", "video/", "audio/", "file/")


def _is_media_contract_type(type_name: str) -> bool:
    normalized = type_name.strip().lower()
    return normalized.startswith(MEDIA_PREFIXES)


class ApiInputNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "name": ("STRING", {"default": "prompt"}),
                "type": ("STRING", {"default": "string"}),
                "required": ("BOOLEAN", {"default": True}),
                "value": ("STRING", {"default": ""}),
            },
            "optional": {
                "ce_state_db": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints"

    @staticmethod
    def _resolve_media_file_id(*, name: str, type: str, value: str, state_db_path: str) -> str:
        from comfy_endpoints.gateway.job_store import JobStore

        normalized_type = type.strip().lower()
        if not _is_media_contract_type(normalized_type):
            return value

        trimmed_value = value.strip()
        if not trimmed_value.startswith("fid_"):
            return value

        if not state_db_path:
            raise ValueError("Missing ce_state_db for media input")

        store = JobStore(Path(state_db_path))
        record = store.get_file(trimmed_value)
        if not record:
            raise ValueError(f"Unknown media file_id for input '{name}': {trimmed_value}")
        if not record.storage_path.exists():
            raise ValueError(f"Media file_id has missing storage for input '{name}': {trimmed_value}")
        return str(record.storage_path)

    def execute(self, name: str, type: str, required: bool, value: str, ce_state_db: str = "") -> tuple[str]:
        _ = (name, type, required)
        state_db_path = ce_state_db.strip() or os.getenv("COMFY_ENDPOINTS_STATE_DB", "").strip()
        resolved_value = self._resolve_media_file_id(
            name=name,
            type=type,
            value=value,
            state_db_path=state_db_path,
        )
        return (resolved_value,)


class ApiOutputNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "name": ("STRING", {"default": "image"}),
                "type": ("STRING", {"default": "image/png"}),
            },
            "optional": {
                "value": ("STRING", {"default": ""}),
                "image": ("IMAGE",),
                "ce_job_id": ("STRING", {"default": ""}),
                "ce_artifacts_dir": ("STRING", {"default": ""}),
                "ce_state_db": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints"
    OUTPUT_NODE = True

    @staticmethod
    def _write_artifact(artifacts_dir: str, job_id: str, output_name: str, output_value: object) -> None:
        if not artifacts_dir or not job_id:
            return
        artifact_dir = Path(artifacts_dir) / job_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / Path(output_name).name
        if isinstance(output_value, str):
            artifact_path.write_text(output_value, encoding="utf-8")
            return
        artifact_path.write_text(json.dumps(output_value), encoding="utf-8")

    @staticmethod
    def _encode_image(image: object, media_type: str) -> tuple[bytes, str]:
        import numpy as np
        from PIL import Image

        array_value = image
        if isinstance(array_value, (list, tuple)) and array_value:
            array_value = array_value[0]
        if hasattr(array_value, "detach"):
            array_value = array_value.detach().cpu().numpy()
        array_value = np.asarray(array_value)
        if array_value.ndim == 4:
            array_value = array_value[0]
        if array_value.ndim != 3:
            raise ValueError("ApiOutput image must be rank-3 or rank-4 tensor")

        if array_value.dtype != np.uint8:
            if float(array_value.max()) <= 1.0:
                array_value = np.clip(array_value, 0.0, 1.0) * 255.0
            else:
                array_value = np.clip(array_value, 0.0, 255.0)
            array_value = array_value.astype(np.uint8)

        channels = int(array_value.shape[2])
        if channels == 1:
            mode = "L"
            array_value = array_value[:, :, 0]
        elif channels == 3:
            mode = "RGB"
        elif channels == 4:
            mode = "RGBA"
        else:
            raise ValueError(f"Unsupported image channel count: {channels}")

        normalized_media_type = media_type.strip().lower()
        format_by_subtype = {
            "image/png": ("PNG", ".png"),
            "image/jpeg": ("JPEG", ".jpg"),
            "image/webp": ("WEBP", ".webp"),
        }
        image_format, suffix = format_by_subtype.get(normalized_media_type, ("PNG", ".png"))
        buffer = io.BytesIO()
        Image.fromarray(array_value, mode=mode).save(buffer, format=image_format)
        return buffer.getvalue(), suffix

    @staticmethod
    def _create_generated_file_id(
        *,
        output_name: str,
        media_type: str,
        image_value: object | None,
        state_db_path: str,
    ) -> str:
        from comfy_endpoints.gateway.job_store import JobStore

        if not state_db_path:
            raise ValueError("Missing ce_state_db for media output")
        if image_value is None:
            raise ValueError("Missing image input for media output")

        payload_bytes, suffix = ApiOutputNode._encode_image(image_value, media_type)
        app_id = os.getenv("COMFY_ENDPOINTS_APP_ID", "").strip() or None
        store = JobStore(Path(state_db_path))
        record = store.create_file(
            content=payload_bytes,
            media_type=media_type,
            source="generated",
            app_id=app_id,
            original_name=f"{Path(output_name).name}{suffix}",
        )
        return record.file_id

    @staticmethod
    def _create_generated_binary_file_id(
        *,
        output_name: str,
        media_type: str,
        media_value: object,
        state_db_path: str,
    ) -> str:
        from comfy_endpoints.gateway.job_store import JobStore

        if not state_db_path:
            raise ValueError("Missing ce_state_db for media output")

        payload_bytes: bytes
        suffix = ""
        if isinstance(media_value, (bytes, bytearray)):
            payload_bytes = bytes(media_value)
        elif isinstance(media_value, str):
            media_path = Path(media_value.strip())
            if not media_path.exists() or not media_path.is_file():
                raise ValueError(f"Missing media output file: {media_value}")
            payload_bytes = media_path.read_bytes()
            suffix = media_path.suffix
        else:
            raise ValueError(f"Unsupported media output payload type: {type(media_value)}")

        if not payload_bytes:
            raise ValueError("Media output payload is empty")

        app_id = os.getenv("COMFY_ENDPOINTS_APP_ID", "").strip() or None
        normalized_name = Path(output_name).name
        if suffix:
            original_name = f"{normalized_name}{suffix}" if not normalized_name.endswith(suffix) else normalized_name
        else:
            guessed_suffix = mimetypes.guess_extension(media_type) or ""
            original_name = f"{normalized_name}{guessed_suffix}"

        store = JobStore(Path(state_db_path))
        record = store.create_file(
            content=payload_bytes,
            media_type=media_type,
            source="generated",
            app_id=app_id,
            original_name=original_name,
        )
        return record.file_id

    def execute(
        self,
        name: str,
        type: str,
        value: object = "",
        image: object | None = None,
        ce_job_id: str = "",
        ce_artifacts_dir: str = "",
        ce_state_db: str = "",
    ) -> tuple[str]:
        normalized_type = type.strip().lower()
        media_output = _is_media_contract_type(normalized_type)
        resolved_value = image if image is not None else value
        if media_output:
            if isinstance(value, str) and value.strip().startswith("fid_"):
                resolved_value = value.strip()
            elif normalized_type.startswith("image/"):
                state_db_path = ce_state_db.strip() or os.getenv("COMFY_ENDPOINTS_STATE_DB", "").strip()
                resolved_value = self._create_generated_file_id(
                    output_name=name,
                    media_type=normalized_type,
                    image_value=image,
                    state_db_path=state_db_path,
                )
            else:
                state_db_path = ce_state_db.strip() or os.getenv("COMFY_ENDPOINTS_STATE_DB", "").strip()
                resolved_value = self._create_generated_binary_file_id(
                    output_name=name,
                    media_type=normalized_type,
                    media_value=value,
                    state_db_path=state_db_path,
                )

        artifacts_dir = ce_artifacts_dir.strip() or os.getenv(
            "COMFY_ENDPOINTS_ARTIFACTS_DIR", "/opt/comfy_endpoints/runtime/artifacts"
        )
        self._write_artifact(artifacts_dir=artifacts_dir, job_id=ce_job_id.strip(), output_name=name, output_value=resolved_value)
        output_payload = {
            "name": name,
            "type": type,
            "value": resolved_value,
        }
        return (json.dumps(output_payload, default=str),)


class PathToImageTensorNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "path": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("image",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints/Path"

    @staticmethod
    def _load_image_array(path: str) -> Any:
        import numpy as np
        from PIL import Image

        image_path = Path(path.strip())
        if not image_path.exists() or not image_path.is_file():
            raise ValueError(f"Invalid image path: {path}")
        image = Image.open(image_path).convert("RGB")
        array_value = np.asarray(image, dtype=np.float32) / 255.0
        return array_value

    def execute(self, path: str) -> tuple[Any]:
        import torch

        array_value = self._load_image_array(path)
        tensor_value = torch.from_numpy(array_value).unsqueeze(0)
        return (tensor_value,)


class PathToVideoTensorNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "path": ("STRING", {"default": ""}),
            },
            "optional": {
                "max_frames": ("INT", {"default": 0, "min": 0, "max": 1048576}),
            },
        }

    RETURN_TYPES = ("IMAGE", "INT", "FLOAT")
    RETURN_NAMES = ("frames", "frame_count", "fps")
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints/Path"

    def execute(self, path: str, max_frames: int = 0) -> tuple[Any, int, float]:
        import cv2
        import numpy as np
        import torch

        video_path = Path(path.strip())
        if not video_path.exists() or not video_path.is_file():
            raise ValueError(f"Invalid video path: {path}")

        capture = cv2.VideoCapture(str(video_path))
        if not capture.isOpened():
            raise ValueError(f"Unable to open video: {path}")
        try:
            fps_value = float(capture.get(cv2.CAP_PROP_FPS) or 0.0)
            if fps_value <= 0.0:
                fps_value = 24.0
            frames: list[Any] = []
            while True:
                if max_frames > 0 and len(frames) >= max_frames:
                    break
                ok, frame = capture.read()
                if not ok:
                    break
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                normalized = rgb_frame.astype(np.float32) / 255.0
                frames.append(normalized)
        finally:
            capture.release()

        if not frames:
            raise ValueError(f"No frames decoded from video: {path}")

        stacked = np.stack(frames, axis=0)
        tensor_value = torch.from_numpy(stacked)
        return (tensor_value, int(tensor_value.shape[0]), float(fps_value))


class PathToTensorNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "path": ("STRING", {"default": ""}),
            },
            "optional": {
                "max_frames": ("INT", {"default": 0, "min": 0, "max": 1048576}),
            },
        }

    RETURN_TYPES = ("IMAGE", "STRING", "INT", "FLOAT")
    RETURN_NAMES = ("tensor", "media_kind", "frame_count", "fps")
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints/Path"

    def execute(self, path: str, max_frames: int = 0) -> tuple[Any, str, int, float]:
        normalized_suffix = Path(path.strip()).suffix.lower()
        video_suffixes = {".mp4", ".mov", ".webm", ".mkv", ".avi", ".m4v"}
        if normalized_suffix in video_suffixes:
            frames, frame_count, fps_value = PathToVideoTensorNode().execute(path=path, max_frames=max_frames)
            return (frames, "video", frame_count, fps_value)
        image_tensor = PathToImageTensorNode().execute(path=path)[0]
        return (image_tensor, "image", 1, 0.0)


class ImageTensorToPathNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "image": ("IMAGE",),
            },
            "optional": {
                "path": ("STRING", {"default": ""}),
                "format": ("STRING", {"default": "png"}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("path",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints/Path"

    @staticmethod
    def _to_hwc_uint8(image: Any) -> Any:
        import numpy as np

        array_value = image
        if isinstance(array_value, (list, tuple)) and array_value:
            array_value = array_value[0]
        if hasattr(array_value, "detach"):
            array_value = array_value.detach().cpu().numpy()
        array_value = np.asarray(array_value)
        if array_value.ndim == 4:
            array_value = array_value[0]
        if array_value.ndim != 3:
            raise ValueError("Image tensor must be rank-3 or rank-4")
        if array_value.dtype != np.uint8:
            if float(array_value.max()) <= 1.0:
                array_value = np.clip(array_value, 0.0, 1.0) * 255.0
            else:
                array_value = np.clip(array_value, 0.0, 255.0)
            array_value = array_value.astype(np.uint8)
        return array_value

    def execute(self, image: Any, path: str = "", format: str = "png") -> tuple[str]:
        from PIL import Image

        array_value = self._to_hwc_uint8(image)
        normalized_format = (format or "png").strip().lower()
        suffix = f".{normalized_format}"
        if path.strip():
            output_path = Path(path.strip())
            if output_path.suffix:
                suffix = output_path.suffix
        else:
            fd, tmp_path = tempfile.mkstemp(prefix="comfy_image_", suffix=suffix)
            os.close(fd)
            output_path = Path(tmp_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        channels = int(array_value.shape[2])
        if channels == 1:
            image_mode = "L"
            save_value = array_value[:, :, 0]
        elif channels == 3:
            image_mode = "RGB"
            save_value = array_value
        elif channels == 4:
            image_mode = "RGBA"
            save_value = array_value
        else:
            raise ValueError(f"Unsupported image channel count: {channels}")
        Image.fromarray(save_value, mode=image_mode).save(output_path)
        return (str(output_path),)


class VideoTensorToPathNode:
    @classmethod
    def INPUT_TYPES(cls) -> dict[str, dict[str, tuple]]:
        return {
            "required": {
                "frames": ("IMAGE",),
            },
            "optional": {
                "path": ("STRING", {"default": ""}),
                "fps": ("FLOAT", {"default": 24.0, "min": 1.0, "max": 240.0}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("path",)
    FUNCTION = "execute"
    CATEGORY = "ComfyEndpoints/Path"

    @staticmethod
    def _to_nhwc_uint8(frames: Any) -> Any:
        import numpy as np

        array_value = frames
        if hasattr(array_value, "detach"):
            array_value = array_value.detach().cpu().numpy()
        array_value = np.asarray(array_value)
        if array_value.ndim == 3:
            array_value = array_value[None, ...]
        if array_value.ndim != 4:
            raise ValueError("Video frames tensor must be rank-4 or rank-3")
        if array_value.dtype != np.uint8:
            if float(array_value.max()) <= 1.0:
                array_value = np.clip(array_value, 0.0, 1.0) * 255.0
            else:
                array_value = np.clip(array_value, 0.0, 255.0)
            array_value = array_value.astype(np.uint8)
        return array_value

    def execute(self, frames: Any, path: str = "", fps: float = 24.0) -> tuple[str]:
        import cv2

        frame_array = self._to_nhwc_uint8(frames)
        if path.strip():
            output_path = Path(path.strip())
        else:
            fd, tmp_path = tempfile.mkstemp(prefix="comfy_video_", suffix=".mp4")
            os.close(fd)
            output_path = Path(tmp_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        frame_height = int(frame_array.shape[1])
        frame_width = int(frame_array.shape[2])
        writer = cv2.VideoWriter(
            str(output_path),
            cv2.VideoWriter_fourcc(*"mp4v"),
            float(fps),
            (frame_width, frame_height),
        )
        if not writer.isOpened():
            raise ValueError(f"Unable to open video writer for path: {output_path}")
        try:
            for frame in frame_array:
                bgr_frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
                writer.write(bgr_frame)
        finally:
            writer.release()
        return (str(output_path),)


NODE_CLASS_MAPPINGS = {
    "ApiInput": ApiInputNode,
    "ApiOutput": ApiOutputNode,
    "PathToTensor": PathToTensorNode,
    "PathToImageTensor": PathToImageTensorNode,
    "PathToVideoTensor": PathToVideoTensorNode,
    "ImageTensorToPath": ImageTensorToPathNode,
    "VideoTensorToPath": VideoTensorToPathNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ApiInput": "ComfyEndpoints API Input",
    "ApiOutput": "ComfyEndpoints API Output",
    "PathToTensor": "Path To Tensor",
    "PathToImageTensor": "Path To Image Tensor",
    "PathToVideoTensor": "Path To Video Tensor",
    "ImageTensorToPath": "Image Tensor To Path",
    "VideoTensorToPath": "Video Tensor To Path",
}


def _is_supported_output_type(type_name: str) -> bool:
    normalized = type_name.strip().lower()
    if normalized in SCALAR_CONTRACT_TYPES:
        return True
    return bool(MEDIA_TYPE_PATTERN.fullmatch(normalized))


def validate_contract_nodes(inputs: list[dict[str, Any]], outputs: list[dict[str, Any]]) -> None:
    if not inputs or not outputs:
        raise ValueError("Workflow must include at least one ApiInput and one ApiOutput node")

    seen_names: set[str] = set()
    for item in outputs:
        name = str(item.get("name", "")).strip()
        if not name:
            raise ValueError("ApiOutput name is required")
        if name in seen_names:
            raise ValueError(f"Duplicate ApiOutput name: {name}")
        seen_names.add(name)

        output_type = str(item.get("type", "")).strip()
        if not _is_supported_output_type(output_type):
            raise ValueError(f"Unsupported ApiOutput type: {output_type}")


def export_contract_from_workflow(workflow_path: Path, output_path: Path) -> None:
    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
    nodes = workflow.get("nodes", [])

    inputs: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []

    for node in nodes:
        class_type = node.get("type") or node.get("class_type")
        node_id = str(node.get("id", ""))
        widgets = node.get("widgets_values", [])

        if class_type == "ApiInput":
            name = str(widgets[0] if len(widgets) > 0 else "input")
            value_type = str(widgets[1] if len(widgets) > 1 else "string")
            required = bool(widgets[2] if len(widgets) > 2 else True)
            inputs.append(
                {
                    "name": name,
                    "type": value_type,
                    "required": required,
                    "node_id": node_id,
                }
            )

        if class_type == "ApiOutput":
            name = str(widgets[0] if len(widgets) > 0 else "output")
            value_type = str(widgets[1] if len(widgets) > 1 else "string")
            outputs.append(
                {
                    "name": name,
                    "type": value_type,
                    "node_id": node_id,
                }
            )

    validate_contract_nodes(inputs=inputs, outputs=outputs)

    payload = {
        "contract_id": f"{workflow_path.stem}-contract",
        "version": "v1",
        "inputs": inputs,
        "outputs": outputs,
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
