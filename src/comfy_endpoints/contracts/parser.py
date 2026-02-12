from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    pass


def load_structured_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    content = path.read_text(encoding="utf-8")

    if suffix == ".json":
        return json.loads(content)

    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ContractError(
                "YAML support requires PyYAML. Install with `pip install pyyaml` or use JSON."
            ) from exc

        data = yaml.safe_load(content)
        if not isinstance(data, dict):
            raise ContractError(f"Expected mapping document in {path}")
        return data

    raise ContractError(f"Unsupported file extension for {path}")
