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

    raise ContractError(f"Unsupported file extension for {path}. Only .json is supported.")
