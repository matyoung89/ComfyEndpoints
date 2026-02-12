from __future__ import annotations

import os
from pathlib import Path


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    if "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None

    if value.startswith(("\"", "'")) and value.endswith(("\"", "'")) and len(value) >= 2:
        value = value[1:-1]

    return key, value


def load_env_file(path: Path, overwrite: bool = False) -> bool:
    if not path.exists() or not path.is_file():
        return False

    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(line)
        if not parsed:
            continue
        key, value = parsed
        current = os.environ.get(key)
        if overwrite or current is None or current == "":
            os.environ[key] = value

    return True


def load_local_env(preferred_files: tuple[str, ...] = (".env.local", ".env")) -> Path | None:
    explicit_path = os.getenv("COMFY_ENDPOINTS_ENV_FILE", "").strip()
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if load_env_file(path):
            return path
        return None

    search_paths: list[Path] = [Path.cwd()]
    project_root = Path(__file__).resolve().parents[3]
    if project_root not in search_paths:
        search_paths.append(project_root)

    for base in search_paths:
        for filename in preferred_files:
            candidate = (base / filename).resolve()
            if load_env_file(candidate):
                return candidate

    return None
