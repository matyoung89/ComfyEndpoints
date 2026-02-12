from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from comfy_endpoints.models import AppSpecV1


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _source_fingerprint(project_root: Path) -> str:
    include_roots = [
        project_root / "src",
        project_root / "docker",
        project_root / "comfy_plugin",
        project_root / "pyproject.toml",
    ]
    digest = hashlib.sha256()
    for root in include_roots:
        if root.is_file():
            digest.update(root.read_bytes())
            continue
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            digest.update(str(path.relative_to(project_root)).encode("utf-8"))
            digest.update(path.read_bytes())
    return digest.hexdigest()


def _git_fingerprint(project_root: Path) -> str | None:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        dirty_hash = hashlib.sha256(dirty.encode("utf-8")).hexdigest()[:12]
        return f"{head}:{dirty_hash}"
    except Exception:  # noqa: BLE001
        return None


def compute_image_fingerprint(app_spec: AppSpecV1, dockerfile_contents: str, project_root: Path) -> str:
    plugin_refs = [f"{plugin.repo}@{plugin.ref}" for plugin in app_spec.build.plugins]
    payload = {
        "comfy_version": app_spec.build.comfy_version,
        "version": app_spec.version,
        "plugins": sorted(plugin_refs),
        "dockerfile_sha256": _hash_text(dockerfile_contents),
        "git_fingerprint": _git_fingerprint(project_root),
        "source_fingerprint": _source_fingerprint(project_root),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:12]
