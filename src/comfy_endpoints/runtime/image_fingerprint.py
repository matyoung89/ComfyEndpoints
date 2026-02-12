from __future__ import annotations

import hashlib
import json

from comfy_endpoints.models import AppSpecV1


def compute_image_fingerprint(app_spec: AppSpecV1, dockerfile_contents: str) -> str:
    plugin_refs = [f"{plugin.repo}@{plugin.ref}" for plugin in app_spec.build.plugins]
    payload = {
        "comfy_version": app_spec.build.comfy_version,
        "version": app_spec.version,
        "plugins": sorted(plugin_refs),
        "dockerfile_sha256": hashlib.sha256(dockerfile_contents.encode("utf-8")).hexdigest(),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:12]
