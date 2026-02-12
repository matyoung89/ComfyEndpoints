from __future__ import annotations

import json
import urllib.error
import urllib.request


class ComfyClientError(RuntimeError):
    pass


class ComfyClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def queue_prompt(self, prompt_payload: dict) -> str:
        request = urllib.request.Request(
            f"{self.base_url}/prompt",
            data=json.dumps(prompt_payload).encode("utf-8"),
            headers={"content-type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise ComfyClientError(f"Comfy queue HTTP error: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ComfyClientError(f"Comfy queue connection error: {exc.reason}") from exc

        prompt_id = body.get("prompt_id")
        if not prompt_id:
            raise ComfyClientError("Comfy response missing prompt_id")
        return str(prompt_id)
