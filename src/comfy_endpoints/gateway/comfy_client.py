from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class ComfyClientError(RuntimeError):
    pass


class ComfyClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _request_json(self, path: str, method: str = "GET", payload: dict | None = None) -> dict:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers={"content-type": "application/json", "accept": "application/json"},
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise ComfyClientError(f"Comfy {method} {path} HTTP error: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ComfyClientError(f"Comfy {method} {path} connection error: {exc.reason}") from exc

        try:
            payload = json.loads(body or "{}")
        except json.JSONDecodeError as exc:
            raise ComfyClientError(f"Comfy {method} {path} returned invalid JSON") from exc

        if not isinstance(payload, dict):
            raise ComfyClientError(f"Comfy {method} {path} returned non-object JSON")

        return payload

    def queue_prompt(self, prompt_payload: dict) -> str:
        body = self._request_json("/prompt", method="POST", payload=prompt_payload)

        prompt_id = body.get("prompt_id")
        if not prompt_id:
            raise ComfyClientError("Comfy response missing prompt_id")
        return str(prompt_id)

    def get_history(self, prompt_id: str) -> dict:
        encoded_prompt_id = urllib.parse.quote(prompt_id, safe="")
        return self._request_json(f"/history/{encoded_prompt_id}", method="GET")

    def get_view_media(self, filename: str, subfolder: str, media_type: str) -> bytes:
        query = urllib.parse.urlencode(
            {
                "filename": filename,
                "subfolder": subfolder,
                "type": media_type,
            }
        )
        request = urllib.request.Request(
            f"{self.base_url}/view?{query}",
            headers={"accept": "*/*"},
            method="GET",
        )

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            raise ComfyClientError(f"Comfy view HTTP error: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ComfyClientError(f"Comfy view connection error: {exc.reason}") from exc
