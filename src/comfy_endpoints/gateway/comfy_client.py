from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class ComfyClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_text: str | None = None,
        response_json: dict | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.response_json = response_json


class ComfyClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _request_json(
        self,
        path: str,
        method: str = "GET",
        payload: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request_headers = {"content-type": "application/json", "accept": "application/json"}
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=request_headers,
            method=method,
        )

        response_text: str | None = None
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            response_text = exc.read().decode("utf-8", errors="replace")
            response_json = None
            try:
                parsed = json.loads(response_text or "{}")
                if isinstance(parsed, dict):
                    response_json = parsed
            except json.JSONDecodeError:
                response_json = None
            raise ComfyClientError(
                f"Comfy {method} {path} HTTP error: {exc.code}",
                status_code=exc.code,
                response_text=response_text,
                response_json=response_json,
            ) from exc
        except urllib.error.URLError as exc:
            raise ComfyClientError(f"Comfy {method} {path} connection error: {exc.reason}") from exc

        try:
            payload = json.loads(body or "{}")
        except json.JSONDecodeError as exc:
            raise ComfyClientError(f"Comfy {method} {path} returned invalid JSON") from exc

        if not isinstance(payload, dict):
            raise ComfyClientError(f"Comfy {method} {path} returned non-object JSON")

        return payload

    def _request_text(
        self,
        path: str,
        method: str = "GET",
        payload: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        data = payload.encode("utf-8") if payload is not None else None
        request_headers = {"accept": "*/*"}
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=request_headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            response_text = exc.read().decode("utf-8", errors="replace")
            response_json = None
            try:
                parsed = json.loads(response_text or "{}")
                if isinstance(parsed, dict):
                    response_json = parsed
            except json.JSONDecodeError:
                response_json = None
            raise ComfyClientError(
                f"Comfy {method} {path} HTTP error: {exc.code}",
                status_code=exc.code,
                response_text=response_text,
                response_json=response_json,
            ) from exc
        except urllib.error.URLError as exc:
            raise ComfyClientError(f"Comfy {method} {path} connection error: {exc.reason}") from exc

    def _request_json_with_path_fallback(
        self,
        paths: list[str],
        *,
        method: str = "GET",
        payload: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict:
        if not paths:
            raise ComfyClientError("No request paths provided")

        last_error: ComfyClientError | None = None
        for path in paths:
            try:
                return self._request_json(path, method=method, payload=payload, headers=headers)
            except ComfyClientError as exc:
                last_error = exc
                if exc.status_code != 404:
                    raise
                continue

        if last_error is not None:
            raise last_error
        raise ComfyClientError("Request failed without error details")

    def _request_text_with_path_fallback(
        self,
        paths: list[str],
        *,
        method: str = "GET",
        payload: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        if not paths:
            raise ComfyClientError("No request paths provided")

        last_error: ComfyClientError | None = None
        for path in paths:
            try:
                return self._request_text(path, method=method, payload=payload, headers=headers)
            except ComfyClientError as exc:
                last_error = exc
                if exc.status_code != 404:
                    raise
                continue

        if last_error is not None:
            raise last_error
        raise ComfyClientError("Request failed without error details")

    def queue_prompt(self, prompt_payload: dict) -> str:
        body = self._request_json("/prompt", method="POST", payload=prompt_payload)

        prompt_id = body.get("prompt_id")
        if not prompt_id:
            raise ComfyClientError("Comfy response missing prompt_id")
        return str(prompt_id)

    def get_history(self, prompt_id: str) -> dict:
        encoded_prompt_id = urllib.parse.quote(prompt_id, safe="")
        return self._request_json(f"/history/{encoded_prompt_id}", method="GET")

    def interrupt(self) -> None:
        self._request_json("/interrupt", method="POST", payload={})

    def delete_prompt_from_queue(self, prompt_id: str) -> None:
        # ComfyUI queue delete format accepts list of prompt ids.
        self._request_json("/queue", method="POST", payload={"delete": [prompt_id]})

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

    def get_external_models(self) -> object:
        query = urllib.parse.urlencode({"mode": "default"})
        payload = self._request_json_with_path_fallback(
            paths=[
                f"/api/externalmodel/getlist?{query}",
                f"/manager/externalmodel/getlist?{query}",
                f"/externalmodel/getlist?{query}",
            ],
            method="GET",
        )
        return payload

    def get_object_info(self) -> dict:
        payload = self._request_json("/object_info", method="GET")
        if not isinstance(payload, dict):
            raise ComfyClientError("Comfy object_info returned non-object JSON")
        return payload

    def get_custom_node_mappings(self) -> object:
        query = urllib.parse.urlencode({"mode": "default"})
        return self._request_json_with_path_fallback(
            paths=[
                f"/api/customnode/getmappings?{query}",
                f"/manager/customnode/getmappings?{query}",
                f"/customnode/getmappings?{query}",
            ],
            method="GET",
        )

    def get_custom_node_list(self) -> object:
        query = urllib.parse.urlencode({"mode": "default", "skip_update": "true"})
        return self._request_json_with_path_fallback(
            paths=[
                f"/api/customnode/getlist?{query}",
                f"/manager/customnode/getlist?{query}",
                f"/customnode/getlist?{query}",
            ],
            method="GET",
        )

    def install_custom_node_by_git_url(self, git_url: str) -> str:
        return self._request_text_with_path_fallback(
            [
                "/api/customnode/install/git_url",
                "/manager/customnode/install/git_url",
                "/customnode/install/git_url",
            ],
            method="POST",
            payload=git_url,
            headers={"content-type": "text/plain", "Security-Level": "weak"},
        )
