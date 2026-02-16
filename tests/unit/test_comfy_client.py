from __future__ import annotations

import json
import unittest
from unittest import mock

from comfy_endpoints.gateway.comfy_client import ComfyClient, ComfyClientError


class _MockResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        _ = (exc_type, exc, tb)
        return False


class ComfyClientTest(unittest.TestCase):
    def test_get_history_returns_payload(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch(
            "urllib.request.urlopen",
            return_value=_MockResponse(json.dumps({"abc": {"outputs": {}}}).encode("utf-8")),
        ):
            payload = client.get_history("abc")
        self.assertIn("abc", payload)

    def test_get_view_media_returns_bytes(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch("urllib.request.urlopen", return_value=_MockResponse(b"media")):
            payload = client.get_view_media("file.png", "", "output")
        self.assertEqual(payload, b"media")

    def test_invalid_json_raises(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch("urllib.request.urlopen", return_value=_MockResponse(b"not-json")):
            with self.assertRaises(ComfyClientError):
                client.get_history("abc")

    def test_get_object_info_returns_payload(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch(
            "urllib.request.urlopen",
            return_value=_MockResponse(json.dumps({"UNETLoader": {"input": {}}}).encode("utf-8")),
        ):
            payload = client.get_object_info()
        self.assertIn("UNETLoader", payload)

    def test_get_external_models_uses_mode_query(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch(
            "urllib.request.urlopen",
            return_value=_MockResponse(json.dumps({"models": []}).encode("utf-8")),
        ) as mocked_urlopen:
            _ = client.get_external_models()

        request = mocked_urlopen.call_args.args[0]
        self.assertIn("/externalmodel/getlist?mode=default", request.full_url)

    def test_get_custom_node_mappings_uses_mode_query(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch(
            "urllib.request.urlopen",
            return_value=_MockResponse(json.dumps({"x": []}).encode("utf-8")),
        ) as mocked_urlopen:
            _ = client.get_custom_node_mappings()
        request = mocked_urlopen.call_args.args[0]
        self.assertIn("/customnode/getmappings?mode=default", request.full_url)

    def test_get_custom_node_list_uses_mode_query(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch(
            "urllib.request.urlopen",
            return_value=_MockResponse(json.dumps({"node_packs": []}).encode("utf-8")),
        ) as mocked_urlopen:
            _ = client.get_custom_node_list()
        request = mocked_urlopen.call_args.args[0]
        self.assertIn("/customnode/getlist?mode=default&skip_update=true", request.full_url)

    def test_install_custom_node_by_git_url_posts_plain_text(self) -> None:
        client = ComfyClient("http://127.0.0.1:8188")
        with mock.patch("urllib.request.urlopen", return_value=_MockResponse(b"ok")) as mocked_urlopen:
            _ = client.install_custom_node_by_git_url("https://github.com/example/custom-node")
        request = mocked_urlopen.call_args.args[0]
        self.assertIn("/customnode/install/git_url", request.full_url)
        self.assertEqual(request.data, b"https://github.com/example/custom-node")


if __name__ == "__main__":
    unittest.main()
