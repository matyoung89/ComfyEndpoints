from __future__ import annotations

from comfy_endpoints.providers.stub_provider import UnsupportedProviderAdapter


class VastProvider(UnsupportedProviderAdapter):
    def __init__(self):
        super().__init__("vast")
