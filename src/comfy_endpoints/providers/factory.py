from __future__ import annotations

from comfy_endpoints.models import ProviderName
from comfy_endpoints.providers.aws_provider import AwsProvider
from comfy_endpoints.providers.base import CloudProviderAdapter
from comfy_endpoints.providers.gcp_provider import GcpProvider
from comfy_endpoints.providers.lambda_provider import LambdaProvider
from comfy_endpoints.providers.runpod_provider import RunpodProvider
from comfy_endpoints.providers.vast_provider import VastProvider


def build_provider(provider_name: ProviderName) -> CloudProviderAdapter:
    if provider_name == ProviderName.RUNPOD:
        return RunpodProvider()

    if provider_name == ProviderName.VAST:
        return VastProvider()

    if provider_name == ProviderName.LAMBDA:
        return LambdaProvider()

    if provider_name == ProviderName.AWS:
        return AwsProvider()

    if provider_name == ProviderName.GCP:
        return GcpProvider()

    raise RuntimeError(f"Unsupported provider: {provider_name.value}")
