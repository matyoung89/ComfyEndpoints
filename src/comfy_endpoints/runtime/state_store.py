from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from comfy_endpoints.models import DeploymentRecord, DeploymentState, ProviderName


class DeploymentStore:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.deployments_file = self.state_dir / "deployments.json"
        if not self.deployments_file.exists():
            self.deployments_file.write_text("{}", encoding="utf-8")

    def _load(self) -> dict[str, dict]:
        return json.loads(self.deployments_file.read_text(encoding="utf-8"))

    def _save(self, payload: dict[str, dict]) -> None:
        self.deployments_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def put(self, record: DeploymentRecord) -> None:
        data = self._load()
        serialized = asdict(record)
        serialized["provider"] = record.provider.value
        serialized["state"] = record.state.value
        data[record.app_id] = serialized
        self._save(data)

    def get(self, app_id: str) -> DeploymentRecord | None:
        data = self._load()
        raw = data.get(app_id)
        if not raw:
            return None

        return DeploymentRecord(
            app_id=raw["app_id"],
            deployment_id=raw["deployment_id"],
            provider=ProviderName(raw["provider"]),
            state=DeploymentState(raw["state"]),
            endpoint_url=raw.get("endpoint_url"),
            api_key_ref=raw.get("api_key_ref"),
            metadata=raw.get("metadata", {}),
        )

    def delete(self, app_id: str) -> None:
        data = self._load()
        if app_id in data:
            del data[app_id]
            self._save(data)
