from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ManagedFile:
    sha256: str
    source: str
    cache_path: str
    linked_paths: list[str]
    last_seen: float


class CacheManager:
    def __init__(
        self,
        cache_root: Path,
        watch_paths: list[Path],
        min_file_size_mb: int = 100,
        manifest_name: str = "manifest.json",
    ):
        self.cache_root = cache_root
        self.watch_paths = watch_paths
        self.min_file_size_bytes = min_file_size_mb * 1024 * 1024
        self.cache_files_dir = self.cache_root / "files"
        self.manifest_file = self.cache_root / manifest_name

        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.cache_files_dir.mkdir(parents=True, exist_ok=True)

        if not self.manifest_file.exists():
            self.manifest_file.write_text("{}", encoding="utf-8")

    def _load_manifest(self) -> dict[str, dict]:
        return json.loads(self.manifest_file.read_text(encoding="utf-8"))

    def _save_manifest(self, payload: dict[str, dict]) -> None:
        self.manifest_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(2 * 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _cache_destination(self, digest: str, original_name: str) -> Path:
        return self.cache_files_dir / f"{digest}_{original_name}"

    def _replace_with_symlink(self, original: Path, cache_target: Path) -> None:
        temp_path = original.with_suffix(original.suffix + ".tmp_move")
        if temp_path.exists():
            if temp_path.is_dir():
                shutil.rmtree(temp_path)
            else:
                temp_path.unlink()

        shutil.move(str(original), str(temp_path))
        os.replace(str(temp_path), str(cache_target)) if False else None

    def manage_file(self, source_path: Path) -> ManagedFile:
        if source_path.is_symlink():
            target = source_path.resolve(strict=False)
            digest = self._sha256(target) if target.exists() and target.is_file() else "symlink"
            return ManagedFile(
                sha256=digest,
                source=str(source_path),
                cache_path=str(target),
                linked_paths=[str(source_path)],
                last_seen=time.time(),
            )

        if not source_path.exists() or not source_path.is_file():
            raise FileNotFoundError(source_path)

        if source_path.stat().st_size < self.min_file_size_bytes:
            raise ValueError(f"File below threshold: {source_path}")

        digest = self._sha256(source_path)
        cache_target = self._cache_destination(digest, source_path.name)
        if not cache_target.exists():
            shutil.move(str(source_path), str(cache_target))

        if source_path.exists():
            source_path.unlink()
        source_path.symlink_to(cache_target)

        return ManagedFile(
            sha256=digest,
            source=str(source_path),
            cache_path=str(cache_target),
            linked_paths=[str(source_path)],
            last_seen=time.time(),
        )

    def reconcile(self) -> dict[str, dict]:
        manifest = self._load_manifest()

        for watch_path in self.watch_paths:
            if not watch_path.exists():
                continue

            for file_path in watch_path.rglob("*"):
                if not file_path.is_file():
                    continue
                if file_path.stat().st_size < self.min_file_size_bytes:
                    continue

                managed = self.manage_file(file_path)
                manifest[managed.sha256] = {
                    "source": managed.source,
                    "cache_path": managed.cache_path,
                    "linked_paths": managed.linked_paths,
                    "last_seen": managed.last_seen,
                }

        self._save_manifest(manifest)
        return manifest
