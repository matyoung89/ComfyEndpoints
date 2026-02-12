from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.deploy.cache_manager import CacheManager


class CacheManagerTest(unittest.TestCase):
    def test_reconcile_moves_large_file_to_cache_and_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            watch_dir = root / "models"
            cache_root = root / "cache"
            watch_dir.mkdir(parents=True)

            large_file = watch_dir / "checkpoint.safetensors"
            large_file.write_bytes(b"x" * 2048)

            manager = CacheManager(
                cache_root=cache_root,
                watch_paths=[watch_dir],
                min_file_size_mb=0,
            )

            manifest = manager.reconcile()
            self.assertEqual(len(manifest.keys()), 1)
            self.assertTrue(large_file.is_symlink())
            self.assertTrue(large_file.resolve().exists())


if __name__ == "__main__":
    unittest.main()
