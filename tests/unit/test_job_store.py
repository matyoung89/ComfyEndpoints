from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from comfy_endpoints.gateway.job_store import JobStore


class JobStoreTest(unittest.TestCase):
    def test_job_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = JobStore(Path(tmp_dir) / "jobs.db")
            job_id = store.create({"prompt": "hello"})
            store.mark_running(job_id)
            store.mark_completed(job_id, {"prompt_id": "abc"})

            record = store.get(job_id)
            assert record is not None
            self.assertEqual(record.state, "completed")
            self.assertEqual(record.output_payload, {"prompt_id": "abc"})

    def test_file_create_and_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = JobStore(Path(tmp_dir) / "jobs.db")
            record = store.create_file(
                content=b"abc123",
                media_type="image/png",
                source="uploaded",
                app_id="demo",
                original_name="image.png",
            )
            loaded = store.get_file(record.file_id)
            assert loaded is not None
            self.assertEqual(loaded.file_id, record.file_id)
            self.assertEqual(loaded.media_type, "image/png")
            self.assertEqual(loaded.source, "uploaded")
            self.assertEqual(loaded.app_id, "demo")
            self.assertTrue(loaded.storage_path.exists())

    def test_file_list_with_filters_and_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = JobStore(Path(tmp_dir) / "jobs.db")
            first = store.create_file(
                content=b"first",
                media_type="image/png",
                source="uploaded",
                app_id="demo-a",
                original_name="a.png",
            )
            _second = store.create_file(
                content=b"second",
                media_type="video/mp4",
                source="generated",
                app_id="demo-b",
                original_name="b.mp4",
            )

            filtered, filtered_cursor = store.list_files(limit=10, media_type="image/png")
            self.assertEqual(len(filtered), 1)
            self.assertEqual(filtered[0].file_id, first.file_id)
            self.assertIsNone(filtered_cursor)

            page_one, next_cursor = store.list_files(limit=1)
            self.assertEqual(len(page_one), 1)
            self.assertIsNotNone(next_cursor)
            page_two, page_two_cursor = store.list_files(limit=1, cursor=next_cursor)
            self.assertEqual(len(page_two), 1)
            self.assertNotEqual(page_one[0].file_id, page_two[0].file_id)
            self.assertIsNone(page_two_cursor)

    def test_output_artifacts_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = JobStore(Path(tmp_dir) / "jobs.db")
            job_id = store.create({"prompt": "hello"})
            outputs = {
                "image": "fid_generated",
                "caption": "done",
                "score": 1.25,
                "meta": {"seed": 42},
            }
            store.write_output_artifacts(job_id, outputs)

            loaded = store.read_output_artifacts(job_id)
            self.assertEqual(loaded["image"], "fid_generated")
            self.assertEqual(loaded["caption"], "done")
            self.assertEqual(loaded["score"], 1.25)
            self.assertEqual(loaded["meta"], {"seed": 42})


if __name__ == "__main__":
    unittest.main()
