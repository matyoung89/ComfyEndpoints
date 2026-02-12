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


if __name__ == "__main__":
    unittest.main()
