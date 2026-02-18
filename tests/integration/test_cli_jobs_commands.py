from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from comfy_endpoints.cli.main import main
from comfy_endpoints.models import DeploymentRecord, DeploymentState, ProviderName
from comfy_endpoints.runtime.state_store import DeploymentStore


class CliJobsCommandsIntegrationTest(unittest.TestCase):
    def _state_dir_with_demo(self, root: Path) -> Path:
        state_dir = root / ".comfy_endpoints"
        store = DeploymentStore(state_dir=state_dir)
        store.put(
            DeploymentRecord(
                app_id="demo",
                deployment_id="dep-demo",
                provider=ProviderName.RUNPOD,
                state=DeploymentState.READY,
                endpoint_url="https://demo.example.com",
                api_key_ref="secret://demo/api_key",
                metadata={},
            )
        )
        return state_dir

    def test_jobs_get_returns_job_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = self._state_dir_with_demo(root)
            with mock.patch(
                "comfy_endpoints.cli.main._request_json",
                return_value={"job_id": "job-123", "state": "running"},
            ) as mocked_request:
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    self.assertEqual(
                        main(
                            [
                                "--state-dir",
                                str(state_dir),
                                "jobs",
                                "get",
                                "demo",
                                "job-123",
                            ]
                        ),
                        0,
                    )
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["job_id"], "job-123")
                self.assertEqual(payload["state"], "running")
                mocked_request.assert_called_once_with(
                    endpoint_url="https://demo.example.com",
                    app_id="demo",
                    path="/jobs/job-123",
                )

    def test_complete_suggests_jobs_subcommand_and_app_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = self._state_dir_with_demo(root)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(
                    main(
                        [
                            "--state-dir",
                            str(state_dir),
                            "_complete",
                            "--index",
                            "2",
                            "--words",
                            "comfy-endpoints",
                            "jobs",
                            "",
                        ]
                    ),
                    0,
                )
            suggestions = set(stdout.getvalue().splitlines())
            self.assertIn("get", suggestions)
            self.assertIn("cancel", suggestions)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(
                    main(
                        [
                            "--state-dir",
                            str(state_dir),
                            "_complete",
                            "--index",
                            "3",
                            "--words",
                            "comfy-endpoints",
                            "jobs",
                            "get",
                            "",
                        ]
                    ),
                    0,
                )
            self.assertIn("demo", set(stdout.getvalue().splitlines()))

    def test_jobs_cancel_posts_cancel_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_dir = self._state_dir_with_demo(root)
            with mock.patch(
                "comfy_endpoints.cli.main._request_json_post",
                return_value={"job_id": "job-777", "state": "canceling", "cancel_requested": True},
            ) as mocked_post:
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    self.assertEqual(
                        main(
                            [
                                "--state-dir",
                                str(state_dir),
                                "jobs",
                                "cancel",
                                "demo",
                                "job-777",
                            ]
                        ),
                        0,
                    )
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["state"], "canceling")
                mocked_post.assert_called_once_with(
                    endpoint_url="https://demo.example.com",
                    app_id="demo",
                    path="/jobs/job-777/cancel",
                    payload={},
                )


if __name__ == "__main__":
    unittest.main()
