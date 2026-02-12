from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class JobRecord:
    job_id: str
    state: str
    input_payload: dict
    output_payload: dict | None
    error: str | None


class JobStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                input_payload TEXT NOT NULL,
                output_payload TEXT,
                error TEXT
            )
            """
        )
        self.conn.commit()

    def create(self, payload: dict) -> str:
        job_id = uuid.uuid4().hex
        self.conn.execute(
            "INSERT INTO jobs (job_id, state, input_payload, output_payload, error) VALUES (?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps(payload), None, None),
        )
        self.conn.commit()
        return job_id

    def mark_running(self, job_id: str) -> None:
        self.conn.execute("UPDATE jobs SET state = ? WHERE job_id = ?", ("running", job_id))
        self.conn.commit()

    def mark_completed(self, job_id: str, output_payload: dict) -> None:
        self.conn.execute(
            "UPDATE jobs SET state = ?, output_payload = ? WHERE job_id = ?",
            ("completed", json.dumps(output_payload), job_id),
        )
        self.conn.commit()

    def mark_failed(self, job_id: str, error: str) -> None:
        self.conn.execute(
            "UPDATE jobs SET state = ?, error = ? WHERE job_id = ?",
            ("failed", error, job_id),
        )
        self.conn.commit()

    def get(self, job_id: str) -> JobRecord | None:
        row = self.conn.execute(
            "SELECT job_id, state, input_payload, output_payload, error FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if not row:
            return None

        output_payload = json.loads(row[3]) if row[3] else None
        return JobRecord(
            job_id=row[0],
            state=row[1],
            input_payload=json.loads(row[2]),
            output_payload=output_payload,
            error=row[4],
        )
