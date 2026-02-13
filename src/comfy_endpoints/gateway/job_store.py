from __future__ import annotations

import json
import mimetypes
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path


@dataclass(slots=True)
class JobRecord:
    job_id: str
    state: str
    input_payload: dict
    output_payload: dict | None
    error: str | None


@dataclass(slots=True)
class FileRecord:
    file_id: str
    media_type: str
    size_bytes: int
    sha256_hex: str
    source: str
    app_id: str | None
    original_name: str | None
    created_at: str
    storage_path: Path
    cursor_id: int


class JobStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_storage_root = self.db_path.parent / "files"
        self.file_storage_root.mkdir(parents=True, exist_ok=True)
        self.artifact_storage_root = self.db_path.parent / "artifacts"
        self.artifact_storage_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        self._init_schema()

    def _execute(self, sql: str, params: tuple[object, ...] = ()) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.execute(sql, params)

    def _fetchone(self, sql: str, params: tuple[object, ...] = ()) -> tuple | None:
        with self._lock:
            return self.conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple]:
        with self._lock:
            return self.conn.execute(sql, params).fetchall()

    def _commit(self) -> None:
        with self._lock:
            self.conn.commit()

    def _init_schema(self) -> None:
        self._execute(
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
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL UNIQUE,
                storage_rel_path TEXT NOT NULL,
                media_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                sha256_hex TEXT NOT NULL,
                source TEXT NOT NULL,
                app_id TEXT,
                original_name TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        self._commit()

    def create(self, payload: dict) -> str:
        job_id = uuid.uuid4().hex
        self._execute(
            "INSERT INTO jobs (job_id, state, input_payload, output_payload, error) VALUES (?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps(payload), None, None),
        )
        self._commit()
        return job_id

    def mark_running(self, job_id: str) -> None:
        self._execute("UPDATE jobs SET state = ? WHERE job_id = ?", ("running", job_id))
        self._commit()

    def mark_completed(self, job_id: str, output_payload: dict) -> None:
        self._execute(
            "UPDATE jobs SET state = ?, output_payload = ? WHERE job_id = ?",
            ("completed", json.dumps(output_payload), job_id),
        )
        self._commit()

    def mark_failed(self, job_id: str, error: str) -> None:
        self._execute(
            "UPDATE jobs SET state = ?, error = ? WHERE job_id = ?",
            ("failed", error, job_id),
        )
        self._commit()

    @staticmethod
    def _sanitize_artifact_name(output_name: str) -> str:
        sanitized = Path(output_name).name.strip()
        if not sanitized:
            raise ValueError("Output name is empty")
        if sanitized in {".", ".."}:
            raise ValueError("Invalid output name")
        return sanitized

    def _artifact_path(self, job_id: str, output_name: str) -> Path:
        safe_name = self._sanitize_artifact_name(output_name)
        return self.artifact_storage_root / job_id / safe_name

    def write_output_artifacts(self, job_id: str, outputs: dict[str, object]) -> None:
        target_dir = self.artifact_storage_root / job_id
        target_dir.mkdir(parents=True, exist_ok=True)
        for output_name, value in outputs.items():
            artifact_path = self._artifact_path(job_id, output_name)
            if isinstance(value, str):
                artifact_path.write_text(value, encoding="utf-8")
                continue
            artifact_path.write_text(json.dumps(value), encoding="utf-8")

    def read_output_artifacts(self, job_id: str) -> dict[str, object]:
        target_dir = self.artifact_storage_root / job_id
        if not target_dir.exists() or not target_dir.is_dir():
            return {}

        resolved: dict[str, object] = {}
        for artifact_path in sorted(target_dir.iterdir(), key=lambda item: item.name):
            if not artifact_path.is_file():
                continue
            raw_value = artifact_path.read_text(encoding="utf-8")
            try:
                resolved[artifact_path.name] = json.loads(raw_value)
            except json.JSONDecodeError:
                resolved[artifact_path.name] = raw_value
        return resolved

    def get(self, job_id: str) -> JobRecord | None:
        row = self._fetchone(
            "SELECT job_id, state, input_payload, output_payload, error FROM jobs WHERE job_id = ?",
            (job_id,),
        )
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

    def _sanitize_original_name(self, original_name: str | None) -> str | None:
        if not original_name:
            return None
        normalized = Path(original_name).name.strip()
        if not normalized:
            return None
        return normalized

    def create_file(
        self,
        content: bytes,
        media_type: str | None,
        source: str,
        app_id: str | None,
        original_name: str | None = None,
    ) -> FileRecord:
        if not content:
            raise ValueError("File payload is empty")

        file_id = f"fid_{uuid.uuid4().hex}"
        normalized_name = self._sanitize_original_name(original_name)
        content_media_type = (media_type or "").strip() or "application/octet-stream"
        suffix = ""
        if normalized_name and "." in normalized_name:
            suffix = Path(normalized_name).suffix
        elif content_media_type:
            suffix = mimetypes.guess_extension(content_media_type) or ""

        relative_name = f"{file_id}{suffix}"
        target_path = self.file_storage_root / relative_name
        target_path.write_bytes(content)
        digest = sha256(content).hexdigest()
        created_at = datetime.now(UTC).isoformat()

        self._execute(
            """
            INSERT INTO files (
                file_id, storage_rel_path, media_type, size_bytes, sha256_hex, source, app_id, original_name, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                relative_name,
                content_media_type,
                len(content),
                digest,
                source,
                app_id,
                normalized_name,
                created_at,
            ),
        )
        row_id = int(self._fetchone("SELECT last_insert_rowid()")[0])
        self._commit()
        return FileRecord(
            file_id=file_id,
            media_type=content_media_type,
            size_bytes=len(content),
            sha256_hex=digest,
            source=source,
            app_id=app_id,
            original_name=normalized_name,
            created_at=created_at,
            storage_path=target_path,
            cursor_id=row_id,
        )

    def register_existing_file(
        self,
        storage_path: Path,
        media_type: str | None,
        source: str,
        app_id: str | None,
        original_name: str | None = None,
    ) -> FileRecord:
        if not storage_path.exists() or not storage_path.is_file():
            raise ValueError(f"File not found: {storage_path}")

        content = storage_path.read_bytes()
        return self.create_file(
            content=content,
            media_type=media_type,
            source=source,
            app_id=app_id,
            original_name=original_name or storage_path.name,
        )

    def _row_to_file_record(self, row: tuple) -> FileRecord:
        return FileRecord(
            file_id=row[1],
            media_type=row[3],
            size_bytes=int(row[4]),
            sha256_hex=row[5],
            source=row[6],
            app_id=row[7],
            original_name=row[8],
            created_at=row[9],
            storage_path=self.file_storage_root / row[2],
            cursor_id=int(row[0]),
        )

    def get_file(self, file_id: str) -> FileRecord | None:
        row = self._fetchone(
            """
            SELECT id, file_id, storage_rel_path, media_type, size_bytes, sha256_hex, source, app_id, original_name, created_at
            FROM files
            WHERE file_id = ?
            """,
            (file_id,),
        )
        if not row:
            return None
        return self._row_to_file_record(row)

    def list_files(
        self,
        limit: int = 50,
        cursor: str | None = None,
        media_type: str | None = None,
        source: str | None = None,
        app_id: str | None = None,
    ) -> tuple[list[FileRecord], str | None]:
        effective_limit = max(1, min(limit, 200))
        query = (
            "SELECT id, file_id, storage_rel_path, media_type, size_bytes, sha256_hex, source, app_id, original_name, created_at "
            "FROM files"
        )
        conditions: list[str] = []
        params: list[object] = []

        if cursor:
            conditions.append("id < ?")
            params.append(int(cursor))
        if media_type:
            conditions.append("media_type = ?")
            params.append(media_type)
        if source:
            conditions.append("source = ?")
            params.append(source)
        if app_id:
            conditions.append("app_id = ?")
            params.append(app_id)

        if conditions:
            query = f"{query} WHERE {' AND '.join(conditions)}"
        query = f"{query} ORDER BY id DESC LIMIT ?"
        params.append(effective_limit + 1)

        rows = self._fetchall(query, tuple(params))
        has_more = len(rows) > effective_limit
        visible_rows = rows[:effective_limit]
        records = [self._row_to_file_record(row) for row in visible_rows]
        next_cursor = str(records[-1].cursor_id) if has_more and records else None
        return records, next_cursor
