from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("VAULTLINE_ENV", "test")

from archive_contract import build_manifest
from app import app, get_db, init_db
from worker import manifest_from_journal, persist_archive_manifest
from worker_adapters import (
    AdapterError,
    download_r2_object,
    lock_postgres_candidates,
    upload_to_r2,
    validate_backup_artifact,
)


class FakeR2Client:
    def __init__(self, *, remote_sha256: str | None = None, corrupt_download: bool = False) -> None:
        self.remote_sha256 = remote_sha256
        self.corrupt_download = corrupt_download
        self.size = 0
        self.metadata: dict[str, str] = {}
        self.payload = b""

    def upload_file(self, filename, _bucket, _key, ExtraArgs):
        self.size = Path(filename).stat().st_size
        self.payload = Path(filename).read_bytes()
        self.metadata = dict(ExtraArgs["Metadata"])

    def head_object(self, **_kwargs):
        metadata = dict(self.metadata)
        if self.remote_sha256 is not None:
            metadata["sha256"] = self.remote_sha256
        return {"ContentLength": self.size, "ETag": '"multipart-etag"', "Metadata": metadata}

    def download_fileobj(self, _bucket, _key, writer):
        payload = self.payload
        if self.corrupt_download and payload:
            payload = bytes([payload[0] ^ 1]) + payload[1:]
        writer.write(payload)


class QueryCursor:
    def __init__(self, statements: list[tuple[str, object]]) -> None:
        self.statements = statements
        self.description = [("row_count",)]

    def execute(self, query, params=None):
        self.statements.append((query, params))

    def fetchall(self):
        return [(3,)]


class QueryConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []

    def cursor(self):
        return QueryCursor(self.statements)


class ArchiveHardeningTests(unittest.TestCase):
    def test_source_delete_journal_rebuilds_committed_portable_manifest(self):
        row = {
            "archive_id": "a" * 64,
            "namespace": "youtube",
            "engine": "PostgreSQL",
            "database_name": "youtube",
            "schema_name": "public",
            "table_name": "videos",
            "age_column": "published_at",
            "cutoff_value": "2026-09-01T00:00:00+00:00",
            "min_value": "2026-01-01T00:00:00+00:00",
            "max_value": "2026-08-31T00:00:00+00:00",
            "row_count": 2,
            "deleted_rows": 2,
            "format": "parquet",
            "columns_json": '["video_id", "published_at"]',
            "primary_keys_json": '["video_id"]',
            "data_objects_json": '[{"key":"data.parquet","verified":true}]',
            "schema_hash": "b" * 64,
            "manifest_key": "manifest.json",
            "created_at": "2026-09-01T00:00:00+00:00",
            "source_delete_committed_at": "2026-09-02T00:00:00+00:00",
            "error_message": "",
        }
        manifest = manifest_from_journal(row, {"connection_name": "YouTube Scraper"})
        self.assertEqual(manifest["status"], "committed")
        self.assertTrue(manifest["integrity"]["all_data_objects_verified"])
        self.assertEqual(manifest["primary_keys"], ["video_id"])
        self.assertIsNotNone(manifest["committed_at"])

    def test_healthz_is_public_and_checks_sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            original_database = app.config["DATABASE"]
            app.config["DATABASE"] = Path(directory) / "health-test.db"
            try:
                response = app.test_client().get("/healthz")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json(), {"ready": True, "status": "ok"})
            finally:
                app.config["DATABASE"] = original_database

    def test_r2_upload_verifies_sha256_metadata(self):
        client = FakeR2Client()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "archive.bin"
            path.write_bytes(b"archive payload")
            with patch("worker_adapters.r2_client", return_value=(client, "cold")):
                result = upload_to_r2({"id": 9}, path, "archives/data.bin", "application/octet-stream")

        expected = hashlib.sha256(b"archive payload").hexdigest()
        self.assertEqual(result["sha256"], expected)
        self.assertEqual(client.metadata["sha256"], expected)
        self.assertTrue(result["verified"])

    def test_r2_upload_rejects_checksum_mismatch_even_when_size_matches(self):
        client = FakeR2Client(remote_sha256="0" * 64)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "archive.bin"
            path.write_bytes(b"same size is not sufficient")
            with patch("worker_adapters.r2_client", return_value=(client, "cold")):
                with self.assertRaisesRegex(AdapterError, "SHA-256"):
                    upload_to_r2({}, path, "archives/data.bin", "application/octet-stream")

    def test_r2_upload_rejects_corrupt_readback_with_matching_metadata(self):
        client = FakeR2Client(corrupt_download=True)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "archive.bin"
            path.write_bytes(b"same length but different bytes")
            with patch("worker_adapters.r2_client", return_value=(client, "cold")):
                with self.assertRaisesRegex(AdapterError, "downloaded object bytes"):
                    upload_to_r2({}, path, "archives/data.bin", "application/octet-stream")

    def test_restore_download_rejects_ledger_checksum_mismatch(self):
        client = FakeR2Client()
        client.payload = b"remote backup"
        client.size = len(client.payload)
        client.metadata = {"sha256": hashlib.sha256(client.payload).hexdigest()}
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "backup.dump"
            with patch("worker_adapters.r2_client", return_value=(client, "cold")):
                with self.assertRaisesRegex(AdapterError, "ledger"):
                    download_r2_object(
                        {}, "backup.dump", destination,
                        expected_size=len(client.payload), expected_sha256="0" * 64,
                    )

    def test_mysql_backup_validation_rejects_binary_garbage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "backup.sql"
            path.write_bytes(b"not sql\x00garbage")
            with self.assertRaisesRegex(AdapterError, "recognizable SQL dump"):
                validate_backup_artifact({"engine": "MySQL"}, path)

    def test_mutating_request_requires_csrf(self):
        with tempfile.TemporaryDirectory() as directory:
            original_database = app.config["DATABASE"]
            app.config["DATABASE"] = Path(directory) / "csrf-test.db"
            try:
                client = app.test_client()
                response = client.post("/login", data={"username": "x", "password": "x"})
                self.assertEqual(response.status_code, 403)
                login_page = client.get("/login")
                self.assertEqual(login_page.status_code, 200)
                with client.session_transaction() as session:
                    token = session["csrf_token"]
                response = client.post(
                    "/login",
                    data={
                        "username": "vaultline@admin",
                        "password": "VaultLine@Admin12345",
                        "csrf_token": token,
                    },
                )
                self.assertEqual(response.status_code, 302)
            finally:
                app.config["DATABASE"] = original_database

    def test_manifest_exposes_verified_integrity(self):
        manifest = build_manifest(
            {
                "archive_namespace": "youtube",
                "connection_name": "YouTube Scraper",
                "engine": "PostgreSQL",
                "database_name": "youtube",
                "archive_format": "Parquet",
            },
            12,
            "public",
            "videos",
            status="ready",
            age_column="published_at",
            cutoff="2026-01-01T00:00:00+00:00",
            row_count=4,
            deleted_rows=0,
            data_object={"key": "data.parquet", "sha256": "a" * 64, "verified": True},
            columns=["id", "published_at"],
            primary_keys=["id"],
        )
        self.assertEqual(manifest["integrity"]["algorithm"], "sha256")
        self.assertTrue(manifest["integrity"]["all_data_objects_verified"])
        self.assertEqual(manifest["data_objects"][0]["sha256"], "a" * 64)

    def test_postgres_candidate_set_is_locked_and_primary_key_bounded(self):
        connection = QueryConnection()
        table_name, count = lock_postgres_candidates(
            connection,
            {"engine": "PostgreSQL"},
            44,
            "public",
            "videos",
            "published_at",
            "2026-01-01T00:00:00+00:00",
            "",
            ["id"],
        )
        sql = "\n".join(statement for statement, _ in connection.statements)
        self.assertIn("CREATE TEMP TABLE", sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn('SELECT "id"', sql)
        self.assertTrue(table_name.startswith('"vaultline_keys_'))
        self.assertEqual(count, 3)

    def test_postgres_archive_refuses_table_without_primary_key(self):
        with self.assertRaisesRegex(AdapterError, "primary key"):
            lock_postgres_candidates(
                QueryConnection(),
                {"engine": "PostgreSQL"},
                1,
                "public",
                "events",
                "created_at",
                "2026-01-01T00:00:00+00:00",
                "",
                [],
            )

    def test_archive_state_transitions_are_durable_and_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            original_database = app.config["DATABASE"]
            app.config["DATABASE"] = Path(directory) / "vaultline-test.db"
            try:
                with app.app_context():
                    init_db()
                    db = get_db()
                    connection_id = db.execute(
                        """INSERT INTO connections
                           (name, engine, host, port, database_name, username, password)
                           VALUES ('test', 'PostgreSQL', 'localhost', 5432, 'source', 'user', '')"""
                    ).lastrowid
                    job_id = db.execute(
                        """INSERT INTO jobs
                           (name, job_type, connection_id, cadence, run_date, run_time,
                            timezone, cron_expression, retention_days, enabled)
                           VALUES ('archive', 'archive', ?, 'Daily', '2026-01-01', '00:00',
                                   'UTC', '0 0 * * *', 7, 0)""",
                        (connection_id,),
                    ).lastrowid
                    run_id = db.execute(
                        "INSERT INTO job_runs (job_id, status) VALUES (?, 'running')", (job_id,)
                    ).lastrowid
                    db.commit()

                    manifest = build_manifest(
                        {
                            "archive_namespace": "youtube",
                            "connection_name": "test",
                            "engine": "PostgreSQL",
                            "database_name": "source",
                            "archive_format": "Parquet",
                        },
                        run_id,
                        "public",
                        "videos",
                        status="ready",
                        age_column="published_at",
                        cutoff="2026-01-01T00:00:00+00:00",
                        row_count=2,
                        deleted_rows=0,
                        data_object={"key": "data.parquet", "sha256": "b" * 64, "verified": True},
                        columns=["id"],
                        primary_keys=["id"],
                    )
                    persist_archive_manifest(run_id, job_id, manifest)
                    manifest.update(
                        status="source_delete_committed",
                        deleted_rows=2,
                        source_delete_committed_at="2026-01-08T00:00:00+00:00",
                    )
                    persist_archive_manifest(run_id, job_id, manifest)

                    saved = db.execute(
                        "SELECT status, deleted_rows, source_delete_committed_at "
                        "FROM archive_manifests WHERE archive_id = ?",
                        (manifest["archive_id"],),
                    ).fetchone()
                    self.assertEqual(saved["status"], "source_delete_committed")
                    self.assertEqual(saved["deleted_rows"], 2)
                    self.assertEqual(
                        saved["source_delete_committed_at"], "2026-01-08T00:00:00+00:00"
                    )
            finally:
                app.config["DATABASE"] = original_database


if __name__ == "__main__":
    unittest.main()
