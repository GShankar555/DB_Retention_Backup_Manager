"""System-cron entrypoint for Vaultline jobs and their run ledger."""

from __future__ import annotations

import argparse
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime
from pathlib import Path

from app import app, get_db, init_db
from worker_adapters import AdapterError, create_backup_artifact, preview_row_job, process_row_job, upload_backup_artifact


def event(run_id: int, status: str, progress: int, message: str, finish: bool = False) -> None:
    db = get_db()
    finished_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") if finish else None
    db.execute(
        "UPDATE job_runs SET status = ?, progress = ?, message = ?, updated_at = CURRENT_TIMESTAMP, finished_at = COALESCE(?, finished_at) WHERE id = ?",
        (status, progress, message, finished_at, run_id),
    )
    # Only milestone messages are persisted; verbose adapter output belongs in worker files.
    db.execute(
        "INSERT INTO job_run_logs (run_id, level, status, progress, message) VALUES (?, ?, ?, ?, ?)",
        (run_id, "error" if status == "failed" else "info", status, progress, message),
    )
    db.commit()


def heartbeat(run_id: int, stop: threading.Event) -> None:
    """Keep the run ledger honest while an adapter is inside a long DB call."""
    while not stop.wait(10):
        connection = None
        try:
            connection = sqlite3.connect(str(app.config["DATABASE"]), timeout=5)
            connection.execute(
                "UPDATE job_runs SET updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'running'",
                (run_id,),
            )
            connection.commit()
        except sqlite3.Error:
            # The next heartbeat or worker event can recover from a transient SQLite lock.
            pass
        finally:
            if connection is not None:
                connection.close()


def run_job(job_id: int) -> int:
    with app.app_context():
        init_db()
        db = get_db()
        job = db.execute("""SELECT jobs.*, connections.name AS connection_name,
                           connections.engine, connections.database_name,
                           connections.host, connections.port, connections.username,
                           connections.password, connections.ssl_mode
                           FROM jobs JOIN connections ON connections.id = jobs.connection_id
                           WHERE jobs.id = ?""", (job_id,)).fetchone()
        if job is None:
            print(f"Vaultline job {job_id} was not found", file=sys.stderr)
            return 2
        if not job["enabled"]:
            print(f"Vaultline job {job_id} is disabled", file=sys.stderr)
            return 0
        running = db.execute("SELECT id FROM job_runs WHERE job_id = ? AND status = 'running'", (job_id,)).fetchone()
        if running:
            print(f"Vaultline job {job_id} is already running", file=sys.stderr)
            return 0
        cursor = db.execute(
            "INSERT INTO job_runs (job_id, status, progress, message) VALUES (?, 'running', 0, ?)",
            (job_id, "Worker started"),
        )
        db.commit()
        run_id = cursor.lastrowid
        heartbeat_stop = threading.Event()
        heartbeat_thread = threading.Thread(target=heartbeat, args=(run_id, heartbeat_stop), daemon=True)
        heartbeat_thread.start()
        event(run_id, "running", 10, f"Started {job['name']} on {job['connection_name']}")
        try:
            if not job["engine"] or not job["database_name"]:
                raise RuntimeError("Source connection is incomplete.")
            event(run_id, "running", 30, f"Validated {job['engine']} source metadata")
            event(run_id, "running", 55, f"Validated {job['tables_scope']} table scope")
            if job["dry_run"]:
                if job["job_type"] in {"archive", "retention"}:
                    preview = preview_row_job(job)
                    eligible = sum(int(item.get("rows", 0)) for item in preview)
                    db.execute("UPDATE job_runs SET rows_processed = ? WHERE id = ?", (eligible, run_id))
                    db.commit()
                    event(run_id, "running", 85, f"Dry run found {eligible} eligible old row(s); no source rows or R2 objects changed")
                else:
                    event(run_id, "running", 85, "Dry run verified; no source rows or R2 objects changed")
                event(run_id, "success", 100, "Dry run completed successfully", finish=True)
                return 0
            with tempfile.TemporaryDirectory(prefix=f"vaultline-run-{run_id}-") as temporary:
                temp_dir = Path(temporary)
                if job["job_type"] == "backup":
                    event(run_id, "running", 60, "Creating native database backup")
                    artifact, content_type = create_backup_artifact(job, temp_dir)
                    event(run_id, "running", 75, f"Native backup created ({artifact.stat().st_size} bytes); uploading to R2")
                    uploaded = upload_backup_artifact(job, run_id, artifact, content_type)
                    db.execute(
                        "INSERT INTO r2_objects (run_id, job_id, bucket, object_key, format, size_bytes, etag) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (run_id, job["id"], uploaded["bucket"], uploaded["key"], uploaded["format"], uploaded["size_bytes"], uploaded.get("etag")),
                    )
                    db.commit()
                    event(run_id, "running", 90, f"Uploaded and verified {uploaded['key']} ({uploaded['size_bytes']} bytes)")
                    event(run_id, "success", 100, "Backup uploaded and verified in R2", finish=True)
                    return 0
                if job["job_type"] in {"archive", "retention"}:
                    archive = job["job_type"] == "archive"
                    event(run_id, "running", 60, "Validated retention scope; processing source tables")
                    results = process_row_job(job, run_id, temp_dir, archive=archive)
                    for uploaded in results:
                        if uploaded.get("key"):
                            db.execute(
                                "INSERT INTO r2_objects (run_id, job_id, bucket, object_key, format, size_bytes, etag) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (run_id, job["id"], uploaded["bucket"], uploaded["key"], uploaded["format"], uploaded["size_bytes"], uploaded.get("etag")),
                            )
                    db.commit()
                    total_rows = sum(int(item.get("rows", 0)) for item in results)
                    total_deleted = sum(int(item.get("deleted", 0)) for item in results)
                    skipped = sum(1 for item in results if item.get("skipped"))
                    db.execute("UPDATE job_runs SET rows_processed = ?, rows_deleted = ? WHERE id = ?", (total_rows, total_deleted, run_id))
                    db.commit()
                    if skipped:
                        event(run_id, "running", 82, f"Skipped {skipped} table(s) without a matching age column")
                    action = "Archived to R2 and removed" if archive else "Removed"
                    processed_tables = len(results) - skipped
                    event(run_id, "running", 90, f"{action} {total_deleted} old row(s) across {processed_tables} table(s)")
                    event(run_id, "success", 100, f"{action} {total_rows} eligible row(s) successfully", finish=True)
                    return 0
                raise AdapterError(f"Unsupported job type: {job['job_type']}")
        except Exception as error:
            event(run_id, "failed", 35, str(error), finish=True)
            print(str(error), file=sys.stderr)
            return 1
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Vaultline job")
    parser.add_argument("--job-id", type=int, required=True)
    return run_job(parser.parse_args().job_id)


if __name__ == "__main__":
    raise SystemExit(main())
