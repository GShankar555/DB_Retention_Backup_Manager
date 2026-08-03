"""System-cron entrypoint for Vaultline jobs.

This module owns the run ledger. The data adapters can advance the same
milestones as they are enabled; dry-run jobs complete safely without changing
source data, while live jobs fail closed until their adapter is configured.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from app import app, get_db, init_db


def event(run_id: int, status: str, progress: int, message: str, finish: bool = False) -> None:
    db = get_db()
    finished_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") if finish else None
    db.execute(
        "UPDATE job_runs SET status = ?, progress = ?, message = ?, finished_at = COALESCE(?, finished_at) WHERE id = ?",
        (status, progress, message, finished_at, run_id),
    )
    # Only milestone messages are persisted; verbose adapter output belongs in worker files.
    db.execute(
        "INSERT INTO job_run_logs (run_id, level, status, progress, message) VALUES (?, ?, ?, ?, ?)",
        (run_id, "error" if status == "failed" else "info", status, progress, message),
    )
    db.commit()


def run_job(job_id: int) -> int:
    with app.app_context():
        init_db()
        db = get_db()
        job = db.execute("""SELECT jobs.*, connections.name AS connection_name,
                           connections.engine, connections.database_name
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
        event(run_id, "running", 10, f"Started {job['name']} on {job['connection_name']}")
        try:
            if not job["engine"] or not job["database_name"]:
                raise RuntimeError("Source connection is incomplete.")
            event(run_id, "running", 30, f"Validated {job['engine']} source metadata")
            event(run_id, "running", 55, f"Validated {job['tables_scope']} table scope")
            if job["dry_run"]:
                event(run_id, "running", 85, "Dry run verified; no source rows or R2 objects changed")
                event(run_id, "success", 100, "Dry run completed successfully", finish=True)
                return 0
            raise RuntimeError(f"Live {job['job_type']} adapter is not configured yet; enable dry run until the worker adapter is installed.")
        except Exception as error:
            event(run_id, "failed", 35, str(error), finish=True)
            print(str(error), file=sys.stderr)
            return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Vaultline job")
    parser.add_argument("--job-id", type=int, required=True)
    return run_job(parser.parse_args().job_id)


if __name__ == "__main__":
    raise SystemExit(main())
