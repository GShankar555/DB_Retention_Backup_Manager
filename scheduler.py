"""Cron's UTC-minute entrypoint for timezone-aware Vaultline schedules."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from app import BASE_DIR, LOG_DIR, WORKER_SCRIPT, app, get_db


def due_job_ids(now_utc: datetime) -> list[int]:
    due: list[int] = []
    with app.app_context():
        jobs = get_db().execute(
            "SELECT id, cron_expression, timezone, cadence, run_date FROM jobs WHERE enabled = 1 ORDER BY id"
        ).fetchall()
        for job in jobs:
            try:
                local = now_utc.astimezone(ZoneInfo(job["timezone"] or "UTC"))
                local = local.replace(second=0, microsecond=0)
                if not croniter.match(job["cron_expression"], local, precision_in_seconds=1):
                    continue
                if job["cadence"] == "Biweekly":
                    anchor = datetime.strptime(job["run_date"], "%Y-%m-%d").date()
                    if local.date() < anchor or (local.date() - anchor).days % 14:
                        continue
                due.append(job["id"])
            except (ValueError, KeyError, TypeError, ZoneInfoNotFoundError) as error:
                print(f"Vaultline job {job['id']} has an invalid schedule: {error}", file=sys.stderr)
    return due


def main() -> int:
    now_utc = datetime.now(timezone.utc)
    job_ids = due_job_ids(now_utc)
    for job_id in job_ids:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with (LOG_DIR / f"job-{job_id}.log").open("ab") as log:
            environment = os.environ.copy()
            environment["VAULTLINE_SCHEDULED"] = "1"
            subprocess.Popen(
                [sys.executable, str(WORKER_SCRIPT), "--job-id", str(job_id)],
                cwd=str(BASE_DIR), env=environment, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        print(f"Vaultline scheduled job {job_id} at {now_utc:%Y-%m-%d %H:%M} UTC")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
