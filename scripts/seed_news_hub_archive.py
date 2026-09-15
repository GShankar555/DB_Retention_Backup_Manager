#!/usr/bin/env python3
"""Idempotently create/update the News Hub connection and cold-archive job in
Vaultline's SQLite store, from News Hub's own .env file.

Reuses Django's DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD convention (the
same one News Hub and Newshub Brain already share) instead of duplicating
credentials into Vaultline-specific environment variables. Run this once
locally against a dev .env, and again on the Linode VM against the
production .env after Vaultline is deployed there.

Usage:
    .venv/bin/python scripts/seed_news_hub_archive.py [/path/to/news-hub/backend/.env]

The job is created disabled and in dry-run mode. Review it in the Vaultline
UI, fill in the R2 credentials (Settings or the job form), run a dry run,
then enable it and turn dry-run off per deploy/archive/news-hub.md in the
News Hub repo.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

DEFAULT_ENV_PATH = BASE_DIR.parent / "news-hub" / "backend" / ".env"

CONNECTION_NAME = "News Hub"
JOB_NAME = "News Hub Cold Archive"

AGE_COLUMNS = "published_at, fetched_at, started_at, timestamp, created_at"
SELECTED_TABLES = (
    "newsapi_articleversion,newsapi_liveupdate,newsapi_article,"
    "newsapi_scraperun,newsapi_syncbatch"
)
ARCHIVE_NAMESPACE = "news-hub"
RETENTION_DAYS = 14


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def main() -> int:
    env_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_ENV_PATH
    if not env_path.is_file():
        print(f"News Hub .env not found: {env_path}", file=sys.stderr)
        print("Pass its path explicitly: seed_news_hub_archive.py /path/to/.env", file=sys.stderr)
        return 2
    env = parse_env_file(env_path)

    host = env.get("DB_HOST", "").strip()
    if not host:
        print(f"DB_HOST is missing from {env_path}", file=sys.stderr)
        return 2
    port = int(env.get("DB_PORT", "5432") or 5432)
    database_name = env.get("DB_NAME", "newshub")
    username = env.get("DB_USER", "newshub")
    password = env.get("DB_PASSWORD", "")

    r2_account_id = env.get("ARCHIVE_R2_ACCOUNT_ID", "")
    r2_bucket = env.get("ARCHIVE_R2_BUCKET", "")
    r2_access_key = env.get("ARCHIVE_R2_ACCESS_KEY", "")
    r2_secret_key = env.get("ARCHIVE_R2_SECRET_KEY", "")

    from app import app, get_db, init_db  # noqa: E402  (needs sys.path set first)

    with app.app_context():
        init_db()
        db = get_db()

        existing_connection = db.execute(
            "SELECT id FROM connections WHERE name = ?", (CONNECTION_NAME,)
        ).fetchone()
        if existing_connection:
            connection_id = existing_connection["id"]
            db.execute(
                """UPDATE connections SET engine = ?, host = ?, port = ?, database_name = ?,
                       username = ?, password = ?, ssl_mode = ? WHERE id = ?""",
                ("PostgreSQL", host, port, database_name, username, password, "prefer", connection_id),
            )
            action_connection = "Updated"
        else:
            cursor = db.execute(
                """INSERT INTO connections (name, engine, host, port, database_name, username, password, ssl_mode)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (CONNECTION_NAME, "PostgreSQL", host, port, database_name, username, password, "prefer"),
            )
            connection_id = cursor.lastrowid
            action_connection = "Created"
        db.commit()

        existing_job = db.execute(
            "SELECT id, enabled, dry_run FROM jobs WHERE name = ?", (JOB_NAME,)
        ).fetchone()
        job_fields = {
            "job_type": "archive",
            "connection_id": connection_id,
            "r2_bucket": r2_bucket,
            "r2_account_id": r2_account_id,
            "r2_access_key": r2_access_key,
            "r2_secret_key": r2_secret_key,
            "cadence": "Daily",
            "run_date": "2026-01-01",
            "run_time": "00:30",
            "timezone": "Asia/Kolkata",
            "cron_expression": "30 0 * * *",
            "retention_days": RETENTION_DAYS,
            "retention_column": AGE_COLUMNS,
            "tables_scope": "selected",
            "selected_tables": SELECTED_TABLES,
            "archive_format": "Parquet",
            "archive_namespace": ARCHIVE_NAMESPACE,
        }
        if existing_job:
            job_id = existing_job["id"]
            assignments = ", ".join(f"{key} = ?" for key in job_fields)
            db.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", (*job_fields.values(), job_id))
            action_job = "Updated"
            dry_run, enabled = existing_job["dry_run"], existing_job["enabled"]
        else:
            job_fields["name"] = JOB_NAME
            job_fields["dry_run"] = 1
            job_fields["enabled"] = 0
            columns = ", ".join(job_fields.keys())
            placeholders = ", ".join("?" for _ in job_fields)
            cursor = db.execute(f"INSERT INTO jobs ({columns}) VALUES ({placeholders})", tuple(job_fields.values()))
            job_id = cursor.lastrowid
            action_job = "Created"
            dry_run, enabled = 1, 0
        db.commit()

    print(f"{action_connection} connection '{CONNECTION_NAME}' (id={connection_id}) -> {host}:{port}/{database_name}")
    print(f"{action_job} job '{JOB_NAME}' (id={job_id}), dry_run={bool(dry_run)}, enabled={bool(enabled)}")
    if not (r2_account_id and r2_bucket and r2_access_key and r2_secret_key):
        print(f"R2 credentials are not set in {env_path} (ARCHIVE_R2_*) - job left with blank R2 fields.")
    print("Review in the Vaultline UI, run a dry run, then enable the job and turn dry-run off once R2 is verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
