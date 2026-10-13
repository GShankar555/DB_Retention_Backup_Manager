#!/usr/bin/env python3
"""Idempotently seed the YouTube scraper's seven-day R2 archive profile.

The script imports only non-secret connection/R2 identifiers. New jobs remain
disabled and dry-run protected; provide secrets to the service through the
per-connection/per-job environment variables printed at the end.

Usage:
    .venv/bin/python scripts/seed_youtube_archive.py [/path/to/Yt_scrapper/.env]
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

DEFAULT_ENV_PATH = BASE_DIR.parent / "Yt_scrapper" / ".env"
CONNECTION_NAME = "YouTube Scraper"
JOB_NAME = "YouTube Cold Archive"
ARCHIVE_NAMESPACE = "youtube"
RETENTION_DAYS = 7

# Order matters: the first column present on each table is its age column.
AGE_COLUMNS = (
    "polled_at,fetched_at,started_at,ts,day,published_at,discovered_at,"
    "detected_at,generated_at,asserted_at,observed_at,failed_at,finished_at,"
    "captured_at,indexed_at,available_at,created_at,updated_at"
)

# Runtime/configuration and work-queue tables intentionally stay hot. These are
# the durable content, version, run, event, and metric tables that have a stable
# primary key and a meaningful age column.
SELECTED_TABLES = (
    "quality_findings,embeddings,claims,measurements,item_entities,search_chunks,"
    "search_documents,dead_letters,jobs,raw_objects,source_health,video_stats,"
    "processing_runs,content_versions,ingestion_runs,event_log,budget,videos,"
    "content_items"
)


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
        print(f"YouTube scraper .env not found: {env_path}", file=sys.stderr)
        print("Pass its path explicitly: seed_youtube_archive.py /path/to/.env", file=sys.stderr)
        return 2
    env = parse_env_file(env_path)
    host = env.get("DB_HOST", "").strip()
    if not host:
        print(f"DB_HOST is missing from {env_path}", file=sys.stderr)
        return 2
    try:
        port = int(env.get("DB_PORT", "5432") or 5432)
    except ValueError:
        print(f"DB_PORT must be an integer in {env_path}", file=sys.stderr)
        return 2

    database_name = env.get("DB_NAME", "newshub_brain").strip()
    username = env.get("DB_USER", "newshub_brain").strip()
    ssl_mode = env.get("DB_SSL_MODE", "prefer").strip() or "prefer"
    r2_account_id = env.get("ARCHIVE_R2_ACCOUNT_ID", "").strip()
    r2_bucket = env.get("ARCHIVE_R2_BUCKET", "").strip()

    from app import app, get_db, init_db  # noqa: E402

    with app.app_context():
        init_db()
        db = get_db()
        connection = db.execute(
            "SELECT id FROM connections WHERE name = ?", (CONNECTION_NAME,)
        ).fetchone()
        if connection:
            connection_id = connection["id"]
            db.execute(
                """UPDATE connections SET engine = ?, host = ?, port = ?, database_name = ?,
                          username = ?, password = ?, ssl_mode = ? WHERE id = ?""",
                ("PostgreSQL", host, port, database_name, username, "", ssl_mode, connection_id),
            )
            connection_action = "Updated"
        else:
            cursor = db.execute(
                """INSERT INTO connections
                   (name, engine, host, port, database_name, username, password, ssl_mode)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (CONNECTION_NAME, "PostgreSQL", host, port, database_name, username, "", ssl_mode),
            )
            connection_id = cursor.lastrowid
            connection_action = "Created"

        existing = db.execute(
            "SELECT id, enabled, dry_run FROM jobs WHERE name = ?", (JOB_NAME,)
        ).fetchone()
        fields = {
            "job_type": "archive",
            "connection_id": connection_id,
            "r2_bucket": r2_bucket,
            "r2_account_id": r2_account_id,
            "r2_access_key": "",
            "r2_secret_key": "",
            "cadence": "Weekly",
            "run_date": "2026-01-01",
            "run_time": "01:00",
            "timezone": "Asia/Kolkata",
            "cron_expression": "0 1 * * 0",
            "retention_days": RETENTION_DAYS,
            "retention_column": AGE_COLUMNS,
            "tables_scope": "selected",
            "selected_tables": SELECTED_TABLES,
            "archive_format": "Parquet",
            "archive_namespace": ARCHIVE_NAMESPACE,
        }
        if existing:
            job_id = existing["id"]
            assignments = ", ".join(f"{key} = ?" for key in fields)
            db.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", (*fields.values(), job_id))
            job_action = "Updated"
            dry_run, enabled = existing["dry_run"], existing["enabled"]
        else:
            fields.update(name=JOB_NAME, dry_run=1, enabled=0)
            columns = ", ".join(fields)
            placeholders = ", ".join("?" for _ in fields)
            cursor = db.execute(
                f"INSERT INTO jobs ({columns}) VALUES ({placeholders})", tuple(fields.values())
            )
            job_id = cursor.lastrowid
            job_action = "Created"
            dry_run, enabled = 1, 0
        db.commit()

    print(
        f"{connection_action} connection '{CONNECTION_NAME}' (id={connection_id}) "
        f"-> {host}:{port}/{database_name}"
    )
    print(
        f"{job_action} job '{JOB_NAME}' (id={job_id}), retention={RETENTION_DAYS} days, "
        f"dry_run={bool(dry_run)}, enabled={bool(enabled)}"
    )
    print(f"Set VAULTLINE_CONNECTION_{connection_id}_PASSWORD in the Vaultline service environment.")
    print(
        f"Set VAULTLINE_JOB_{job_id}_R2_ACCESS_KEY and "
        f"VAULTLINE_JOB_{job_id}_R2_SECRET_KEY there as well."
    )
    print("Review the scope, run a dry run, verify R2, then explicitly enable live execution.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
