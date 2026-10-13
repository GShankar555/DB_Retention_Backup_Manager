"""Download, verify, and optionally restore one full-backup object from R2.

Validation-only mode is the default and never connects to a restore target.
An actual PostgreSQL restore requires an operator-created empty database plus
the explicit ``--confirm RESTORE`` guard. This command never drops databases.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from app import app, get_db, init_db
from worker_adapters import (
    AdapterError,
    download_r2_object,
    engine_name,
    restore_postgresql_backup,
    validate_backup_artifact,
)


def load_backup_object(object_id: int):
    row = get_db().execute(
        """SELECT r2_objects.*, jobs.job_type, jobs.r2_bucket, jobs.r2_account_id,
                  jobs.r2_access_key, jobs.r2_secret_key, jobs.archive_namespace,
                  connections.name AS connection_name, connections.engine,
                  connections.database_name, connections.host, connections.port,
                  connections.username, connections.password, connections.ssl_mode
             FROM r2_objects
             JOIN jobs ON jobs.id = r2_objects.job_id
             JOIN connections ON connections.id = jobs.connection_id
            WHERE r2_objects.id = ?""",
        (object_id,),
    ).fetchone()
    if row is None:
        raise AdapterError(f"R2 object {object_id} was not found.")
    if row["job_type"] != "backup":
        raise AdapterError("Restore drills accept full-backup objects only, not row archives or manifests.")
    return row


def run_restore_drill(object_id: int, target_database: str | None = None, confirm: str = "") -> int:
    with app.app_context():
        init_db()
        db = get_db()
        job = load_backup_object(object_id)
        mode = "restore" if target_database else "validate"
        if target_database and confirm != "RESTORE":
            raise AdapterError("Actual restore requires --confirm RESTORE.")
        if target_database and engine_name(job) != "postgresql":
            raise AdapterError("Executed restore drills currently support PostgreSQL backups only.")
        drill_id = db.execute(
            """INSERT INTO restore_drills
               (object_id, job_id, mode, target_database, status)
               VALUES (?, ?, ?, ?, 'running')""",
            (object_id, job["job_id"], mode, target_database),
        ).lastrowid
        db.commit()
        try:
            with tempfile.TemporaryDirectory(prefix=f"vaultline-restore-{drill_id}-") as directory:
                suffix = Path(job["object_key"]).suffix or ".backup"
                artifact = Path(directory) / f"backup{suffix}"
                downloaded = download_r2_object(
                    job,
                    job["object_key"],
                    artifact,
                    expected_size=job["size_bytes"],
                    expected_sha256=job["sha256"],
                )
                validation = validate_backup_artifact(job, artifact)
                result = {"download": downloaded, "artifact": validation}
                if target_database:
                    result["restore"] = restore_postgresql_backup(job, artifact, target_database)
            db.execute(
                """UPDATE restore_drills
                      SET status = 'success', verification_json = ?,
                          finished_at = CURRENT_TIMESTAMP
                    WHERE id = ?""",
                (json.dumps(result, sort_keys=True), drill_id),
            )
            db.commit()
            print(json.dumps({"drill_id": drill_id, "status": "success", **result}, indent=2))
            return 0
        except Exception as error:
            db.execute(
                """UPDATE restore_drills
                      SET status = 'failed', error_message = ?,
                          finished_at = CURRENT_TIMESTAMP
                    WHERE id = ?""",
                (str(error)[:4000], drill_id),
            )
            db.commit()
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify or exercise one Vaultline R2 backup")
    parser.add_argument("--object-id", type=int, required=True, help="r2_objects.id for a full backup")
    parser.add_argument("--target-database", help="Pre-created empty PostgreSQL database for an actual restore")
    parser.add_argument("--confirm", default="", help="Must be RESTORE when --target-database is used")
    args = parser.parse_args()
    try:
        return run_restore_drill(args.object_id, args.target_database, args.confirm)
    except AdapterError as error:
        print(str(error))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
