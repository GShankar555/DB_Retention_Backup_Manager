# Vaultline DB Manager

Flask-first control plane for universal database backups, retention and archive jobs on a Linode VM.

## What is implemented

- Server-rendered Flask app using Jinja, plain HTML, CSS and a small vanilla JavaScript file.
- SQLite metadata storage for database connections, jobs and activity logs.
- Connection options for PostgreSQL, MySQL, MariaDB, SQL Server and MongoDB.
- Three job modes: backup to Cloudflare R2, data retention, and archive old rows to Parquet in R2 before deleting them.
- Daily, weekly, biweekly and monthly schedules, with date/time/timezone fields and generated cron expressions. Each generated entry includes `CRON_TZ`, so a 23:00 Asia/Kolkata job stays at 23:00 IST even when the VM uses UTC.
- Direct cron override for advanced operators.
- CRUD flows for jobs and connections, with destructive action confirmation.
- Dry-run protection, verified R2 uploads, object ledger metrics, deleted-row counts and an activity/audit view.
- Versioned `archives/v1/<namespace>/...` Parquet manifests. A manifest is accepted by readers only after its source deletion commits; it records source identity, table, columns, primary keys, cutoff, row counts, schema hash and R2 data objects.
- Simple session authentication with the provided administrator credentials.
- Authenticated JSON endpoints: `/api/health`, `/api/jobs`, and `/api/connections`.
- Working control APIs for connection testing, search, CSV export, retention dry runs, SQLite backup, R2 settings and metrics.
- Job create/update/delete automatically rewrites a managed system cron file, defaulting to `/etc/cron.d/vaultline`.

## Build plan for the production worker

1. **Connect:** validate source credentials with the selected database adapter and keep secrets encrypted on the Linode VM.
2. **Preview:** query row counts, table scope and estimated bytes. Require approval for destructive jobs unless dry-run is selected.
3. **Schedule:** job create/update/delete rewrites `/etc/cron.d/vaultline` with the saved cron expression, `CRON_TZ` and worker command. Click **Settings → Sync now** after deploying scheduler changes. Set `VAULTLINE_CRON_FILE`, `VAULTLINE_CRON_USER`, and `VAULTLINE_WORKER` when the deployment uses different paths or users.
4. **Execute:** native PostgreSQL, MySQL/MariaDB and MongoDB backups are uploaded to R2 through its S3-compatible API. Retention and archive jobs support PostgreSQL, MySQL/MariaDB, SQL Server and MongoDB table/collection scope; archive jobs write Parquet, CSV or JSONL before deletion.
5. **Verify:** check object existence, size and checksum before deleting source rows. The worker run ledger now stores useful milestone status/progress events; write each stage to it and mark the job result.

## Run locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py
```

The connection tester and row adapter use `psycopg` for PostgreSQL, `PyMySQL` for MySQL/MariaDB, and `pymongo` for MongoDB. SQL Server testing and row operations require `pyodbc` plus Microsoft ODBC Driver 18. Full native backups additionally require `pg_dump`, `mysqldump`/`mariadb-dump`, or `mongodump` on the Linode VM.

On Ubuntu/Debian, install the common native tools before creating live backup jobs:

```bash
apt-get update
apt-get install -y postgresql-client default-mysql-client
```

Install MongoDB Database Tools separately when MongoDB backups are needed. After deployment, run `pip install -r requirements.txt`, restart Gunicorn, configure the R2 credentials on the job, test the source connection, run a dry run, then disable dry run for the live job.

Open `http://localhost:5000`. The SQLite database is created at `instance/vaultline.db` unless `VAULTLINE_DB` is set.

For deployment, override them with `VAULTLINE_ADMIN_USERNAME`, `VAULTLINE_ADMIN_PASSWORD`, and set a strong `VAULTLINE_SECRET`.

## Linode deployment shape

Use Gunicorn behind Nginx, keep `VAULTLINE_SECRET` in an environment file, and run `worker.py` plus cron under a restricted service account. Install the OS-level backup tools before disabling dry run. The worker verifies uploaded object size before any archive/retention delete is committed. SQL Server jobs use a compressed logical table export because native `.bak` files must be written on the SQL Server host.

## Universal cold archive contract

Archive jobs are project-agnostic. Set a stable **Archive namespace** such as `news-hub` on the job. The worker writes data to:

```text
archives/v1/<namespace>/<connection>/<database>/<schema>/<table>/run-<id>/data.parquet
archives/v1/<namespace>/<connection>/<database>/<schema>/<table>/run-<id>/manifest.json
```

Consumers must import only manifests whose `status` is `committed`. The manager's SQLite `archive_manifests` table is an operational catalog; the R2 manifest is the portable source of truth, so a project can retrieve cold data without sharing Vaultline's SQLite database. News Hub imports these manifests into its own PostgreSQL `newsapi_archivemanifest` table.

For large tables, use Parquet and a narrow selected-table scope. The worker streams rows in bounded batches and never loads a PostgreSQL table into process memory. Row jobs inspect foreign keys and process selected tables child-first so parent deletes do not violate references. Always run a dry run first and verify the first committed manifest before enabling a 14-day deletion policy.
