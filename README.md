# Vaultline DB Manager

Flask-first control plane for universal database backups, retention and archive jobs on a Linode VM.

## What is implemented

- Server-rendered Flask app using Jinja, plain HTML, CSS and a small vanilla JavaScript file.
- SQLite metadata storage for database connections, jobs and activity logs.
- Connection options for PostgreSQL, MySQL, MariaDB, SQL Server and MongoDB.
- Three job modes: backup to Cloudflare R2, data retention, and archive old rows to Parquet in R2 before deleting them.
- Daily, weekly, biweekly and monthly schedules, with date/time/timezone fields and generated cron expressions.
- Direct cron override for advanced operators.
- CRUD flows for jobs and connections, with destructive action confirmation.
- Dry-run protection and an activity/audit view.
- Simple session authentication with the provided administrator credentials.
- Authenticated JSON endpoints: `/api/health`, `/api/jobs`, and `/api/connections`.
- Working control APIs for connection testing, search, CSV export, retention dry runs, SQLite backup, R2 settings and metrics.
- Job create/update/delete automatically rewrites a managed system cron file, defaulting to `/etc/cron.d/vaultline`.

## Build plan for the production worker

1. **Connect:** validate source credentials with the selected database adapter and keep secrets encrypted on the Linode VM.
2. **Preview:** query row counts, table scope and estimated bytes. Require approval for destructive jobs unless dry-run is selected.
3. **Schedule:** job create/update/delete rewrites `/etc/cron.d/vaultline` with the saved cron expression and worker command. Set `VAULTLINE_CRON_FILE`, `VAULTLINE_CRON_USER`, and `VAULTLINE_WORKER` when the deployment uses different paths or users.
4. **Execute:** stream native backups or table batches to R2 using its S3-compatible API. Archive jobs should write Parquet parts with a stable schema and metadata manifest.
5. **Verify:** check object existence, size and checksum before deleting source rows. The worker run ledger now stores useful milestone status/progress events; write each stage to it and mark the job result.

## Run locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py
```

The connection tester uses `psycopg` for PostgreSQL, `PyMySQL` for MySQL/MariaDB, and `pymongo` for MongoDB. SQL Server testing requires `pyodbc` plus Microsoft ODBC Driver 18 to be installed on the Linode VM.

Open `http://localhost:5000`. The SQLite database is created at `instance/vaultline.db` unless `VAULTLINE_DB` is set.

For deployment, override them with `VAULTLINE_ADMIN_USERNAME`, `VAULTLINE_ADMIN_PASSWORD`, and set a strong `VAULTLINE_SECRET`.

## Linode deployment shape

Use Gunicorn behind Nginx, keep `VAULTLINE_SECRET` in an environment file, and run `worker.py` plus cron under a restricted service account. The current worker provides safe dry-run execution and run logging; database export, Parquet conversion, R2 upload, checksum verification and purge execution are the next adapter layer.
