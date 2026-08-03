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

## Build plan for the production worker

1. **Connect:** validate source credentials with the selected database adapter and keep secrets encrypted on the Linode VM.
2. **Preview:** query row counts, table scope and estimated bytes. Require approval for destructive jobs unless dry-run is selected.
3. **Schedule:** render the saved cron expression into `/etc/cron.d/vaultline` with a dedicated service user. Keep cron outside Flask so jobs survive application restarts.
4. **Execute:** stream native backups or table batches to R2 using its S3-compatible API. Archive jobs should write Parquet parts with a stable schema and metadata manifest.
5. **Verify:** check object existence, size and checksum before deleting source rows. Write each stage to the activity log and mark the job result.

## Run locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open `http://localhost:5000`. The SQLite database is created at `instance/vaultline.db` unless `VAULTLINE_DB` is set.

Default administrator login:

- Username: `vaultline@admin`
- Password: `VaultLine@Admin12345`

For deployment, override them with `VAULTLINE_ADMIN_USERNAME`, `VAULTLINE_ADMIN_PASSWORD`, and set a strong `VAULTLINE_SECRET`.

## Linode deployment shape

Use Gunicorn behind Nginx, keep `VAULTLINE_SECRET` in an environment file, and run the worker/cron installer under a restricted service account. The Flask app should manage configuration and previews; the worker should own database export, Parquet conversion, R2 upload, checksum verification and purge execution.
