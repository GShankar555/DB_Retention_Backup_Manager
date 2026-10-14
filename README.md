# Vaultline DB Manager

Flask-first control plane for universal database backups, retention and archive jobs on a Linode VM.

## What is implemented

- Server-rendered Flask app using Jinja, plain HTML, CSS and a small vanilla JavaScript file.
- SQLite metadata storage for database connections, jobs and activity logs.
- Connection options for PostgreSQL, MySQL, MariaDB, SQL Server and MongoDB.
- Three job modes: backup to Cloudflare R2, data retention, and archive old rows to Parquet in R2 before deleting them.
- Daily, weekly, biweekly and monthly schedules, with date/time/timezone fields and generated cron expressions. A UTC-minute dispatcher checks each expression in its saved timezone, so a 23:00 Asia/Kolkata job runs at 23:00 IST even when the VM uses UTC.
- Direct cron override for advanced operators.
- CRUD flows for jobs and connections, with destructive action confirmation.
- Dry-run protection, size + full read-back SHA-256 verified R2 uploads, object ledger metrics, deleted-row counts and an activity/audit view.
- Versioned `archives/v1/<namespace>/...` Parquet manifests. A manifest is accepted by readers only after its source deletion commits; it records source identity, table, columns, primary keys, cutoff, row counts, schema hash and R2 data objects.
- Simple session authentication with the provided administrator credentials.
- Authenticated JSON endpoints: `/api/health`, `/api/jobs`, and `/api/connections`.
- Working control APIs for connection testing, search, CSV export, retention dry runs, SQLite backup, R2 settings and metrics.
- Job create/update/delete automatically rewrites a managed system cron file, defaulting to `/etc/cron.d/vaultline`.

## Build plan for the production worker

1. **Connect:** validate source credentials with the selected database adapter and keep secrets encrypted on the Linode VM.
2. **Preview:** query row counts, table scope and estimated bytes. Require approval for destructive jobs unless dry-run is selected.
3. **Schedule:** job create/update/delete rewrites `/etc/cron.d/vaultline` with one minute-level entry for `scheduler.py`. The dispatcher evaluates enabled jobs against their saved cron expression and timezone, then launches due workers. Click **Settings → Sync now** after deploying scheduler changes. Set `VAULTLINE_CRON_FILE`, `VAULTLINE_CRON_USER`, `VAULTLINE_SCHEDULER`, and `VAULTLINE_WORKER` when the deployment uses different paths or users.
4. **Execute:** native PostgreSQL, MySQL/MariaDB and MongoDB backups are uploaded to R2 through its S3-compatible API. Retention and archive jobs support PostgreSQL, MySQL/MariaDB, SQL Server and MongoDB table/collection scope; archive jobs write Parquet, CSV or JSONL before deletion.
5. **Verify:** check object existence, size and SHA-256 metadata before deleting source rows. PostgreSQL archives lock and materialize the exact primary-key set, serialize only that set, and refuse deletion if row counts diverge. The archive journal persists `ready`, `source_delete_committed`, `committed`, and `failed` transitions for recovery/audit.

## Run locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
VAULTLINE_ENV=development python app.py
```

The app and worker load `.env.development` and then `.env`; variables already set by the service take precedence. Database host, port, name, user and password, R2 account and bucket, job schedule, dry run and the job enabled flag are edited in the UI. A password entered while adding a connection is stored in the local VaultLine SQLite database. Jobs can also be enabled or disabled directly from the Jobs list; that action refreshes the managed cron file. For stronger secret isolation, keep `DB_PASSWORD`, `ARCHIVE_R2_ACCESS_KEY` and `ARCHIVE_R2_SECRET_KEY` in the environment file. The environment password applies only when the connection's database name and user match `DB_NAME` and `DB_USER`; R2 keys apply only when a job's account and bucket match `ARCHIVE_R2_ACCOUNT_ID` and `ARCHIVE_R2_BUCKET`. For other connections or jobs, use `VAULTLINE_CONNECTION_<id>_PASSWORD` and `VAULTLINE_JOB_<id>_R2_ACCESS_KEY`/`VAULTLINE_JOB_<id>_R2_SECRET_KEY`. Environment secrets take precedence over values stored in SQLite.

The connection tester and row adapter use `psycopg` for PostgreSQL, `PyMySQL` for MySQL/MariaDB, and `pymongo` for MongoDB. SQL Server testing and row operations require `pyodbc` plus Microsoft ODBC Driver 18. Full native backups additionally require `pg_dump`, `mysqldump`/`mariadb-dump`, or `mongodump` on the Linode VM.

On Ubuntu/Debian, install the common native tools before creating live backup jobs:

```bash
apt-get update
apt-get install -y postgresql-client default-mysql-client
```

Install MongoDB Database Tools separately when MongoDB backups are needed. After deployment, run `pip install -r requirements.txt`, restart Gunicorn, configure the R2 credentials on the job, test the source connection, run a dry run, then disable dry run for the live job.

Open `http://localhost:5000`. The SQLite database is created at `instance/vaultline.db` unless `VAULTLINE_DB` is set.

For deployment, set `VAULTLINE_ENV=production`, provide
`VAULTLINE_ADMIN_USERNAME`, a password of at least 16 characters, and a strong
`VAULTLINE_SECRET` of at least 32 characters. Production refuses to start with
development defaults. `/healthz` is the only public probe; management APIs remain
session-authenticated.

### Backup validation and restore drills

Every new native backup is structurally validated before upload (including
`pg_restore --list` for PostgreSQL), then size- and SHA-256-verified after a full R2
read-back. Validate a recorded backup without touching a database:

```bash
VAULTLINE_ENV=production .venv/bin/python restore_drill.py --object-id OBJECT_ID
```

For an executed PostgreSQL drill, create an empty non-production database and use the
explicit guard. The command refuses the source database and any non-empty target:

```bash
VAULTLINE_ENV=production .venv/bin/python restore_drill.py \
  --object-id OBJECT_ID --target-database vaultline_restore_drill --confirm RESTORE
```

Results are recorded in `restore_drills`. If a worker crashes after source deletion
but before final manifest promotion, the next archive run automatically republishes
the journaled manifest as committed before processing new rows.

## Linode deployment shape

Use Gunicorn behind Nginx, keep `VAULTLINE_SECRET` in an environment file, and run `worker.py` plus cron under a restricted service account (or root, if that matches how the rest of the host is run — `VAULTLINE_CRON_USER` defaults to `root` since the managed jobs run native dump tools and delete rows, and a non-root Gunicorn account needs explicit write access to `/etc/cron.d/vaultline` before **Settings → Sync now** will work). Install the OS-level backup tools before disabling dry run. The worker verifies uploaded object size, SHA-256 metadata, and a full R2 read-back digest before any archive deletion is committed. SQL Server jobs use a compressed logical table export because native `.bak` files must be written on the SQL Server host.

### Seeding project connections and jobs

`scripts/seed_news_hub_archive.py` creates or updates the News Hub PostgreSQL connection and its cold-archive job from News Hub's own `.env` (same `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD` convention, plus `ARCHIVE_R2_*` if already set there). It's idempotent — safe to re-run after credentials change. New jobs are created disabled and in dry-run mode:

```bash
# local dev
VAULTLINE_ENV=development .venv/bin/python scripts/seed_news_hub_archive.py /path/to/news-hub/backend/.env

# production, once this app is running on the target host
venv/bin/python scripts/seed_news_hub_archive.py /var/www/news-hub/backend/.env
```

Review the created job in the UI, run a dry run, then use **Enable job** and **Enable dry run** on its edit form. Use **Settings → Sync now** to write the cron entry. The seed script reads Vaultline's `.env.development` when present and stores only non-secret connection and R2 fields in SQLite; rerunning it preserves an existing job's enabled and dry-run flags.

`scripts/seed_youtube_archive.py` does the same for the YouTube scraper and its content, version, processing-run, event, and time-series tables:

```bash
VAULTLINE_ENV=development .venv/bin/python scripts/seed_youtube_archive.py /path/to/Yt_scrapper/.env
```

Both profiles default to a seven-day hot window, weekly Sunday execution in their saved
timezone, Parquet in Cloudflare R2, disabled scheduling, and dry-run mode. The YouTube
script prints the exact per-connection and per-job environment variable names needed
for secrets; it never copies passwords or R2 keys into SQLite.

## Universal cold archive contract

Archive jobs are project-agnostic. Set a stable **Archive namespace** such as `news-hub` on the job. The worker writes data to:

```text
archives/v1/<namespace>/<connection>/<database>/<schema>/<table>/run-<id>/data.parquet
archives/v1/<namespace>/<connection>/<database>/<schema>/<table>/run-<id>/manifest.json
```

Consumers must import only manifests whose `status` is `committed`. The manager's SQLite `archive_manifests` table is an operational catalog; the R2 manifest is the portable source of truth, so a project can retrieve cold data without sharing Vaultline's SQLite database. News Hub imports these manifests into its own PostgreSQL `newsapi_archivemanifest` table.

For large tables, use Parquet and a narrow selected-table scope. The worker streams rows in bounded batches and never loads a PostgreSQL table into process memory. Row jobs inspect foreign keys and process selected tables child-first so parent deletes do not violate references. PostgreSQL archive tables must have a primary key; tables without one are refused rather than deleted ambiguously. Always run a dry run first and verify the first committed manifest before enabling the seven-day policy.
