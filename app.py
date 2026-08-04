from __future__ import annotations

import os
import csv
import io
import shlex
import sqlite3
import secrets
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import Flask, Response, flash, g, jsonify, redirect, render_template, request, send_file, session, url_for
from worker_adapters import preview_row_job


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
DATABASE = Path(os.getenv("VAULTLINE_DB", INSTANCE_DIR / "vaultline.db"))
CRON_FILE = Path(os.getenv("VAULTLINE_CRON_FILE", "/etc/cron.d/vaultline"))
CRON_USER = os.getenv("VAULTLINE_CRON_USER", "root")
WORKER_SCRIPT = Path(os.getenv("VAULTLINE_WORKER", BASE_DIR / "worker.py"))
LOG_DIR = Path(os.getenv("VAULTLINE_LOG_DIR", "/var/log/vaultline"))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("VAULTLINE_SECRET", "change-this-in-production")
app.config["DATABASE"] = DATABASE
ADMIN_USERNAME = os.getenv("VAULTLINE_ADMIN_USERNAME", "vaultline@admin")
ADMIN_PASSWORD = os.getenv("VAULTLINE_ADMIN_PASSWORD", "VaultLine@Admin12345")


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        INSTANCE_DIR.mkdir(exist_ok=True)
        g.db = sqlite3.connect(app.config["DATABASE"], timeout=10)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.execute("PRAGMA busy_timeout = 10000")
        g.db.execute("PRAGMA journal_mode = WAL")
    return g.db


@app.teardown_appcontext
def close_db(_exception: BaseException | None = None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            engine TEXT NOT NULL,
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            database_name TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            ssl_mode TEXT DEFAULT 'require',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            connection_id INTEGER NOT NULL,
            r2_bucket TEXT,
            r2_account_id TEXT,
            r2_access_key TEXT,
            r2_secret_key TEXT,
            cadence TEXT NOT NULL,
            run_date TEXT NOT NULL,
            run_time TEXT NOT NULL,
            timezone TEXT NOT NULL DEFAULT 'Asia/Kolkata',
            cron_expression TEXT NOT NULL,
            retention_days INTEGER,
            retention_column TEXT NOT NULL DEFAULT 'created_at',
            tables_scope TEXT NOT NULL DEFAULT 'all',
            selected_tables TEXT,
            archive_format TEXT DEFAULT 'Parquet',
            archive_namespace TEXT NOT NULL DEFAULT '',
            dry_run INTEGER NOT NULL DEFAULT 1,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (connection_id) REFERENCES connections(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            detail TEXT NOT NULL,
            tone TEXT NOT NULL DEFAULT 'teal',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS job_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            progress INTEGER NOT NULL DEFAULT 0,
            message TEXT NOT NULL DEFAULT '',
            rows_processed INTEGER NOT NULL DEFAULT 0,
            rows_deleted INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS job_run_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            level TEXT NOT NULL DEFAULT 'info',
            status TEXT NOT NULL,
            progress INTEGER NOT NULL DEFAULT 0,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES job_runs(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS r2_objects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            bucket TEXT NOT NULL,
            object_key TEXT NOT NULL,
            format TEXT NOT NULL,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            etag TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES job_runs(id) ON DELETE CASCADE,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS archive_manifests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archive_id TEXT NOT NULL UNIQUE,
            run_id INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            namespace TEXT NOT NULL,
            engine TEXT NOT NULL,
            database_name TEXT NOT NULL,
            schema_name TEXT,
            table_name TEXT NOT NULL,
            status TEXT NOT NULL,
            format TEXT NOT NULL,
            age_column TEXT NOT NULL,
            cutoff_value TEXT NOT NULL,
            min_value TEXT,
            max_value TEXT,
            row_count INTEGER NOT NULL DEFAULT 0,
            deleted_rows INTEGER NOT NULL DEFAULT 0,
            manifest_key TEXT NOT NULL,
            data_objects_json TEXT NOT NULL DEFAULT '[]',
            columns_json TEXT NOT NULL DEFAULT '[]',
            primary_keys_json TEXT NOT NULL DEFAULT '[]',
            schema_hash TEXT,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            committed_at TEXT,
            FOREIGN KEY (run_id) REFERENCES job_runs(id) ON DELETE CASCADE,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        """
    )
    columns = {row[1] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
    if "retention_column" not in columns:
        db.execute("ALTER TABLE jobs ADD COLUMN retention_column TEXT NOT NULL DEFAULT 'created_at'")
    if "archive_namespace" not in columns:
        db.execute("ALTER TABLE jobs ADD COLUMN archive_namespace TEXT NOT NULL DEFAULT ''")
    run_columns = {row[1] for row in db.execute("PRAGMA table_info(job_runs)").fetchall()}
    if "rows_processed" not in run_columns:
        db.execute("ALTER TABLE job_runs ADD COLUMN rows_processed INTEGER NOT NULL DEFAULT 0")
    if "rows_deleted" not in run_columns:
        db.execute("ALTER TABLE job_runs ADD COLUMN rows_deleted INTEGER NOT NULL DEFAULT 0")
    if "updated_at" not in run_columns:
        db.execute("ALTER TABLE job_runs ADD COLUMN updated_at TEXT")
        db.execute("UPDATE job_runs SET updated_at = COALESCE(finished_at, started_at, CURRENT_TIMESTAMP) WHERE updated_at IS NULL")
    # Seed the reusable default target from an existing configured job once.
    existing_default = db.execute("SELECT 1 FROM settings WHERE key = 'r2_account_id'").fetchone()
    if not existing_default:
        source = db.execute("""SELECT r2_account_id, r2_bucket, r2_access_key, r2_secret_key
                              FROM jobs
                              WHERE COALESCE(r2_account_id, '') <> ''
                                 OR COALESCE(r2_bucket, '') <> ''
                                 OR COALESCE(r2_access_key, '') <> ''
                              ORDER BY id LIMIT 1""").fetchone()
        if source:
            for key in ("r2_account_id", "r2_bucket", "r2_access_key", "r2_secret_key"):
                db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, source[key] or ""))
    db.commit()


def build_cron(cadence: str, run_date: str, run_time: str) -> str:
    try:
        date = datetime.strptime(run_date, "%Y-%m-%d")
        hour, minute = (int(value) for value in run_time.split(":", 1))
    except (TypeError, ValueError):
        return "0 0 * * *"
    if cadence == "Hourly":
        # The selected minute is retained; the hour and date are ignored.
        return f"{minute} * * * *"
    if cadence == "Daily":
        return f"{minute} {hour} * * *"
    if cadence == "Weekly":
        # Python uses Monday=0; cron uses Sunday=0 and Monday=1.
        return f"{minute} {hour} * * {(date.weekday() + 1) % 7}"
    if cadence == "Biweekly":
        # Cron triggers weekly on the anchor weekday; the worker applies the
        # 14-day anchor check because standard cron cannot express every 14
        # days safely across month boundaries.
        return f"{minute} {hour} * * {(date.weekday() + 1) % 7}"
    return f"{minute} {hour} {date.day} * *"


def valid_timezone(value: str | None) -> str:
    """Return a cron-compatible IANA timezone, or the application default."""
    candidate = (value or "Asia/Kolkata").strip()
    try:
        ZoneInfo(candidate)
    except (ZoneInfoNotFoundError, ValueError):
        return "Asia/Kolkata"
    return candidate


def server_timezone() -> str:
    current = datetime.now().astimezone()
    return getattr(current.tzinfo, "key", None) or current.tzname() or "local"


def log_activity(title: str, detail: str, tone: str = "teal") -> None:
    db = get_db()
    db.execute("INSERT INTO activity (title, detail, tone) VALUES (?, ?, ?)", (title, detail, tone))
    db.commit()


def human_bytes(value: int | float | None) -> str:
    size = float(value or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return "0 B"


def connection_fields(source: dict) -> dict:
    try:
        port = int(source.get("port") or 5432)
    except (TypeError, ValueError):
        port = 0
    return {
        "name": (source.get("name") or "").strip(),
        "engine": source.get("engine", "PostgreSQL"),
        "host": (source.get("host") or "").strip(),
        "port": port,
        "database_name": (source.get("database_name") or "").strip(),
        "username": (source.get("username") or "").strip(),
        "password": source.get("password") or "",
        "ssl_mode": source.get("ssl_mode", "require"),
    }


def test_database_connection(source: dict) -> tuple[bool, str]:
    """Test a saved or unsaved connection using the matching optional driver."""
    data = connection_fields(source)
    if not data["host"] or not data["database_name"] or not data["username"] or not 1 <= data["port"] <= 65535:
        return False, "Valid host, port, database name and username are required."
    engine = data["engine"].lower()
    try:
        if engine == "postgresql":
            try:
                import psycopg
                with psycopg.connect(
                    host=data["host"], port=data["port"], dbname=data["database_name"],
                    user=data["username"], password=data["password"], connect_timeout=5,
                ) as connection:
                    connection.execute("SELECT 1")
            except ImportError:
                import psycopg2
                connection = psycopg2.connect(
                    host=data["host"], port=data["port"], dbname=data["database_name"],
                    user=data["username"], password=data["password"], connect_timeout=5,
                )
                connection.close()
        elif engine in {"mysql", "mariadb"}:
            import pymysql
            connection = pymysql.connect(
                host=data["host"], port=data["port"], database=data["database_name"],
                user=data["username"], password=data["password"], connect_timeout=5,
            )
            connection.close()
        elif engine == "sql server":
            import pyodbc
            connection = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={data['host']},{data['port']};DATABASE={data['database_name']};"
                f"UID={data['username']};PWD={data['password']};"
                "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"
            )
            connection.close()
        elif engine == "mongodb":
            from pymongo import MongoClient
            client = MongoClient(
                host=data["host"], port=data["port"], username=data["username"],
                password=data["password"], serverSelectionTimeoutMS=5000,
            )
            client.admin.command("ping")
            client.close()
        else:
            return False, f"Unsupported database engine: {data['engine']}"
    except ImportError as error:
        return False, f"Driver is not installed for {data['engine']}: {error.name}"
    except Exception as error:  # database drivers expose different exception types
        return False, str(error)[:240] or "Connection failed."
    return True, "Connection verified successfully."


def setting_value(key: str, default: str = "") -> str:
    row = get_db().execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def scheduler_info() -> dict:
    installed = CRON_FILE.exists()
    worker_exists = WORKER_SCRIPT.exists()
    ready = installed and worker_exists
    return {
        "installed": installed,
        "worker_exists": worker_exists,
        "ready": ready,
        "label": "Ready" if ready else ("Cron installed; worker missing" if installed else "Not installed"),
        "path": str(CRON_FILE),
        "worker_path": str(WORKER_SCRIPT),
        "server_timezone": server_timezone(),
    }


JOB_SELECT = """SELECT jobs.*, connections.name AS connection_name, connections.engine,
                       connections.database_name, latest_run.status AS latest_status,
                       latest_run.progress AS latest_progress, latest_run.message AS latest_message,
                       latest_run.started_at AS latest_started_at, latest_run.finished_at AS latest_finished_at
                FROM jobs JOIN connections ON connections.id = jobs.connection_id
                LEFT JOIN job_runs latest_run ON latest_run.id = (
                    SELECT MAX(id) FROM job_runs WHERE job_runs.job_id = jobs.id
                )"""


def fetch_jobs(order: str = "DESC") -> list[sqlite3.Row]:
    return get_db().execute(f"{JOB_SELECT} ORDER BY jobs.id {order}").fetchall()


def sync_cron_file() -> tuple[bool, str]:
    """Rewrite the managed /etc/cron.d file from enabled SQLite jobs."""
    db = get_db()
    jobs = db.execute("SELECT id, name, cron_expression, timezone FROM jobs WHERE enabled = 1 ORDER BY id").fetchall()
    try:
        if not jobs:
            if CRON_FILE.exists():
                CRON_FILE.unlink()
            return True, "No enabled jobs; managed cron file removed."
        CRON_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        python_bin = shlex.quote(sys.executable)
        worker = shlex.quote(str(WORKER_SCRIPT))
        workdir = shlex.quote(str(BASE_DIR))
        lines = [
            "# Managed by Vaultline. Manual edits will be overwritten.",
            "SHELL=/bin/sh",
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "",
        ]
        for job in jobs:
            log_path = shlex.quote(str(LOG_DIR / f"job-{job['id']}.log"))
            command = f"{job['cron_expression']} {CRON_USER} cd {workdir} && VAULTLINE_SCHEDULED=1 {python_bin} {worker} --job-id {job['id']} >> {log_path} 2>&1"
            lines.append(f"# {job['name']} (job {job['id']})")
            lines.append(f"CRON_TZ={valid_timezone(job['timezone'])}")
            lines.append(command)
        lines.append("")
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=CRON_FILE.parent, delete=False) as temporary:
            temporary.write("\n".join(lines))
            temporary_path = Path(temporary.name)
        os.chmod(temporary_path, 0o644)
        os.replace(temporary_path, CRON_FILE)
        return True, f"{len(jobs)} cron job(s) installed in {CRON_FILE}."
    except OSError as error:
        try:
            if "temporary_path" in locals() and temporary_path.exists():
                temporary_path.unlink()
        except OSError:
            pass
        return False, f"Could not update {CRON_FILE}: {error}"


def form_payload() -> dict:
    cadence = request.form.get("cadence", "Daily")
    run_date = request.form.get("run_date", datetime.now().strftime("%Y-%m-%d"))
    run_time = request.form.get("run_time", "23:30")
    return {
        "name": request.form.get("name", "").strip(),
        "job_type": request.form.get("job_type", "backup"),
        "connection_id": request.form.get("connection_id", type=int),
        "r2_bucket": request.form.get("r2_bucket", "").strip(),
        "r2_account_id": request.form.get("r2_account_id", "").strip(),
        "r2_access_key": request.form.get("r2_access_key", "").strip(),
        "r2_secret_key": request.form.get("r2_secret_key", "").strip(),
        "cadence": cadence,
        "run_date": run_date,
        "run_time": run_time,
        "timezone": valid_timezone(request.form.get("timezone", "Asia/Kolkata")),
        "cron_expression": request.form.get("cron_expression", "").strip() or build_cron(cadence, run_date, run_time),
        "retention_days": request.form.get("retention_days", type=int),
        "retention_column": request.form.get("retention_column", "created_at").strip() or "created_at",
        "tables_scope": request.form.get("tables_scope", "all"),
        "selected_tables": request.form.get("selected_tables", "").strip(),
        "archive_format": request.form.get("archive_format", "Parquet"),
        "archive_namespace": request.form.get("archive_namespace", "").strip(),
        "dry_run": 1 if request.form.get("dry_run") == "on" else 0,
    }


@app.context_processor
def inject_globals():
    db = get_db()
    return {
        "current_year": datetime.now().year,
        "nav_counts": {
            "Jobs": db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
            "Connections": db.execute("SELECT COUNT(*) FROM connections").fetchone()[0],
            "Retention": db.execute("SELECT COUNT(*) FROM jobs WHERE job_type IN ('retention', 'archive')").fetchone()[0],
        },
        "logged_in_user": session.get("username"),
        "scheduler_status": scheduler_info(),
        "human_bytes": human_bytes,
    }


@app.before_request
def prepare_database():
    init_db()


@app.before_request
def require_login():
    if request.endpoint == "static" or request.endpoint == "login":
        return None
    if not session.get("authenticated"):
        return redirect(url_for("login", next=request.path))
    return None


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("authenticated"):
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if secrets.compare_digest(username, ADMIN_USERNAME) and secrets.compare_digest(password, ADMIN_PASSWORD):
            session.clear()
            session["authenticated"] = True
            session["username"] = username
            next_path = request.args.get("next", "")
            return redirect(next_path if next_path.startswith("/") else url_for("dashboard"))
        flash("Invalid username or password.", "error")
    return render_template("login.html")


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def dashboard():
    db = get_db()
    jobs = fetch_jobs()
    connections = db.execute("SELECT * FROM connections ORDER BY id").fetchall()
    activities = db.execute("SELECT * FROM activity ORDER BY id DESC LIMIT 5").fetchall()
    next_job = jobs[0] if jobs else None
    metrics = {
        "stored_r2_bytes": db.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM r2_objects").fetchone()[0],
        "executions": db.execute("SELECT COUNT(*) FROM job_runs WHERE status = 'success'").fetchone()[0],
        "rows_deleted": db.execute("SELECT COALESCE(SUM(rows_deleted), 0) FROM job_runs").fetchone()[0],
    }
    return render_template(
        "dashboard.html",
        active="Overview",
        jobs=jobs,
        connections=connections,
        activities=activities,
        protected_count=len(connections),
        next_job=next_job,
        metrics=metrics,
    )


@app.route("/jobs")
def jobs():
    db = get_db()
    filter_type = request.args.get("filter", "all")
    query = JOB_SELECT
    params = []
    if filter_type in {"backup", "archive", "retention"}:
        query += " WHERE jobs.job_type = ?"
        params.append(filter_type)
    query += " ORDER BY jobs.id DESC"
    jobs = db.execute(query, params).fetchall()
    return render_template("jobs.html", active="Jobs", jobs=jobs, filter_type=filter_type)


@app.route("/jobs/new", methods=["GET", "POST"])
def new_job():
    db = get_db()
    connections = db.execute("SELECT * FROM connections ORDER BY name").fetchall()
    if request.method == "POST":
        payload = form_payload()
        if not payload["name"] or not payload["connection_id"]:
            flash("Job name and source connection are required.", "error")
        else:
            columns = ", ".join(payload.keys())
            placeholders = ", ".join("?" for _ in payload)
            db.execute(f"INSERT INTO jobs ({columns}) VALUES ({placeholders})", tuple(payload.values()))
            db.commit()
            cron_ok, cron_message = sync_cron_file()
            log_activity("New job created", f"{payload['name']} · {cron_message}", "teal" if cron_ok else "amber")
            flash("Job created and system cron updated." if cron_ok else f"Job saved, but cron update failed: {cron_message}", "success" if cron_ok else "error")
            return redirect(url_for("jobs"))
    form = request.form if request.method == "POST" else {
        "r2_account_id": setting_value("r2_account_id"),
        "r2_bucket": setting_value("r2_bucket") or "vaultline-prod",
        "r2_access_key": setting_value("r2_access_key"),
        "r2_secret_key": setting_value("r2_secret_key"),
    }
    return render_template("job_form.html", active="Jobs", connections=connections, job=None, form=form)


@app.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
def edit_job(job_id: int):
    db = get_db()
    job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if job is None:
        return redirect(url_for("jobs"))
    connections = db.execute("SELECT * FROM connections ORDER BY name").fetchall()
    if request.method == "POST":
        payload = form_payload()
        assignments = ", ".join(f"{key} = ?" for key in payload)
        db.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", (*payload.values(), job_id))
        db.commit()
        cron_ok, cron_message = sync_cron_file()
        log_activity("Job updated", f"{payload['name']} · {cron_message}", "blue" if cron_ok else "amber")
        flash("Job updated and system cron refreshed." if cron_ok else f"Job saved, but cron update failed: {cron_message}", "success" if cron_ok else "error")
        return redirect(url_for("jobs"))
    return render_template("job_form.html", active="Jobs", connections=connections, job=job, form=dict(job))


@app.post("/jobs/<int:job_id>/delete")
def delete_job(job_id: int):
    db = get_db()
    job = db.execute("SELECT name FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if job:
        db.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        db.commit()
        cron_ok, cron_message = sync_cron_file()
        log_activity("Job deleted", f"{job['name']} · {cron_message}", "amber")
        flash("Job deleted and system cron refreshed." if cron_ok else f"Job deleted, but cron update failed: {cron_message}", "success" if cron_ok else "error")
    return redirect(request.referrer or url_for("jobs"))


@app.route("/connections", methods=["GET", "POST"])
def connections():
    db = get_db()
    if request.method == "POST":
        values = (
            request.form.get("name", "").strip(), request.form.get("engine", "PostgreSQL"),
            request.form.get("host", "").strip(), request.form.get("port", type=int) or 5432,
            request.form.get("database_name", "").strip(), request.form.get("username", "").strip(),
            request.form.get("password", ""), request.form.get("ssl_mode", "require"),
        )
        if not all(values[index] for index in (0, 2, 4, 5)):
            flash("Name, host, database and username are required.", "error")
        else:
            db.execute("""INSERT INTO connections
                (name, engine, host, port, database_name, username, password, ssl_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", values)
            db.commit()
            log_activity("Connection added", f"{values[0]} · {values[1]}", "teal")
            flash("Database connection added.", "success")
            return redirect(url_for("connections"))
    return render_template("connections.html", active="Connections", connections=db.execute("SELECT * FROM connections ORDER BY id DESC").fetchall())


@app.post("/connections/<int:connection_id>/delete")
def delete_connection(connection_id: int):
    db = get_db()
    connection = db.execute("SELECT name FROM connections WHERE id = ?", (connection_id,)).fetchone()
    if connection:
        db.execute("DELETE FROM connections WHERE id = ?", (connection_id,))
        db.commit()
        cron_ok, cron_message = sync_cron_file()
        log_activity("Connection removed", f"{connection['name']} · {cron_message}", "amber")
        flash("Connection removed and system cron refreshed." if cron_ok else f"Connection removed, but cron update failed: {cron_message}", "success" if cron_ok else "error")
    return redirect(url_for("connections"))


@app.route("/connections/<int:connection_id>/edit", methods=["GET", "POST"])
def edit_connection(connection_id: int):
    db = get_db()
    connection = db.execute("SELECT * FROM connections WHERE id = ?", (connection_id,)).fetchone()
    if connection is None:
        return redirect(url_for("connections"))
    if request.method == "POST":
        source = connection_fields(request.form)
        if not source["name"] or not source["host"] or not source["database_name"] or not source["username"]:
            flash("Name, host, database and username are required.", "error")
        else:
            password = source["password"] or connection["password"]
            db.execute("""UPDATE connections SET name = ?, engine = ?, host = ?, port = ?,
                         database_name = ?, username = ?, password = ?, ssl_mode = ? WHERE id = ?""",
                       (source["name"], source["engine"], source["host"], source["port"], source["database_name"], source["username"], password, source["ssl_mode"], connection_id))
            db.commit()
            log_activity("Connection updated", f"{source['name']} · configuration saved", "blue")
            flash("Database connection updated.", "success")
            return redirect(url_for("connections"))
    return render_template("connection_form.html", active="Connections", connection=connection, form=request.form if request.method == "POST" else dict(connection))


@app.route("/retention")
def retention():
    db = get_db()
    jobs = db.execute("""SELECT jobs.*, connections.name AS connection_name, connections.engine, connections.database_name
                        FROM jobs JOIN connections ON connections.id = jobs.connection_id
                        WHERE jobs.job_type IN ('retention', 'archive') ORDER BY jobs.id DESC""").fetchall()
    return render_template("retention.html", active="Retention", jobs=jobs)


@app.route("/activity")
def activity():
    activities = get_db().execute("SELECT * FROM activity ORDER BY id DESC").fetchall()
    return render_template("activity.html", active="Activity", activities=activities)


@app.route("/settings")
def settings():
    return render_template(
        "settings.html",
        active="Settings",
        r2_account_id=setting_value("r2_account_id"),
        r2_bucket=setting_value("r2_bucket"),
        r2_endpoint=setting_value("r2_endpoint"),
        r2_access_key=setting_value("r2_access_key"),
        r2_secret_key=setting_value("r2_secret_key"),
        scheduler=scheduler_info(),
    )


@app.get("/api/health")
def api_health():
    db = get_db()
    db.execute("SELECT 1").fetchone()
    return jsonify({
        "status": "ok",
        "authenticated": True,
        "database": "sqlite",
        "jobs": db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
        "connections": db.execute("SELECT COUNT(*) FROM connections").fetchone()[0],
        "scheduler": scheduler_info(),
    })


@app.get("/api/jobs")
def api_jobs():
    rows = get_db().execute(f"""SELECT jobs.id, jobs.name, jobs.job_type, jobs.cadence,
                              jobs.run_date, jobs.run_time, jobs.cron_expression,
                              jobs.enabled, connections.name AS connection_name,
                              connections.engine, connections.database_name,
                              latest_run.status AS latest_status,
                              latest_run.progress AS latest_progress,
                              latest_run.message AS latest_message,
                              latest_run.started_at AS latest_started_at,
                              latest_run.finished_at AS latest_finished_at
                              FROM jobs JOIN connections ON connections.id = jobs.connection_id
                              LEFT JOIN job_runs latest_run ON latest_run.id = (
                                SELECT MAX(id) FROM job_runs WHERE job_runs.job_id = jobs.id
                              ) ORDER BY jobs.id DESC""").fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/connections")
def api_connections():
    rows = get_db().execute("""SELECT id, name, engine, host, port, database_name,
                              username, ssl_mode, created_at
                              FROM connections ORDER BY id DESC""").fetchall()
    return jsonify([dict(row) for row in rows])


@app.route("/jobs/<int:job_id>/runs")
def job_runs(job_id: int):
    db = get_db()
    job = db.execute(f"{JOB_SELECT} WHERE jobs.id = ?", (job_id,)).fetchone()
    if job is None:
        return redirect(url_for("jobs"))
    runs = db.execute("SELECT * FROM job_runs WHERE job_id = ? ORDER BY id DESC LIMIT 20", (job_id,)).fetchall()
    logs = {}
    for run in runs:
        logs[run["id"]] = db.execute("SELECT * FROM job_run_logs WHERE run_id = ? ORDER BY id", (run["id"],)).fetchall()
    return render_template("job_runs.html", active="Jobs", job=job, runs=runs, logs=logs)


@app.post("/api/jobs/<int:job_id>/run")
def api_run_job(job_id: int):
    db = get_db()
    job = db.execute("SELECT id, name, enabled FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if job is None:
        return jsonify({"ok": False, "message": "Job not found."}), 404
    if not job["enabled"]:
        return jsonify({"ok": False, "message": "Job is disabled."}), 409
    if not WORKER_SCRIPT.exists():
        return jsonify({"ok": False, "message": f"Worker is missing: {WORKER_SCRIPT}"}), 503
    running = db.execute("SELECT id FROM job_runs WHERE job_id = ? AND status = 'running'", (job_id,)).fetchone()
    if running:
        return jsonify({"ok": False, "message": "This job is already running.", "run_id": running["id"]}), 409
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_handle = (LOG_DIR / f"job-{job_id}.log").open("ab")
    except OSError as error:
        return jsonify({"ok": False, "message": f"Worker log could not be opened: {error}"}), 503
    process = subprocess.Popen(
        [sys.executable, str(WORKER_SCRIPT), "--job-id", str(job_id)],
        cwd=str(BASE_DIR), stdout=log_handle, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    log_handle.close()
    return jsonify({"ok": True, "message": f"{job['name']} queued.", "process_id": process.pid}), 202


@app.get("/api/jobs/<int:job_id>/runs")
def api_job_runs(job_id: int):
    db = get_db()
    if db.execute("SELECT id FROM jobs WHERE id = ?", (job_id,)).fetchone() is None:
        return jsonify({"ok": False, "message": "Job not found."}), 404
    runs = db.execute("SELECT * FROM job_runs WHERE job_id = ? ORDER BY id DESC LIMIT 20", (job_id,)).fetchall()
    payload = []
    for run in runs:
        item = dict(run)
        item["logs"] = [dict(log) for log in db.execute("SELECT * FROM job_run_logs WHERE run_id = ? ORDER BY id", (run["id"],)).fetchall()]
        payload.append(item)
    return jsonify({"ok": True, "runs": payload})


@app.post("/api/connections/test")
def api_test_unsaved_connection():
    source = request.get_json(silent=True) or request.form.to_dict()
    ok, message = test_database_connection(source)
    return jsonify({"ok": ok, "message": message}), (200 if ok else 422)


@app.post("/api/connections/<int:connection_id>/test")
def api_test_saved_connection(connection_id: int):
    connection = get_db().execute("SELECT * FROM connections WHERE id = ?", (connection_id,)).fetchone()
    if connection is None:
        return jsonify({"ok": False, "message": "Connection not found."}), 404
    ok, message = test_database_connection(dict(connection))
    if ok:
        log_activity("Connection verified", f"{connection['name']} · {connection['engine']}", "teal")
    return jsonify({"ok": ok, "message": message, "connection_id": connection_id}), (200 if ok else 422)


@app.get("/api/search")
def api_search():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"jobs": [], "connections": []})
    like = f"%{query}%"
    db = get_db()
    jobs = db.execute("""SELECT jobs.id, jobs.name, jobs.job_type, connections.name AS connection_name
                         FROM jobs JOIN connections ON connections.id = jobs.connection_id
                         WHERE jobs.name LIKE ? OR connections.name LIKE ?
                         ORDER BY jobs.id DESC LIMIT 10""", (like, like)).fetchall()
    connections = db.execute("""SELECT id, name, engine, database_name
                               FROM connections WHERE name LIKE ? OR host LIKE ? OR database_name LIKE ?
                               ORDER BY id DESC LIMIT 10""", (like, like, like)).fetchall()
    return jsonify({"jobs": [dict(row) for row in jobs], "connections": [dict(row) for row in connections]})


@app.get("/api/metrics")
def api_metrics():
    db = get_db()
    return jsonify({
        "protected_databases": db.execute("SELECT COUNT(*) FROM connections").fetchone()[0],
        "scheduled_jobs": db.execute("SELECT COUNT(*) FROM jobs WHERE enabled = 1").fetchone()[0],
        "stored_r2_bytes": db.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM r2_objects").fetchone()[0],
        "reclaimed_bytes": None,
        "rows_deleted": db.execute("SELECT COALESCE(SUM(rows_deleted), 0) FROM job_runs").fetchone()[0],
        "executions": db.execute("SELECT COUNT(*) FROM job_runs WHERE status = 'success'").fetchone()[0],
        "scheduler": scheduler_info(),
    })


@app.get("/api/system/scheduler")
def api_scheduler_status():
    return jsonify(scheduler_info())


@app.post("/api/system/scheduler/sync")
def api_scheduler_sync():
    ok, message = sync_cron_file()
    return jsonify({"ok": ok, "message": message, **scheduler_info()}), (200 if ok else 500)


@app.get("/api/export/report")
def api_export_report():
    db = get_db()
    rows = db.execute("""SELECT jobs.name, jobs.job_type, connections.name AS connection,
                        connections.engine, jobs.cadence, jobs.run_date, jobs.run_time,
                        jobs.cron_expression, jobs.enabled
                        FROM jobs JOIN connections ON connections.id = jobs.connection_id
                        ORDER BY jobs.id DESC""").fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["job_name", "job_type", "connection", "engine", "cadence", "run_date", "run_time", "cron_expression", "enabled"])
    writer.writerows([tuple(row) for row in rows])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=vaultline-report.csv"},
    )


@app.post("/api/retention/dry-run")
def api_retention_dry_run():
    rows = get_db().execute("""SELECT jobs.*, connections.name AS connection_name,
                              connections.engine, connections.database_name,
                              connections.host, connections.port, connections.username,
                              connections.password, connections.ssl_mode
                              FROM jobs JOIN connections ON connections.id = jobs.connection_id
                              WHERE jobs.job_type IN ('retention', 'archive')
                              ORDER BY jobs.id DESC""").fetchall()
    policies = []
    for row in rows:
        item = {
            "id": row["id"], "name": row["name"], "connection": row["connection_name"],
            "tables_scope": row["tables_scope"], "selected_tables": row["selected_tables"],
            "retention_days": row["retention_days"], "estimated_rows": 0,
        }
        try:
            preview = preview_row_job(row)
            item["tables"] = preview
            item["estimated_rows"] = sum(int(table["rows"]) for table in preview)
            item["message"] = "Preview only: no source rows or R2 objects were changed."
        except Exception as error:
            item["tables"] = []
            item["message"] = str(error)[:240]
            item["error"] = True
        policies.append(item)
    log_activity("Retention dry run completed", f"{len(policies)} policies previewed", "blue")
    return jsonify({"ok": True, "policies": policies, "message": f"{len(policies)} retention policies previewed."})


@app.get("/api/settings/sqlite-backup")
def api_sqlite_backup():
    get_db().commit()
    return send_file(app.config["DATABASE"], as_attachment=True, download_name="vaultline.db", mimetype="application/octet-stream")


@app.route("/api/settings/r2", methods=["GET", "POST"])
def api_r2_settings():
    db = get_db()
    keys = ("r2_account_id", "r2_bucket", "r2_endpoint", "r2_access_key", "r2_secret_key")
    if request.method == "POST":
        source = request.get_json(silent=True) or request.form
        for key in keys:
            db.execute("INSERT INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP", (key, (source.get(key) or "").strip()))
        db.commit()
        log_activity("R2 settings updated", source.get("r2_bucket") or "Default R2 target", "blue")
    return jsonify({key: setting_value(key) for key in keys if key != "r2_secret_key"})


if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG") == "1")
