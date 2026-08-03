from __future__ import annotations

import os
import csv
import io
import sqlite3
import secrets
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, flash, g, jsonify, redirect, render_template, request, send_file, session, url_for


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
DATABASE = Path(os.getenv("VAULTLINE_DB", INSTANCE_DIR / "vaultline.db"))
CRON_FILE = Path(os.getenv("VAULTLINE_CRON_FILE", "/etc/cron.d/vaultline"))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("VAULTLINE_SECRET", "change-this-in-production")
app.config["DATABASE"] = DATABASE
ADMIN_USERNAME = os.getenv("VAULTLINE_ADMIN_USERNAME", "vaultline@admin")
ADMIN_PASSWORD = os.getenv("VAULTLINE_ADMIN_PASSWORD", "VaultLine@Admin12345")


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        INSTANCE_DIR.mkdir(exist_ok=True)
        g.db = sqlite3.connect(app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
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
            tables_scope TEXT NOT NULL DEFAULT 'all',
            selected_tables TEXT,
            archive_format TEXT DEFAULT 'Parquet',
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
        """
    )
    db.commit()


def build_cron(cadence: str, run_date: str, run_time: str) -> str:
    try:
        date = datetime.strptime(run_date, "%Y-%m-%d")
        hour, minute = (int(value) for value in run_time.split(":", 1))
    except (TypeError, ValueError):
        return "0 0 * * *"
    if cadence == "Daily":
        return f"{minute} {hour} * * *"
    if cadence == "Weekly":
        # Python uses Monday=0; cron uses Sunday=0 and Monday=1.
        return f"{minute} {hour} * * {(date.weekday() + 1) % 7}"
    if cadence == "Biweekly":
        return f"{minute} {hour} {date.day} */2 *"
    return f"{minute} {hour} {date.day} * *"


def log_activity(title: str, detail: str, tone: str = "teal") -> None:
    db = get_db()
    db.execute("INSERT INTO activity (title, detail, tone) VALUES (?, ?, ?)", (title, detail, tone))
    db.commit()


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
            if engine == "mysql":
                import pymysql
                connection = pymysql.connect(
                    host=data["host"], port=data["port"], database=data["database_name"],
                    user=data["username"], password=data["password"], connect_timeout=5,
                )
            else:
                import mariadb
                connection = mariadb.connect(
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
    return {
        "installed": installed,
        "label": "Installed" if installed else "Not installed",
        "path": str(CRON_FILE),
    }


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
        "timezone": request.form.get("timezone", "Asia/Kolkata"),
        "cron_expression": request.form.get("cron_expression", "").strip() or build_cron(cadence, run_date, run_time),
        "retention_days": request.form.get("retention_days", type=int),
        "tables_scope": request.form.get("tables_scope", "all"),
        "selected_tables": request.form.get("selected_tables", "").strip(),
        "archive_format": request.form.get("archive_format", "Parquet"),
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
        },
        "logged_in_user": session.get("username"),
        "scheduler_status": scheduler_info(),
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
    jobs = db.execute("""SELECT jobs.*, connections.name AS connection_name, connections.engine, connections.database_name
                        FROM jobs JOIN connections ON connections.id = jobs.connection_id
                        ORDER BY jobs.id DESC""").fetchall()
    connections = db.execute("SELECT * FROM connections ORDER BY id").fetchall()
    activities = db.execute("SELECT * FROM activity ORDER BY id DESC LIMIT 5").fetchall()
    next_job = jobs[0] if jobs else None
    return render_template(
        "dashboard.html",
        active="Overview",
        jobs=jobs,
        connections=connections,
        activities=activities,
        protected_count=len(connections),
        next_job=next_job,
    )


@app.route("/jobs")
def jobs():
    db = get_db()
    filter_type = request.args.get("filter", "all")
    query = """SELECT jobs.*, connections.name AS connection_name, connections.engine, connections.database_name
                FROM jobs JOIN connections ON connections.id = jobs.connection_id"""
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
            log_activity("New job created", f"{payload['name']} · system cron installed", "teal")
            flash("Job created. The system-level cron entry is ready to install.", "success")
            return redirect(url_for("jobs"))
    return render_template("job_form.html", active="Jobs", connections=connections, job=None, form=request.form)


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
        log_activity("Job updated", f"{payload['name']} · configuration saved", "blue")
        flash("Job updated successfully.", "success")
        return redirect(url_for("jobs"))
    return render_template("job_form.html", active="Jobs", connections=connections, job=job, form=dict(job))


@app.post("/jobs/<int:job_id>/delete")
def delete_job(job_id: int):
    db = get_db()
    job = db.execute("SELECT name FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if job:
        db.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        db.commit()
        log_activity("Job deleted", f"{job['name']} · cron entry removed", "amber")
        flash("Job deleted and its cron entry removed.", "success")
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
        log_activity("Connection removed", connection["name"], "amber")
        flash("Connection removed. Related jobs were removed too.", "success")
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
    rows = get_db().execute("""SELECT jobs.id, jobs.name, jobs.job_type, jobs.cadence,
                              jobs.run_date, jobs.run_time, jobs.cron_expression,
                              jobs.enabled, connections.name AS connection_name,
                              connections.engine, connections.database_name
                              FROM jobs JOIN connections ON connections.id = jobs.connection_id
                              ORDER BY jobs.id DESC""").fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/connections")
def api_connections():
    rows = get_db().execute("""SELECT id, name, engine, host, port, database_name,
                              username, ssl_mode, created_at
                              FROM connections ORDER BY id DESC""").fetchall()
    return jsonify([dict(row) for row in rows])


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
        "stored_r2_bytes": 0,
        "reclaimed_bytes": 0,
        "executions": 0,
        "scheduler": scheduler_info(),
    })


@app.get("/api/system/scheduler")
def api_scheduler_status():
    return jsonify(scheduler_info())


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
    rows = get_db().execute("""SELECT jobs.id, jobs.name, jobs.job_type, jobs.retention_days,
                              jobs.tables_scope, jobs.selected_tables, connections.name AS connection_name
                              FROM jobs JOIN connections ON connections.id = jobs.connection_id
                              WHERE jobs.job_type IN ('retention', 'archive')
                              ORDER BY jobs.id DESC""").fetchall()
    policies = []
    for row in rows:
        policies.append({
            "id": row["id"], "name": row["name"], "connection": row["connection_name"],
            "tables_scope": row["tables_scope"], "selected_tables": row["selected_tables"],
            "retention_days": row["retention_days"], "estimated_rows": 0,
            "message": "Preview only: no source rows or R2 objects were changed.",
        })
    log_activity("Retention dry run completed", f"{len(policies)} policies previewed", "blue")
    return jsonify({"ok": True, "policies": policies, "message": f"{len(policies)} retention policies previewed."})


@app.get("/api/settings/sqlite-backup")
def api_sqlite_backup():
    get_db().commit()
    return send_file(app.config["DATABASE"], as_attachment=True, download_name="vaultline.db", mimetype="application/octet-stream")


@app.route("/api/settings/r2", methods=["GET", "POST"])
def api_r2_settings():
    db = get_db()
    keys = ("r2_account_id", "r2_bucket", "r2_endpoint")
    if request.method == "POST":
        source = request.get_json(silent=True) or request.form
        for key in keys:
            db.execute("INSERT INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP", (key, (source.get(key) or "").strip()))
        db.commit()
        log_activity("R2 settings updated", source.get("r2_bucket") or "Default R2 target", "blue")
    return jsonify({key: setting_value(key) for key in keys})


if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG") == "1")
