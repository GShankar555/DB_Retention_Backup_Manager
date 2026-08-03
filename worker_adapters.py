"""Live database and Cloudflare R2 adapters used by the system worker.

The adapters deliberately keep credentials out of run messages. Native database
dump utilities are preferred for full backups; row-oriented jobs use DB-API
connections and delete only after an archive has been uploaded and verified.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
import tarfile
from pathlib import Path
from typing import Any


class AdapterError(RuntimeError):
    """An expected configuration, source, or destination failure."""


def field(row: Any, name: str, default: Any = "") -> Any:
    try:
        value = row[name]
    except (KeyError, IndexError, TypeError):
        value = default
    return default if value is None else value


def engine_name(job: Any) -> str:
    return str(field(job, "engine")).lower().replace(" ", "")


def connection_values(job: Any) -> dict[str, Any]:
    return {
        "host": field(job, "host"),
        "port": int(field(job, "port", 5432) or 5432),
        "database": field(job, "database_name"),
        "username": field(job, "username"),
        "password": field(job, "password"),
        "ssl_mode": field(job, "ssl_mode", "require") or "require",
    }


def slug(value: str, fallback: str = "job") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return cleaned[:80] or fallback


def locate_tool(name: str, environment_key: str | None = None) -> str | None:
    """Find native tools even when cron does not load the login-shell PATH."""
    configured = os.getenv(environment_key, "").strip() if environment_key else ""
    candidates = [configured] if configured else []
    found = shutil.which(name)
    if found:
        candidates.append(found)
    candidates.extend([f"/usr/bin/{name}", f"/usr/local/bin/{name}"])
    postgres_root = Path("/usr/lib/postgresql")
    if postgres_root.is_dir():
        candidates.extend(str(version / "bin" / name) for version in postgres_root.iterdir())
    candidates.extend(
        f"/usr/{version}/bin/{name}"
        for version in os.listdir("/usr")
        if version.startswith("pgsql-")
    )
    for candidate in candidates:
        if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def object_key(job: Any, run_id: int, filename: str) -> str:
    source = slug(str(field(job, "connection_name", "database")))
    name = slug(str(field(job, "name", "job")))
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y/%m/%d/%H%M%S")
    return f"vaultline/{source}/{name}/run-{run_id}/{stamp}-{filename}"


def r2_client(job: Any):
    account_id = str(field(job, "r2_account_id", "")).strip()
    access_key = str(field(job, "r2_access_key", "")).strip()
    secret_key = str(field(job, "r2_secret_key", "")).strip()
    bucket = str(field(job, "r2_bucket", "")).strip()
    if not account_id or not access_key or not secret_key or not bucket:
        raise AdapterError("R2 account ID, bucket, access key and secret key are required.")
    try:
        import boto3
    except ImportError as error:
        raise AdapterError("R2 adapter is unavailable: install boto3 from requirements.txt.") from error
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )
    return client, bucket


def upload_to_r2(job: Any, path: Path, key: str, content_type: str) -> dict[str, Any]:
    client, bucket = r2_client(job)
    try:
        client.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={"ContentType": content_type, "Metadata": {"vaultline-job": str(field(job, "id"))}},
        )
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        raise AdapterError(f"R2 upload failed: {str(error)[:240]}") from error
    expected = path.stat().st_size
    actual = int(head.get("ContentLength", -1))
    if actual != expected:
        raise AdapterError(f"R2 verification failed: local size {expected} bytes, remote size {actual} bytes.")
    return {"bucket": bucket, "key": key, "size_bytes": actual, "etag": str(head.get("ETag", "")).strip('"')}


def run_command(command: list[str], destination: Path, env: dict[str, str]) -> None:
    try:
        result = subprocess.run(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
    except OSError as error:
        raise AdapterError(f"Required backup utility could not start: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "command failed").strip().replace("\n", " ")
        raise AdapterError(f"Database backup command failed: {detail[:280]}")
    if not destination.exists() or destination.stat().st_size == 0:
        raise AdapterError("Database backup command completed without producing a backup file.")


def create_native_backup(job: Any, destination: Path) -> tuple[str, str]:
    values = connection_values(job)
    engine = engine_name(job)
    env = os.environ.copy()
    if engine == "postgresql":
        utility = locate_tool("pg_dump", "VAULTLINE_PG_DUMP")
        if not utility:
            raise AdapterError("PostgreSQL backup requires pg_dump; set VAULTLINE_PG_DUMP to its absolute path if cron cannot find it.")
        env["PGPASSWORD"] = str(values["password"])
        env["PGSSLMODE"] = str(values["ssl_mode"])
        command = [
            utility, "--host", str(values["host"]), "--port", str(values["port"]),
            "--username", str(values["username"]), "--dbname", str(values["database"]),
            "--format=custom", "--no-owner", "--no-acl", "--file", str(destination),
        ]
        run_command(command, destination, env)
        return destination.name, "application/octet-stream"
    if engine in {"mysql", "mariadb"}:
        utility = locate_tool("mariadb-dump", "VAULTLINE_MARIADB_DUMP") or locate_tool("mysqldump", "VAULTLINE_MYSQLDUMP")
        if not utility:
            raise AdapterError("MySQL/MariaDB backup requires mysqldump or mariadb-dump on the Linode VM.")
        env["MYSQL_PWD"] = str(values["password"])
        command = [
            utility, "--host", str(values["host"]), "--port", str(values["port"]),
            "--user", str(values["username"]), "--single-transaction", "--routines",
            "--triggers", "--hex-blob", "--result-file", str(destination), str(values["database"]),
        ]
        run_command(command, destination, env)
        return destination.name, "application/sql"
    if engine == "mongodb":
        utility = locate_tool("mongodump", "VAULTLINE_MONGODUMP")
        if not utility:
            raise AdapterError("MongoDB backup requires mongodump on the Linode VM.")
        command = [
            utility, "--host", str(values["host"]), "--port", str(values["port"]),
            "--username", str(values["username"]), "--password", str(values["password"]),
            "--db", str(values["database"]), "--archive=" + str(destination), "--gzip",
        ]
        run_command(command, destination, env)
        return destination.name, "application/gzip"
    if engine in {"sqlserver", "mssql"}:
        create_sqlserver_logical_backup(job, destination)
        return destination.name, "application/gzip"
    raise AdapterError(f"Unsupported live backup engine: {field(job, 'engine')}")


def create_sqlserver_logical_backup(job: Any, destination: Path) -> None:
    """Export accessible SQL Server tables when a server-side .bak is unsuitable."""
    connection = connect_database(job)
    try:
        tables = list_tables(connection, job)
        if not tables:
            raise AdapterError("No SQL Server tables were found.")
        with tarfile.open(destination, "w:gz") as archive:
            manifest = {"type": "sqlserver-logical-export", "database": field(job, "database_name"), "tables": []}
            for schema, table in tables:
                reference = table_reference("sqlserver", schema, table)
                cursor = connection.cursor()
                cursor.execute(f"SELECT * FROM {reference}")
                rows = cursor_rows(cursor)
                payload = ("\n".join(json.dumps(row, default=str, separators=(",", ":")) for row in rows) + ("\n" if rows else "")).encode()
                name = f"{slug((schema + '.' if schema else '') + table)}.jsonl"
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
                manifest["tables"].append({"table": reference, "rows": len(rows), "file": name})
            manifest_payload = json.dumps(manifest, indent=2).encode()
            info = tarfile.TarInfo("manifest.json")
            info.size = len(manifest_payload)
            archive.addfile(info, io.BytesIO(manifest_payload))
    finally:
        connection.close()


def backup_database(job: Any, run_id: int, temp_dir: Path) -> dict[str, Any]:
    extension = {"postgresql": ".dump", "mysql": ".sql", "mariadb": ".sql", "mongodb": ".archive.gz", "sqlserver": ".tar.gz", "mssql": ".tar.gz"}.get(engine_name(job), ".backup")
    filename = f"{slug(str(field(job, 'database_name', 'database')))}{extension}"
    destination = temp_dir / filename
    generated, content_type = create_native_backup(job, destination)
    key = object_key(job, run_id, generated)
    result = upload_to_r2(job, destination, key, content_type)
    result["format"] = generated.rsplit(".", 1)[-1]
    return result


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def split_identifier(value: str) -> tuple[str | None, str]:
    parts = [part.strip() for part in value.split(".")]
    if len(parts) == 1:
        schema, table = None, parts[0]
    elif len(parts) == 2:
        schema, table = parts
    else:
        raise AdapterError(f"Invalid table identifier: {value}")
    if not table or not IDENTIFIER.fullmatch(table) or (schema and not IDENTIFIER.fullmatch(schema)):
        raise AdapterError(f"Unsafe table identifier: {value}")
    return schema, table


def quote_identifier(engine: str, value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise AdapterError(f"Unsafe database identifier: {value}")
    return f'"{value}"' if engine in {"postgresql", "sqlserver", "mssql"} else f"`{value}`"


def table_reference(engine: str, schema: str | None, table: str) -> str:
    if engine == "postgresql":
        return f"{quote_identifier(engine, schema or 'public')}.{quote_identifier(engine, table)}"
    if engine in {"sqlserver", "mssql"}:
        return f"{quote_identifier(engine, schema or 'dbo')}.{quote_identifier(engine, table)}"
    return quote_identifier(engine, table)


def connect_database(job: Any):
    values = connection_values(job)
    engine = engine_name(job)
    if engine == "postgresql":
        try:
            import psycopg
            return psycopg.connect(
                host=values["host"], port=values["port"], dbname=values["database"],
                user=values["username"], password=values["password"], sslmode=values["ssl_mode"],
            )
        except ImportError as error:
            raise AdapterError("PostgreSQL row adapter requires psycopg.") from error
    if engine in {"mysql", "mariadb"}:
        try:
            import pymysql
            return pymysql.connect(
                host=values["host"], port=values["port"], database=values["database"],
                user=values["username"], password=values["password"],
                cursorclass=pymysql.cursors.DictCursor, autocommit=False,
            )
        except ImportError as error:
            raise AdapterError("MySQL/MariaDB row adapter requires PyMySQL.") from error
    if engine in {"sqlserver", "mssql"}:
        try:
            import pyodbc
            connection = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={values['host']},{values['port']};DATABASE={values['database']};"
                f"UID={values['username']};PWD={values['password']};"
                "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=10;"
            )
            return connection
        except ImportError as error:
            raise AdapterError("SQL Server row adapter requires pyodbc and ODBC Driver 18.") from error
    raise AdapterError(f"{field(job, 'engine')} does not support table retention in this worker.")


def connect_mongodb(job: Any):
    values = connection_values(job)
    try:
        from pymongo import MongoClient
        client = MongoClient(
            host=values["host"], port=values["port"], username=values["username"],
            password=values["password"], serverSelectionTimeoutMS=10000,
        )
        database = client[values["database"]]
        client.admin.command("ping")
        return client, database
    except ImportError as error:
        raise AdapterError("MongoDB row adapter requires pymongo.") from error
    except Exception as error:
        raise AdapterError(f"MongoDB connection failed: {str(error)[:240]}") from error


def cursor_rows(cursor) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return [dict(row) for row in rows]
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in rows]


def sql_placeholder(engine: str) -> str:
    return "?" if engine in {"sqlserver", "mssql"} else "%s"


def list_tables(connection, job: Any) -> list[tuple[str | None, str]]:
    engine = engine_name(job)
    cursor = connection.cursor()
    if engine == "postgresql":
        cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name")
        rows = cursor_rows(cursor)
        return [(row["table_schema"], row["table_name"]) for row in rows]
    if engine in {"mysql", "mariadb"}:
        cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
        rows = cursor_rows(cursor)
        return [(None, next(iter(row.values()))) for row in rows]
    if engine in {"sqlserver", "mssql"}:
        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME")
        rows = cursor_rows(cursor)
        return [(row["TABLE_SCHEMA"], row["TABLE_NAME"]) for row in rows]
    raise AdapterError("This database engine does not expose SQL tables.")


def selected_tables(connection, job: Any) -> list[tuple[str | None, str]]:
    if field(job, "tables_scope", "all") == "all":
        tables = list_tables(connection, job)
        if not tables:
            raise AdapterError("No source tables were found.")
        return tables
    raw = [value.strip() for value in str(field(job, "selected_tables", "")).split(",") if value.strip()]
    if not raw:
        raise AdapterError("Selected tables scope requires at least one table name.")
    engine = engine_name(job)
    return [(schema, table) for schema, table in (split_identifier(value) for value in raw)]


def table_columns(connection, job: Any, schema: str | None, table: str) -> set[str]:
    engine = engine_name(job)
    cursor = connection.cursor()
    if engine == "postgresql":
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (schema or "public", table))
    elif engine in {"mysql", "mariadb"}:
        cursor.execute("SELECT COLUMN_NAME AS column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (field(job, "database_name"), table))
    else:
        cursor.execute("SELECT COLUMN_NAME AS column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", (schema or "dbo", table))
    return {str(row["column_name"] if "column_name" in row else row["COLUMN_NAME"]) for row in cursor_rows(cursor)}


def age_column(connection, job: Any, schema: str | None, table: str) -> str:
    columns = table_columns(connection, job, schema, table)
    requested = str(field(job, "retention_column", "created_at")).strip()
    if requested in columns:
        return requested
    lowered = {column.lower(): column for column in columns}
    if requested.lower() in lowered:
        return lowered[requested.lower()]
    for candidate in ("created_at", "createdAt", "created_on", "timestamp", "event_time", "updated_at", "date"):
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    raise AdapterError(f"No age column '{requested}' was found on {table_reference(engine_name(job), schema, table)}.")


def write_archive(rows: list[dict[str, Any]], path: Path, archive_format: str) -> str:
    archive_format = (archive_format or "Parquet").lower()
    if archive_format == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as parquet
            parquet.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
        except ImportError as error:
            raise AdapterError("Parquet archive requires pyarrow from requirements.txt.") from error
        return "application/vnd.apache.parquet"
    if archive_format == "csv":
        with path.open("w", newline="", encoding="utf-8") as handle:
            names = list(rows[0]) if rows else []
            writer = csv.DictWriter(handle, fieldnames=names)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: "" if value is None else str(value) for key, value in row.items()})
        return "text/csv"
    if archive_format == "jsonl":
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
        return "application/x-ndjson"
    raise AdapterError(f"Unsupported archive format: {archive_format}")


def document_value(value: Any) -> Any:
    """Make BSON values stable for Parquet/JSON serialization."""
    if value is None or isinstance(value, (str, int, float, bool, dt.date, dt.datetime)):
        return value
    if isinstance(value, dict):
        return json.dumps({str(key): document_value(item) for key, item in value.items()}, default=str, separators=(",", ":"))
    if isinstance(value, (list, tuple)):
        return json.dumps([document_value(item) for item in value], default=str, separators=(",", ":"))
    return str(value)


def process_mongodb_job(job: Any, run_id: int, temp_dir: Path, archive: bool) -> list[dict[str, Any]]:
    client, database = connect_mongodb(job)
    requested = str(field(job, "selected_tables", ""))
    if field(job, "tables_scope", "all") == "all":
        collections = database.list_collection_names()
    else:
        collections = [value.strip() for value in requested.split(",") if value.strip()]
    if not collections:
        client.close()
        raise AdapterError("No MongoDB collections were found in the selected scope.")
    requested_column = str(field(job, "retention_column", "created_at"))
    days = int(field(job, "retention_days", 730) or 730)
    if days < 1:
        client.close()
        raise AdapterError("Retention window must be at least one day.")
    cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
    results: list[dict[str, Any]] = []
    try:
        for collection_name in collections:
            collection = database[collection_name]
            query = {requested_column: {"$lt": cutoff}}
            rows = [{str(key): document_value(value) for key, value in row.items()} for row in collection.find(query)]
            if not rows:
                results.append({"table": collection_name, "rows": 0, "deleted": 0})
                continue
            if archive:
                extension = (str(field(job, "archive_format", "Parquet")) or "Parquet").lower()
                path = temp_dir / f"{slug(collection_name)}.{extension}"
                content_type = write_archive(rows, path, str(field(job, "archive_format", "Parquet")))
                uploaded = upload_to_r2(job, path, object_key(job, run_id, path.name), content_type)
                uploaded.update({"format": extension, "table": collection_name, "rows": len(rows), "age_column": requested_column})
                results.append(uploaded)
            deleted = collection.delete_many(query).deleted_count
            if archive:
                results[-1]["deleted"] = deleted
            else:
                results.append({"table": collection_name, "rows": len(rows), "deleted": deleted, "age_column": requested_column})
    finally:
        client.close()
    return results


def old_rows(connection, job: Any, schema: str | None, table: str) -> tuple[str, list[dict[str, Any]], Any]:
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    column = age_column(connection, job, schema, table)
    days = int(field(job, "retention_days", 730) or 730)
    if days < 1:
        raise AdapterError("Retention window must be at least one day.")
    cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}", (cutoff,))
    return column, cursor_rows(cursor), cutoff


def preview_row_job(job: Any) -> list[dict[str, Any]]:
    """Count eligible rows using the same scope and age rules as live jobs."""
    if engine_name(job) == "mongodb":
        client, database = connect_mongodb(job)
        try:
            collections = database.list_collection_names() if field(job, "tables_scope", "all") == "all" else [value.strip() for value in str(field(job, "selected_tables", "")).split(",") if value.strip()]
            if not collections:
                raise AdapterError("No MongoDB collections were found in the selected scope.")
            days = int(field(job, "retention_days", 730) or 730)
            cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
            column = str(field(job, "retention_column", "created_at"))
            return [{"table": name, "rows": database[name].count_documents({column: {"$lt": cutoff}}), "age_column": column} for name in collections]
        finally:
            client.close()
    connection = connect_database(job)
    engine = engine_name(job)
    results: list[dict[str, Any]] = []
    try:
        days = int(field(job, "retention_days", 730) or 730)
        if days < 1:
            raise AdapterError("Retention window must be at least one day.")
        cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
        for schema, table in selected_tables(connection, job):
            column = age_column(connection, job, schema, table)
            reference = table_reference(engine, schema, table)
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) AS row_count FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}", (cutoff,))
            row = cursor_rows(cursor)[0]
            results.append({"table": reference, "rows": int(row.get("row_count", row.get("COUNT(*)", 0))), "age_column": column})
    finally:
        connection.close()
    return results


def process_row_job(job: Any, run_id: int, temp_dir: Path, archive: bool) -> list[dict[str, Any]]:
    if engine_name(job) == "mongodb":
        return process_mongodb_job(job, run_id, temp_dir, archive)
    connection = connect_database(job)
    results: list[dict[str, Any]] = []
    engine = engine_name(job)
    try:
        tables = selected_tables(connection, job)
        # Resolve every age column before deleting anything. A bad table in an
        # all-tables scope must fail the job before an earlier table is changed.
        for schema, table in tables:
            age_column(connection, job, schema, table)
        for schema, table in tables:
            column, rows, cutoff = old_rows(connection, job, schema, table)
            reference = table_reference(engine, schema, table)
            if not rows:
                results.append({"table": reference, "rows": 0, "deleted": 0})
                continue
            if archive:
                extension = (str(field(job, "archive_format", "Parquet")) or "Parquet").lower()
                path = temp_dir / f"{slug((schema + '.' if schema else '') + table)}.{extension}"
                content_type = write_archive(rows, path, str(field(job, "archive_format", "Parquet")))
                key = object_key(job, run_id, path.name)
                uploaded = upload_to_r2(job, path, key, content_type)
                uploaded.update({"format": extension, "table": reference, "rows": len(rows), "age_column": column, "cutoff": cutoff})
                results.append(uploaded)
            cursor = connection.cursor()
            cursor.execute(f"DELETE FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}", (cutoff,))
            deleted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else len(rows)
            connection.commit()
            if archive:
                results[-1]["deleted"] = deleted
            else:
                results.append({"table": reference, "rows": len(rows), "deleted": deleted, "age_column": column, "cutoff": cutoff})
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()
    return results
