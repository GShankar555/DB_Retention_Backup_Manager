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
from itertools import islice

from archive_contract import build_manifest, data_key, manifest_key


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
    date = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    return f"vaultline/{source}/{date}/run-{run_id}-{filename}"


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
    try:
        from botocore.config import Config
        config = Config(connect_timeout=15, read_timeout=120, retries={"max_attempts": 3, "mode": "standard"})
    except ImportError:
        config = None
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        **({"config": config} if config else {}),
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


def upload_manifest(job: Any, manifest: dict[str, Any], temp_dir: Path) -> dict[str, Any]:
    """Upload a self-describing archive manifest after data-object verification."""
    key = str(manifest["manifest_key"])
    path = temp_dir / f"manifest-{slug(str(manifest['archive_id']))}.json"
    path.write_text(json.dumps(manifest, default=str, indent=2, sort_keys=True), encoding="utf-8")
    result = upload_to_r2(job, path, key, "application/json")
    result.update({"format": "manifest", "kind": "manifest", "archive_id": manifest["archive_id"], "manifest": manifest})
    return result


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
        env["PGCONNECT_TIMEOUT"] = "15"
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


def create_backup_artifact(job: Any, temp_dir: Path) -> tuple[Path, str]:
    extension = {"postgresql": ".dump", "mysql": ".sql", "mariadb": ".sql", "mongodb": ".archive.gz", "sqlserver": ".tar.gz", "mssql": ".tar.gz"}.get(engine_name(job), ".backup")
    filename = f"{slug(str(field(job, 'database_name', 'database')))}{extension}"
    destination = temp_dir / filename
    generated, content_type = create_native_backup(job, destination)
    return destination, content_type


def upload_backup_artifact(job: Any, run_id: int, path: Path, content_type: str) -> dict[str, Any]:
    generated = path.name
    key = object_key(job, run_id, generated)
    result = upload_to_r2(job, path, key, content_type)
    result["format"] = generated.rsplit(".", 1)[-1]
    return result


def backup_database(job: Any, run_id: int, temp_dir: Path) -> dict[str, Any]:
    destination, content_type = create_backup_artifact(job, temp_dir)
    return upload_backup_artifact(job, run_id, destination, content_type)


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


def rows_from_values(cursor, values: list[Any]) -> list[dict[str, Any]]:
    """Normalize a fetched batch without asking the driver for every row again."""
    if not values:
        return []
    if isinstance(values[0], dict):
        return [dict(row) for row in values]
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in values]


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
        return order_tables_for_deletion(connection, job, tables)
    raw = [value.strip() for value in str(field(job, "selected_tables", "")).split(",") if value.strip()]
    if not raw:
        raise AdapterError("Selected tables scope requires at least one table name.")
    tables = [split_identifier(value) for value in raw]
    return order_tables_for_deletion(connection, job, tables)


def _table_key(engine: str, schema: str | None, table: str, job: Any) -> tuple[str, str]:
    """Normalize implicit schemas so FK metadata matches configured tables."""
    if engine == "postgresql":
        return (schema or "public", table)
    if engine in {"sqlserver", "mssql"}:
        return (schema or "dbo", table)
    return (str(field(job, "database_name", "")), table)


def foreign_key_edges(connection, job: Any) -> list[tuple[tuple[str, str], tuple[str, str]]]:
    """Return (child, parent) table edges from database FK metadata."""
    engine = engine_name(job)
    cursor = connection.cursor()
    if engine == "postgresql":
        cursor.execute(
            """SELECT tc.table_schema AS child_schema, tc.table_name AS child_table,
                      ccu.table_schema AS parent_schema, ccu.table_name AS parent_table
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.constraint_column_usage ccu
                   ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'"""
        )
    elif engine in {"mysql", "mariadb"}:
        cursor.execute(
            """SELECT TABLE_SCHEMA AS child_schema, TABLE_NAME AS child_table,
                      REFERENCED_TABLE_SCHEMA AS parent_schema,
                      REFERENCED_TABLE_NAME AS parent_table
                 FROM information_schema.KEY_COLUMN_USAGE
                WHERE REFERENCED_TABLE_NAME IS NOT NULL
                  AND TABLE_SCHEMA = %s""",
            (field(job, "database_name"),),
        )
    elif engine in {"sqlserver", "mssql"}:
        cursor.execute(
            """SELECT child.TABLE_SCHEMA AS child_schema, child.TABLE_NAME AS child_table,
                      parent.TABLE_SCHEMA AS parent_schema, parent.TABLE_NAME AS parent_table
                 FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                 JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS child
                   ON child.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                  AND child.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                 JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS parent
                   ON parent.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                  AND parent.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
                WHERE child.CONSTRAINT_TYPE = 'FOREIGN KEY'"""
        )
    else:
        return []
    edges = []
    for row in cursor_rows(cursor):
        child = _table_key(engine, row.get("child_schema"), row.get("child_table"), job)
        parent = _table_key(engine, row.get("parent_schema"), row.get("parent_table"), job)
        if child != parent:
            edges.append((child, parent))
    return edges


def order_tables_for_deletion(connection, job: Any, tables: list[tuple[str | None, str]]) -> list[tuple[str | None, str]]:
    """Order selected tables child-first so FK parents are deleted last."""
    engine = engine_name(job)
    original = list(dict.fromkeys(tables))
    keys = {_table_key(engine, schema, table, job): (schema, table) for schema, table in original}
    selected = set(keys)
    children = {key: set() for key in selected}
    for child, parent in foreign_key_edges(connection, job):
        if child in selected and parent in selected:
            children[parent].add(child)

    pending = set(selected)
    ordered: list[tuple[str | None, str]] = []
    while pending:
        # A table with no remaining children can be removed without violating
        # a selected-table foreign key. This produces child-before-parent order.
        ready = [
            _table_key(engine, schema, table, job)
            for schema, table in original
            if _table_key(engine, schema, table, job) in pending
            and not (children[_table_key(engine, schema, table, job)] & pending)
        ]
        if not ready:
            # Cyclic foreign keys cannot be solved by ordering alone. Preserve
            # the configured order and let the database report the constraint.
            ready = [
                _table_key(engine, schema, table, job)
                for schema, table in original
                if _table_key(engine, schema, table, job) in pending
            ]
        ordered.extend(keys[key] for key in ready)
        pending.difference_update(ready)
    return ordered


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


def table_primary_keys(connection, job: Any, schema: str | None, table: str) -> list[str]:
    engine = engine_name(job)
    cursor = connection.cursor()
    if engine == "postgresql":
        cursor.execute(
            """SELECT kcu.column_name
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
              WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = %s AND tc.table_name = %s
              ORDER BY kcu.ordinal_position""",
            (schema or "public", table),
        )
        rows = cursor_rows(cursor)
        return [str(row.get("column_name")) for row in rows if row.get("column_name")]
    if engine in {"mysql", "mariadb"}:
        cursor.execute(
            """SELECT COLUMN_NAME AS column_name
               FROM information_schema.key_column_usage
              WHERE constraint_name = 'PRIMARY'
                AND table_schema = %s AND table_name = %s
              ORDER BY ORDINAL_POSITION""",
            (field(job, "database_name"), table),
        )
        rows = cursor_rows(cursor)
        return [str(row.get("column_name")) for row in rows if row.get("column_name")]
    if engine in {"sqlserver", "mssql"}:
        cursor.execute(
            """SELECT ku.COLUMN_NAME AS column_name
                 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                   ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
                ORDER BY ku.ORDINAL_POSITION""",
            (schema or "dbo", table),
        )
        rows = cursor_rows(cursor)
        return [str(row.get("column_name")) for row in rows if row.get("column_name")]
    return []


def archive_bounds(connection, job: Any, schema: str | None, table: str, column: str, cutoff: Any) -> tuple[Any, Any]:
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT MIN({quote_identifier(engine, column)}) AS min_value, MAX({quote_identifier(engine, column)}) AS max_value FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}",
        (cutoff,),
    )
    row = cursor_rows(cursor)[0]
    return row.get("min_value"), row.get("max_value")


def age_column_candidates(job: Any) -> list[str]:
    raw = str(field(job, "retention_column", "created_at"))
    return [candidate.strip() for candidate in raw.split(",") if candidate.strip()]


def age_column(connection, job: Any, schema: str | None, table: str) -> str | None:
    columns = table_columns(connection, job, schema, table)
    lowered = {column.lower(): column for column in columns}
    for candidate in age_column_candidates(job):
        if candidate in columns:
            return candidate
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


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


ARCHIVE_BATCH_SIZE = 1000


def streaming_cursor(connection, engine: str, name: str):
    """Return a cursor that avoids buffering the complete source table."""
    if engine == "postgresql":
        # A named psycopg cursor streams rows from PostgreSQL instead of keeping
        # the complete result set in the worker process.
        return connection.cursor(name=name)
    if engine in {"mysql", "mariadb"}:
        try:
            import pymysql
            return connection.cursor(pymysql.cursors.SSCursor)
        except ImportError:
            pass
    cursor = connection.cursor()
    cursor.arraysize = ARCHIVE_BATCH_SIZE
    return cursor


def stream_archive_rows(connection, job: Any, schema: str | None, table: str, column: str, cutoff: Any, path: Path) -> tuple[str, int, list[str]]:
    """Write an archive in bounded batches and return its MIME type and count."""
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    cursor = streaming_cursor(connection, engine, f"vaultline_archive_{slug(table)}")
    cursor.execute(
        f"SELECT * FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}",
        (cutoff,),
    )
    archive_format = (str(field(job, "archive_format", "Parquet")) or "Parquet").lower()
    total_rows = 0
    first_batch = rows_from_values(cursor, cursor.fetchmany(ARCHIVE_BATCH_SIZE))
    if not first_batch:
        cursor.close()
        return ({"parquet": "application/vnd.apache.parquet", "csv": "text/csv", "jsonl": "application/x-ndjson"}.get(archive_format, "application/octet-stream"), 0, [])

    if archive_format == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as parquet
        except ImportError as error:
            cursor.close()
            raise AdapterError("Parquet archive requires pyarrow from requirements.txt.") from error
        schema = pa.Table.from_pylist(first_batch).schema
        writer = parquet.ParquetWriter(path, schema, compression="zstd")
        try:
            batch = first_batch
            while batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                total_rows += len(batch)
                batch = rows_from_values(cursor, cursor.fetchmany(ARCHIVE_BATCH_SIZE))
        finally:
            writer.close()
            cursor.close()
        return "application/vnd.apache.parquet", total_rows, list(schema.names)

    if archive_format == "csv":
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(first_batch[0]))
            writer.writeheader()
            batch = first_batch
            while batch:
                for row in batch:
                    writer.writerow({key: "" if value is None else str(value) for key, value in row.items()})
                total_rows += len(batch)
                batch = rows_from_values(cursor, cursor.fetchmany(ARCHIVE_BATCH_SIZE))
        cursor.close()
        return "text/csv", total_rows, list(first_batch[0])

    if archive_format == "jsonl":
        with path.open("w", encoding="utf-8") as handle:
            batch = first_batch
            while batch:
                for row in batch:
                    handle.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
                total_rows += len(batch)
                batch = rows_from_values(cursor, cursor.fetchmany(ARCHIVE_BATCH_SIZE))
        cursor.close()
        return "application/x-ndjson", total_rows, list(first_batch[0])

    cursor.close()
    raise AdapterError(f"Unsupported archive format: {archive_format}")


def stream_mongodb_rows(collection: Any, query: dict[str, Any], path: Path, archive_format: str) -> tuple[str, int, list[str]]:
    """Stream MongoDB documents to an archive without materializing a collection."""
    cursor = collection.find(query).batch_size(ARCHIVE_BATCH_SIZE)
    first = next(cursor, None)
    if first is None:
        return ({"parquet": "application/vnd.apache.parquet", "csv": "text/csv", "jsonl": "application/x-ndjson"}.get(archive_format.lower(), "application/octet-stream"), 0, [])
    first_batch = [{str(key): document_value(value) for key, value in first.items()}]
    columns = list(first_batch[0])
    total = 0
    archive_format = archive_format.lower()

    def next_batch() -> list[dict[str, Any]]:
        return [
            {str(key): document_value(value) for key, value in row.items()}
            for row in islice(cursor, ARCHIVE_BATCH_SIZE)
        ]

    if archive_format == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as parquet
        except ImportError as error:
            raise AdapterError("Parquet archive requires pyarrow from requirements.txt.") from error
        schema = pa.Table.from_pylist(first_batch).schema
        writer = parquet.ParquetWriter(path, schema, compression="zstd")
        try:
            batch = first_batch
            while batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                total += len(batch)
                batch = next_batch()
        finally:
            writer.close()
        return "application/vnd.apache.parquet", total, columns
    if archive_format == "csv":
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            batch = first_batch
            while batch:
                for row in batch:
                    writer.writerow({key: "" if value is None else str(value) for key, value in row.items()})
                total += len(batch)
                batch = next_batch()
        return "text/csv", total, columns
    if archive_format == "jsonl":
        with path.open("w", encoding="utf-8") as handle:
            batch = first_batch
            while batch:
                for row in batch:
                    handle.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
                total += len(batch)
                batch = next_batch()
        return "application/x-ndjson", total, columns
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
    requested_columns = age_column_candidates(job)
    days = int(field(job, "retention_days", 730) or 730)
    if days < 1:
        client.close()
        raise AdapterError("Retention window must be at least one day.")
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    results: list[dict[str, Any]] = []
    try:
        for collection_name in collections:
            collection = database[collection_name]
            sample = collection.find_one()
            requested_column = next((candidate for candidate in requested_columns if candidate in (sample or {})), None)
            if not requested_column:
                results.append({"table": collection_name, "rows": 0, "deleted": 0, "skipped": True, "message": "No configured age column found; collection skipped."})
                continue
            query = {requested_column: {"$lt": cutoff}}
            eligible = collection.count_documents(query)
            if not eligible:
                results.append({"table": collection_name, "rows": 0, "deleted": 0})
                continue
            if archive:
                extension = (str(field(job, "archive_format", "Parquet")) or "Parquet").lower()
                path = temp_dir / f"{slug(collection_name)}.{extension}"
                content_type, streamed, columns = stream_mongodb_rows(collection, query, path, extension)
                key = data_key(job, run_id, None, collection_name, extension)
                uploaded = upload_to_r2(job, path, key, content_type)
                uploaded.update({"format": extension, "kind": "data", "table": collection_name, "rows": streamed, "age_column": requested_column})
                manifest = build_manifest(
                    job,
                    run_id,
                    None,
                    collection_name,
                    status="ready",
                    age_column=requested_column,
                    cutoff=cutoff,
                    row_count=streamed,
                    deleted_rows=0,
                    data_object={
                        "key": uploaded["key"], "format": extension,
                        "content_type": content_type, "size_bytes": uploaded["size_bytes"],
                        "etag": uploaded.get("etag"),
                    },
                    columns=columns,
                    primary_keys=["_id"] if "_id" in columns else [],
                )
                upload_manifest(job, manifest, temp_dir)
            deleted = collection.delete_many(query).deleted_count
            if archive:
                manifest["status"] = "committed"
                manifest["deleted_rows"] = deleted
                manifest["committed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
                manifest_uploaded = upload_manifest(job, manifest, temp_dir)
                uploaded["deleted"] = deleted
                uploaded["manifest_key"] = manifest["manifest_key"]
                results.extend([uploaded, manifest_uploaded])
            else:
                results.append({"table": collection_name, "rows": eligible, "deleted": deleted, "age_column": requested_column})
    finally:
        client.close()
    return results


def old_rows(connection, job: Any, schema: str | None, table: str, column: str | None = None) -> tuple[str, list[dict[str, Any]], Any]:
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    column = column or age_column(connection, job, schema, table)
    if not column:
        raise AdapterError(f"No configured age column was found on {reference}.")
    days = int(field(job, "retention_days", 730) or 730)
    if days < 1:
        raise AdapterError("Retention window must be at least one day.")
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}", (cutoff,))
    return column, cursor_rows(cursor), cutoff


def count_old_rows(connection, job: Any, schema: str | None, table: str, column: str | None = None) -> tuple[str, int, Any]:
    """Count eligible rows without materializing them in Python memory."""
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    column = column or age_column(connection, job, schema, table)
    if not column:
        raise AdapterError(f"No configured age column was found on {reference}.")
    days = int(field(job, "retention_days", 730) or 730)
    if days < 1:
        raise AdapterError("Retention window must be at least one day.")
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT COUNT(*) AS row_count FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}",
        (cutoff,),
    )
    row = cursor_rows(cursor)[0]
    count = next((value for key, value in row.items() if str(key).lower() in {"row_count", "count(*)"}), next(iter(row.values())))
    return column, int(count or 0), cutoff


def preview_row_job(job: Any) -> list[dict[str, Any]]:
    """Count eligible rows using the same scope and age rules as live jobs."""
    if engine_name(job) == "mongodb":
        client, database = connect_mongodb(job)
        try:
            collections = database.list_collection_names() if field(job, "tables_scope", "all") == "all" else [value.strip() for value in str(field(job, "selected_tables", "")).split(",") if value.strip()]
            if not collections:
                raise AdapterError("No MongoDB collections were found in the selected scope.")
            days = int(field(job, "retention_days", 730) or 730)
            cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
            requested_columns = age_column_candidates(job)
            results = []
            for name in collections:
                sample = database[name].find_one()
                column = next((candidate for candidate in requested_columns if candidate in (sample or {})), None)
                if not column:
                    results.append({"table": name, "rows": 0, "age_column": None, "skipped": True, "message": "No configured age column found; collection skipped."})
                else:
                    results.append({"table": name, "rows": database[name].count_documents({column: {"$lt": cutoff}}), "age_column": column})
            return results
        finally:
            client.close()
    connection = connect_database(job)
    engine = engine_name(job)
    results: list[dict[str, Any]] = []
    try:
        days = int(field(job, "retention_days", 730) or 730)
        if days < 1:
            raise AdapterError("Retention window must be at least one day.")
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
        for schema, table in selected_tables(connection, job):
            column = age_column(connection, job, schema, table)
            reference = table_reference(engine, schema, table)
            if not column:
                results.append({"table": reference, "rows": 0, "age_column": None, "skipped": True, "message": "No configured age column found; table skipped."})
                continue
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
        prepared = []
        for schema, table in tables:
            column = age_column(connection, job, schema, table)
            reference = table_reference(engine, schema, table)
            if column:
                prepared.append((schema, table, column))
            else:
                results.append({"table": reference, "rows": 0, "deleted": 0, "skipped": True, "message": "No configured age column found; table skipped."})
        if not prepared:
            return results
        for schema, table, column in prepared:
            reference = table_reference(engine, schema, table)
            if not archive:
                # Retention does not need row contents. Counting and deleting in
                # the database keeps a large cleanup bounded in worker memory.
                column, eligible, cutoff = count_old_rows(connection, job, schema, table, column)
                cursor = connection.cursor()
                cursor.execute(
                    f"DELETE FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}",
                    (cutoff,),
                )
                deleted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else eligible
                connection.commit()
                results.append({"table": reference, "rows": eligible, "deleted": deleted, "age_column": column, "cutoff": cutoff})
                continue

            extension = (str(field(job, "archive_format", "Parquet")) or "Parquet").lower()
            path = temp_dir / f"{slug((schema + '.' if schema else '') + table)}.{extension}"
            column, eligible, cutoff = count_old_rows(connection, job, schema, table, column)
            if not eligible:
                results.append({"table": reference, "rows": 0, "deleted": 0})
                continue
            content_type, streamed, columns = stream_archive_rows(connection, job, schema, table, column, cutoff, path)
            key = data_key(job, run_id, schema, table, extension)
            uploaded = upload_to_r2(job, path, key, content_type)
            uploaded.update({"format": extension, "kind": "data", "table": reference, "rows": streamed, "age_column": column, "cutoff": cutoff})
            min_value, max_value = archive_bounds(connection, job, schema, table, column, cutoff)
            primary_keys = table_primary_keys(connection, job, schema, table)
            manifest = build_manifest(
                job,
                run_id,
                schema,
                table,
                status="ready",
                age_column=column,
                cutoff=cutoff,
                row_count=streamed,
                deleted_rows=0,
                data_object={
                    "key": uploaded["key"],
                    "format": extension,
                    "content_type": content_type,
                    "size_bytes": uploaded["size_bytes"],
                    "etag": uploaded.get("etag"),
                },
                columns=columns,
                primary_keys=primary_keys,
                min_value=min_value,
                max_value=max_value,
            )
            manifest_uploaded = upload_manifest(job, manifest, temp_dir)
            cursor = connection.cursor()
            cursor.execute(
                f"DELETE FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}",
                (cutoff,),
            )
            deleted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else streamed
            connection.commit()
            manifest["status"] = "committed"
            manifest["deleted_rows"] = deleted
            manifest["committed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            manifest_uploaded = upload_manifest(job, manifest, temp_dir)
            uploaded["deleted"] = deleted
            uploaded["manifest_key"] = manifest["manifest_key"]
            results.extend([uploaded, manifest_uploaded])
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()
    return results
