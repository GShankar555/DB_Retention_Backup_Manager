"""Live database and Cloudflare R2 adapters used by the system worker.

The adapters deliberately keep credentials out of run messages. Native database
dump utilities are preferred for full backups; row-oriented jobs use DB-API
connections and delete only after an archive has been uploaded and verified.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
import tarfile
from pathlib import Path
from typing import Any, Callable
from itertools import islice

from archive_contract import build_manifest, data_key, manifest_key
from secret_values import connection_password, r2_credentials


class AdapterError(RuntimeError):
    """An expected configuration, source, or destination failure."""


SOURCE_OUTBOX_TABLE = "vaultline_archive_outbox"


def is_foreign_key_violation(error: Exception) -> bool:
    """Recognize raw-SQL delete failures caused by remaining child rows."""
    code = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)
    message = str(error).lower()
    return code == "23503" or "foreign key constraint" in message or "conflicted with the reference constraint" in message


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
        "password": connection_password(job),
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
    access_key, secret_key = r2_credentials(job)
    access_key, secret_key = access_key.strip(), secret_key.strip()
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


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return a streaming SHA-256 digest without loading large archives in RAM."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


class DigestWriter:
    """File-like sink used by boto3 to hash the bytes R2 returns."""

    def __init__(self) -> None:
        self.digest = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        self.size += len(data)
        return len(data)

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


def upload_to_r2(job: Any, path: Path, key: str, content_type: str) -> dict[str, Any]:
    client, bucket = r2_client(job)
    checksum = file_sha256(path)
    metadata = {
        "vaultline-job": str(field(job, "id")),
        # S3 user-metadata keys are case-insensitive and returned lower-case.
        "sha256": checksum,
    }
    try:
        client.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={"ContentType": content_type, "Metadata": metadata},
        )
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        raise AdapterError(f"R2 upload failed: {str(error)[:240]}") from error
    expected = path.stat().st_size
    actual = int(head.get("ContentLength", -1))
    if actual != expected:
        raise AdapterError(f"R2 verification failed: local size {expected} bytes, remote size {actual} bytes.")
    remote_checksum = str((head.get("Metadata") or {}).get("sha256", "")).lower()
    if not remote_checksum or remote_checksum != checksum:
        raise AdapterError(
            "R2 verification failed: the remote SHA-256 metadata does not match the uploaded file."
        )
    verifier = DigestWriter()
    try:
        # ETags are not content hashes for multipart uploads. Read the object
        # back and hash the bytes R2 actually stored before authorizing delete.
        client.download_fileobj(bucket, key, verifier)
    except Exception as error:
        raise AdapterError(f"R2 checksum read-back failed: {str(error)[:240]}") from error
    if verifier.size != expected or verifier.hexdigest() != checksum:
        raise AdapterError(
            "R2 verification failed: downloaded object bytes do not match the local SHA-256."
        )
    return {
        "bucket": bucket,
        "key": key,
        "size_bytes": actual,
        "etag": str(head.get("ETag", "")).strip('"'),
        "sha256": checksum,
        "checksum_algorithm": "sha256",
        "verified": True,
        "verification_method": "full_object_readback_sha256",
        "verified_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


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


def run_checked_command(command: list[str], env: dict[str, str], action: str) -> str:
    """Run a native verification/restore command without leaking its environment."""
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
        raise AdapterError(f"{action} could not start: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "command failed").strip().replace("\n", " ")
        raise AdapterError(f"{action} failed: {detail[:280]}")
    return result.stdout


def validate_backup_artifact(job: Any, path: Path) -> dict[str, Any]:
    """Fail closed unless a backup is structurally readable by its native format.

    PostgreSQL custom dumps receive the strongest check: ``pg_restore --list``
    parses the complete table-of-contents. Other engines use their container or
    stream integrity checks without connecting to, or mutating, a database.
    """
    if not path.is_file() or path.stat().st_size <= 0:
        raise AdapterError("Backup validation failed: artifact is missing or empty.")
    engine = engine_name(job)
    details: dict[str, Any] = {
        "engine": engine,
        "size_bytes": path.stat().st_size,
        "sha256": file_sha256(path),
    }
    if engine == "postgresql":
        utility = locate_tool("pg_restore", "VAULTLINE_PG_RESTORE")
        if not utility:
            raise AdapterError(
                "PostgreSQL backup validation requires pg_restore; set "
                "VAULTLINE_PG_RESTORE to its absolute path if cron cannot find it."
            )
        output = run_checked_command([utility, "--list", str(path)], os.environ.copy(), "PostgreSQL backup validation")
        entries = sum(1 for line in output.splitlines() if line and not line.startswith(";"))
        if entries <= 0:
            raise AdapterError("PostgreSQL backup validation failed: pg_restore found no archive entries.")
        details.update({"validation_method": "pg_restore_list", "archive_entries": entries})
        return details
    if engine in {"mysql", "mariadb"}:
        with path.open("rb") as handle:
            prefix = handle.read(128 * 1024)
        if b"\x00" in prefix or not any(marker in prefix for marker in (b"-- MySQL dump", b"-- MariaDB dump", b"CREATE ", b"/*!")):
            raise AdapterError("MySQL/MariaDB backup validation failed: artifact is not a recognizable SQL dump.")
        details["validation_method"] = "sql_dump_header"
        return details
    if engine == "mongodb":
        try:
            expanded = 0
            with gzip.open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    expanded += len(chunk)
        except (OSError, EOFError) as error:
            raise AdapterError(f"MongoDB backup validation failed: invalid gzip archive: {error}") from error
        if expanded <= 0:
            raise AdapterError("MongoDB backup validation failed: archive expands to zero bytes.")
        details.update({"validation_method": "gzip_crc", "expanded_size_bytes": expanded})
        return details
    if engine in {"sqlserver", "mssql"}:
        try:
            with tarfile.open(path, "r:gz") as archive:
                members = archive.getmembers()
                manifest_member = next((member for member in members if member.name == "manifest.json"), None)
                if manifest_member is None:
                    raise AdapterError("SQL Server backup validation failed: manifest.json is missing.")
                handle = archive.extractfile(manifest_member)
                manifest = json.load(handle) if handle else {}
        except (tarfile.TarError, OSError, json.JSONDecodeError) as error:
            raise AdapterError(f"SQL Server backup validation failed: {error}") from error
        if manifest.get("type") != "sqlserver-logical-export":
            raise AdapterError("SQL Server backup validation failed: unexpected manifest type.")
        details.update({"validation_method": "tar_manifest", "tables": len(manifest.get("tables", []))})
        return details
    raise AdapterError(f"Backup validation is unavailable for engine: {field(job, 'engine')}")


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
    validate_backup_artifact(job, destination)
    return destination, content_type


def upload_backup_artifact(job: Any, run_id: int, path: Path, content_type: str) -> dict[str, Any]:
    generated = path.name
    key = object_key(job, run_id, generated)
    result = upload_to_r2(job, path, key, content_type)
    result["format"] = generated.rsplit(".", 1)[-1]
    return result


def download_r2_object(
    job: Any,
    key: str,
    destination: Path,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Download an R2 object and independently verify its ledger checksum."""
    client, bucket = r2_client(job)
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        with destination.open("wb") as handle:
            client.download_fileobj(bucket, key, handle)
    except Exception as error:
        destination.unlink(missing_ok=True)
        raise AdapterError(f"R2 restore download failed: {str(error)[:240]}") from error
    actual_size = destination.stat().st_size
    ledger_size = int(expected_size) if expected_size is not None else int(head.get("ContentLength", -1))
    if ledger_size >= 0 and actual_size != ledger_size:
        raise AdapterError(
            f"R2 restore verification failed: expected {ledger_size} bytes, downloaded {actual_size} bytes."
        )
    actual_sha256 = file_sha256(destination)
    ledger_sha256 = str(expected_sha256 or (head.get("Metadata") or {}).get("sha256", "")).lower()
    if not ledger_sha256:
        raise AdapterError("R2 restore verification failed: no trusted SHA-256 is available.")
    if actual_sha256 != ledger_sha256:
        raise AdapterError("R2 restore verification failed: downloaded SHA-256 does not match the ledger.")
    return {
        "bucket": bucket,
        "key": key,
        "size_bytes": actual_size,
        "sha256": actual_sha256,
        "verified_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def restore_postgresql_backup(job: Any, path: Path, target_database: str) -> dict[str, Any]:
    """Restore into an operator-created, empty PostgreSQL drill database."""
    if not IDENTIFIER.fullmatch(target_database):
        raise AdapterError("Restore target must be a simple PostgreSQL database identifier.")
    source_database = str(field(job, "database_name", ""))
    if target_database == source_database:
        raise AdapterError("Restore refused: the drill target must not be the source database.")
    values = connection_values(job)
    try:
        import psycopg
        target = psycopg.connect(
            host=values["host"],
            port=values["port"],
            dbname=target_database,
            user=values["username"],
            password=values["password"],
            sslmode=values["ssl_mode"],
            connect_timeout=15,
        )
    except ImportError as error:
        raise AdapterError("PostgreSQL restore drill requires psycopg.") from error
    except Exception as error:
        raise AdapterError(f"Could not connect to PostgreSQL restore target: {str(error)[:240]}") from error
    try:
        with target.cursor() as cursor:
            cursor.execute(
                """SELECT COUNT(*)
                     FROM pg_catalog.pg_class c
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S')
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND n.nspname !~ '^pg_toast'"""
            )
            existing = int(cursor.fetchone()[0])
        if existing:
            raise AdapterError(
                f"Restore refused: target database {target_database} contains {existing} user object(s)."
            )
    finally:
        target.close()
    utility = locate_tool("pg_restore", "VAULTLINE_PG_RESTORE")
    if not utility:
        raise AdapterError("PostgreSQL restore drill requires pg_restore.")
    env = os.environ.copy()
    env["PGPASSWORD"] = str(values["password"])
    env["PGSSLMODE"] = str(values["ssl_mode"])
    env["PGCONNECT_TIMEOUT"] = "15"
    command = [
        utility,
        "--host", str(values["host"]),
        "--port", str(values["port"]),
        "--username", str(values["username"]),
        "--dbname", target_database,
        "--no-owner",
        "--no-acl",
        "--exit-on-error",
        str(path),
    ]
    run_checked_command(command, env, "PostgreSQL restore drill")
    try:
        target = psycopg.connect(
            host=values["host"], port=values["port"], dbname=target_database,
            user=values["username"], password=values["password"],
            sslmode=values["ssl_mode"], connect_timeout=15,
        )
        with target.cursor() as cursor:
            cursor.execute(
                """SELECT COUNT(*)
                     FROM pg_catalog.pg_class c
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p')
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND n.nspname !~ '^pg_toast'"""
            )
            restored_relations = int(cursor.fetchone()[0])
    except Exception as error:
        raise AdapterError(f"Restore completed but validation query failed: {str(error)[:240]}") from error
    finally:
        try:
            target.close()
        except Exception:
            pass
    if restored_relations <= 0:
        raise AdapterError("Restore drill failed: no user tables were found in the target database.")
    return {"target_database": target_database, "restored_relations": restored_relations}


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
        return [
            (row["table_schema"], row["table_name"])
            for row in rows
            if row["table_name"] != SOURCE_OUTBOX_TABLE
        ]
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


def referencing_columns(connection, job: Any, schema: str | None, table: str) -> list[tuple[str | None, str, str, str]]:
    """Return blocking (child schema/table/column, parent column) references.

    CASCADE and SET NULL constraints safely resolve themselves during DELETE;
    only NO ACTION/RESTRICT-style references must exclude a candidate row.
    Return (child_schema, child_table, child_fk_column, this_table_column) for
    every foreign key anywhere in the database that references (schema, table),
    including tables outside this job's configured scope."""
    engine = engine_name(job)
    cursor = connection.cursor()
    if engine == "postgresql":
        cursor.execute(
            """SELECT tc.table_schema AS child_schema, tc.table_name AS child_table,
                      kcu.column_name AS child_column, ccu.column_name AS parent_column,
                      rc.delete_rule AS delete_rule
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.referential_constraints rc
                   ON rc.constraint_name = tc.constraint_name
                  AND rc.constraint_schema = tc.constraint_schema
                 JOIN information_schema.key_column_usage kcu
                   ON kcu.constraint_name = tc.constraint_name
                  AND kcu.constraint_schema = tc.constraint_schema
                 JOIN information_schema.constraint_column_usage ccu
                   ON ccu.constraint_name = tc.constraint_name
                  AND ccu.constraint_schema = tc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND ccu.table_schema = %s AND ccu.table_name = %s""",
            (schema or "public", table),
        )
    elif engine in {"mysql", "mariadb"}:
        cursor.execute(
            """SELECT k.TABLE_SCHEMA AS child_schema, k.TABLE_NAME AS child_table,
                      k.COLUMN_NAME AS child_column, k.REFERENCED_COLUMN_NAME AS parent_column,
                      rc.DELETE_RULE AS delete_rule
                 FROM information_schema.KEY_COLUMN_USAGE k
                 JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                   ON rc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                  AND rc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                WHERE REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s""",
            (field(job, "database_name"), table),
        )
    elif engine in {"sqlserver", "mssql"}:
        cursor.execute(
            """SELECT child_tc.TABLE_SCHEMA AS child_schema, child_tc.TABLE_NAME AS child_table,
                      child_kcu.COLUMN_NAME AS child_column, parent_kcu.COLUMN_NAME AS parent_column,
                      rc.DELETE_RULE AS delete_rule
                 FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                 JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS child_tc
                   ON child_tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME AND child_tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                 JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS parent_tc
                   ON parent_tc.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME AND parent_tc.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE child_kcu
                   ON child_kcu.CONSTRAINT_NAME = child_tc.CONSTRAINT_NAME
                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE parent_kcu
                   ON parent_kcu.CONSTRAINT_NAME = parent_tc.CONSTRAINT_NAME
                WHERE parent_tc.TABLE_SCHEMA = ? AND parent_tc.TABLE_NAME = ?""",
            (schema or "dbo", table),
        )
    else:
        return []
    rows = cursor_rows(cursor)
    this_key = _table_key(engine, schema, table, job)
    return [
        (row.get("child_schema"), row["child_table"], row["child_column"], row["parent_column"])
        for row in rows
        if _table_key(engine, row.get("child_schema"), row["child_table"], job) != this_key
        and str(row.get("delete_rule", "NO ACTION")).upper() not in {"CASCADE", "SET NULL"}
    ]


def exclusion_clause(connection, job: Any, schema: str | None, table: str) -> str:
    """Build a WHERE-clause fragment excluding rows still referenced by a row
    elsewhere in the database by a NO ACTION/RESTRICT constraint. Database
    CASCADE and SET NULL references are intentionally omitted because the
    database resolves them atomically with the parent DELETE."""
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    clauses = [
        f"NOT EXISTS (SELECT 1 FROM {table_reference(engine, child_schema, child_table)} "
        f"WHERE {table_reference(engine, child_schema, child_table)}.{quote_identifier(engine, child_column)} "
        f"= {reference}.{quote_identifier(engine, parent_column)})"
        for child_schema, child_table, child_column, parent_column in referencing_columns(connection, job, schema, table)
    ]
    return "".join(f" AND {clause}" for clause in clauses)


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


def retention_cutoff(connection, job: Any, schema: str | None, table: str, column: str, days: int) -> Any:
    """Return a cutoff compatible with the source column's physical type.

    The YouTube project's legacy tables store ISO-8601 timestamps as TEXT,
    while the canonical tables use TIMESTAMPTZ. PostgreSQL does not implicitly
    compare those text columns with a Python datetime parameter.
    """
    now = dt.datetime.now(dt.timezone.utc)
    # hot_until is already the absolute lifecycle boundary written by the
    # producers. Subtracting the configured window a second time would keep
    # rows hot for fourteen days instead of seven.
    cutoff = now if column.lower() == "hot_until" else now - dt.timedelta(days=days)
    engine = engine_name(job)
    cursor = connection.cursor()
    if engine == "postgresql":
        cursor.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (schema or "public", table, column),
        )
    elif engine in {"mysql", "mariadb"}:
        cursor.execute(
            "SELECT DATA_TYPE AS data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (field(job, "database_name"), table, column),
        )
    else:
        cursor.execute(
            "SELECT DATA_TYPE AS data_type FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
            (schema or "dbo", table, column),
        )
    rows = cursor_rows(cursor)
    data_type = str(rows[0].get("data_type", "")).lower() if rows else ""
    if data_type in {"text", "character varying", "character", "varchar", "nvarchar"}:
        return cutoff.date().isoformat() if column.lower() == "day" else cutoff.isoformat()
    if data_type == "date":
        return cutoff.date()
    return cutoff


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


def postgres_candidate_table(run_id: int, schema: str | None, table: str) -> str:
    """Return a short, quoted session-local table name for one archive set."""
    identity = f"{run_id}|{schema or 'public'}|{table}".encode("utf-8")
    suffix = hashlib.sha256(identity).hexdigest()[:16]
    return quote_identifier("postgresql", f"vaultline_keys_{suffix}")


def lock_postgres_candidates(
    connection,
    job: Any,
    run_id: int,
    schema: str | None,
    table: str,
    column: str,
    cutoff: Any,
    extra_where: str,
    primary_keys: list[str],
) -> tuple[str, int]:
    """Lock and materialize the exact PostgreSQL primary-key set to archive.

    The row locks remain held until the per-table transaction commits. This
    prevents rows being updated/deleted between serialization and deletion,
    while the temporary identity table prevents newly eligible rows from being
    swept into the later DELETE.
    """
    if not primary_keys:
        raise AdapterError(
            f"Archive refused for {table_reference('postgresql', schema, table)}: "
            "a primary key is required for exact-set verification."
        )
    reference = table_reference("postgresql", schema, table)
    candidate_table = postgres_candidate_table(run_id, schema, table)
    keys = ", ".join(quote_identifier("postgresql", key) for key in primary_keys)
    cursor = connection.cursor()
    cursor.execute(
        f"CREATE TEMP TABLE {candidate_table} ON COMMIT DROP AS "
        f"SELECT {keys} FROM {reference} WHERE FALSE"
    )
    cursor.execute(
        f"WITH locked_rows AS ("
        f"SELECT {keys} FROM {reference} "
        f"WHERE {quote_identifier('postgresql', column)} < %s{extra_where} "
        f"FOR UPDATE SKIP LOCKED"
        f") INSERT INTO {candidate_table} ({keys}) SELECT {keys} FROM locked_rows",
        (cutoff,),
    )
    cursor.execute(f"SELECT COUNT(*) AS row_count FROM {candidate_table}")
    count = int(cursor_rows(cursor)[0]["row_count"])
    return candidate_table, count


def postgres_identity_join(primary_keys: list[str], source_alias: str = "source", key_alias: str = "candidate_keys") -> str:
    return " AND ".join(
        f"{source_alias}.{quote_identifier('postgresql', key)} = "
        f"{key_alias}.{quote_identifier('postgresql', key)}"
        for key in primary_keys
    )


def archive_bounds(connection, job: Any, schema: str | None, table: str, column: str, cutoff: Any, extra_where: str = "", *, candidate_table: str | None = None, primary_keys: list[str] | None = None) -> tuple[Any, Any]:
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    cursor = connection.cursor()
    if candidate_table:
        join = postgres_identity_join(primary_keys or [])
        cursor.execute(
            f"SELECT MIN(source.{quote_identifier(engine, column)}) AS min_value, "
            f"MAX(source.{quote_identifier(engine, column)}) AS max_value "
            f"FROM {reference} AS source JOIN {candidate_table} AS candidate_keys ON {join}"
        )
    else:
        cursor.execute(
            f"SELECT MIN({quote_identifier(engine, column)}) AS min_value, MAX({quote_identifier(engine, column)}) AS max_value FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}{extra_where}",
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


def widen_null_typed_fields(schema: Any) -> tuple[Any, set[str]]:
    """Pyarrow infers a column as its ``null`` type when every value in the
    sampled batch is None. A later batch with a real value for that column
    then fails with ``ArrowInvalid: Invalid null value`` because nothing
    else can be written into a null-typed column. Widen those columns to
    nullable strings up front so real values later in the stream fit."""
    import pyarrow as pa

    null_fields = {field.name for field in schema if pa.types.is_null(field.type)}
    if not null_fields:
        return schema, null_fields
    widened = pa.schema([
        pa.field(field.name, pa.string(), nullable=True) if field.name in null_fields else field
        for field in schema
    ])
    return widened, null_fields


def coerce_null_typed_fields(batch: list[dict[str, Any]], null_fields: set[str]) -> list[dict[str, Any]]:
    if not null_fields:
        return batch
    return [
        {key: (str(value) if key in null_fields and value is not None else value) for key, value in row.items()}
        for row in batch
    ]


def json_encode_complex_values(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Postgres jsonb/array columns (dict/list values) have organically
    varying shape row to row - AI-extracted fields especially. Letting
    pyarrow infer a struct/list type from one sample batch is brittle: a
    later row with different keys, nesting, or an all-null nested field
    raises ``ArrowInvalid: Invalid null value`` or a type mismatch. Encode
    these to compact JSON text instead, so every batch is a plain nullable
    string column regardless of what's inside. Readers already accept a
    JSON string here (see News Hub's ``archive.json_field``)."""
    return [
        {
            key: (json.dumps(value, default=str, separators=(",", ":")) if isinstance(value, (dict, list)) else value)
            for key, value in row.items()
        }
        for row in batch
    ]


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


def stream_archive_rows(connection, job: Any, schema: str | None, table: str, column: str, cutoff: Any, path: Path, extra_where: str = "", *, candidate_table: str | None = None, primary_keys: list[str] | None = None) -> tuple[str, int, list[str]]:
    """Write an archive in bounded batches and return its MIME type and count."""
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    cursor = streaming_cursor(connection, engine, f"vaultline_archive_{slug(table)}")
    if candidate_table:
        join = postgres_identity_join(primary_keys or [])
        cursor.execute(
            f"SELECT source.* FROM {reference} AS source "
            f"JOIN {candidate_table} AS candidate_keys ON {join}"
        )
    else:
        cursor.execute(
            f"SELECT * FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}{extra_where}",
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
        first_batch = json_encode_complex_values(first_batch)
        schema = pa.Table.from_pylist(first_batch).schema
        schema, null_fields = widen_null_typed_fields(schema)
        writer = parquet.ParquetWriter(path, schema, compression="zstd")
        try:
            batch = coerce_null_typed_fields(first_batch, null_fields)
            while batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                total_rows += len(batch)
                batch = coerce_null_typed_fields(
                    json_encode_complex_values(rows_from_values(cursor, cursor.fetchmany(ARCHIVE_BATCH_SIZE))), null_fields
                )
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
        schema, null_fields = widen_null_typed_fields(schema)
        writer = parquet.ParquetWriter(path, schema, compression="zstd")
        try:
            batch = coerce_null_typed_fields(first_batch, null_fields)
            while batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                total += len(batch)
                batch = coerce_null_typed_fields(next_batch(), null_fields)
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
    days = int(field(job, "retention_days", 7) or 7)
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
                        "sha256": uploaded["sha256"],
                        "checksum_algorithm": uploaded["checksum_algorithm"],
                        "verified": uploaded["verified"],
                        "verification_method": uploaded["verification_method"],
                        "verified_at": uploaded["verified_at"],
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
    days = int(field(job, "retention_days", 7) or 7)
    if days < 1:
        raise AdapterError("Retention window must be at least one day.")
    cutoff = retention_cutoff(connection, job, schema, table, column, days)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}", (cutoff,))
    return column, cursor_rows(cursor), cutoff


def count_old_rows(connection, job: Any, schema: str | None, table: str, column: str | None = None, extra_where: str = "") -> tuple[str, int, Any]:
    """Count eligible rows without materializing them in Python memory."""
    engine = engine_name(job)
    reference = table_reference(engine, schema, table)
    column = column or age_column(connection, job, schema, table)
    if not column:
        raise AdapterError(f"No configured age column was found on {reference}.")
    days = int(field(job, "retention_days", 7) or 7)
    if days < 1:
        raise AdapterError("Retention window must be at least one day.")
    cutoff = retention_cutoff(connection, job, schema, table, column, days)
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT COUNT(*) AS row_count FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}{extra_where}",
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
            days = int(field(job, "retention_days", 7) or 7)
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
        days = int(field(job, "retention_days", 7) or 7)
        if days < 1:
            raise AdapterError("Retention window must be at least one day.")
        for schema, table in selected_tables(connection, job):
            column = age_column(connection, job, schema, table)
            reference = table_reference(engine, schema, table)
            if not column:
                results.append({"table": reference, "rows": 0, "age_column": None, "skipped": True, "message": "No configured age column found; table skipped."})
                continue
            cutoff = retention_cutoff(connection, job, schema, table, column, days)
            extra_where = exclusion_clause(connection, job, schema, table)
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) AS row_count FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}{extra_where}", (cutoff,))
            row = cursor_rows(cursor)[0]
            results.append({"table": reference, "rows": int(row.get("row_count", row.get("COUNT(*)", 0))), "age_column": column})
    finally:
        connection.close()
    return results


def ensure_source_outbox(connection) -> None:
    """Create the PostgreSQL recovery outbox used by archive DELETE commits."""
    cursor = connection.cursor()
    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS public.{SOURCE_OUTBOX_TABLE} (
                archive_id TEXT PRIMARY KEY,
                manifest JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    connection.commit()


def write_source_outbox(connection, manifest: dict[str, Any]) -> None:
    cursor = connection.cursor()
    cursor.execute(
        f"""INSERT INTO public.{SOURCE_OUTBOX_TABLE} (archive_id, manifest)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (archive_id) DO UPDATE SET manifest = EXCLUDED.manifest""",
        (manifest["archive_id"], json.dumps(manifest, default=str, sort_keys=True)),
    )


def delete_source_outbox(connection, archive_id: str) -> None:
    cursor = connection.cursor()
    cursor.execute(
        f"DELETE FROM public.{SOURCE_OUTBOX_TABLE} WHERE archive_id = %s",
        (archive_id,),
    )
    connection.commit()


def recover_source_outbox(
    connection,
    job: Any,
    temp_dir: Path,
    archive_state_callback: Callable[[dict[str, Any]], None] | None,
) -> int:
    """Finish manifest promotion after an atomically committed source delete."""
    ensure_source_outbox(connection)
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT archive_id, manifest FROM public.{SOURCE_OUTBOX_TABLE} ORDER BY created_at, archive_id"
    )
    rows = cursor_rows(cursor)
    recovered = 0
    for row in rows:
        payload = row["manifest"]
        manifest = json.loads(payload) if isinstance(payload, str) else dict(payload)
        if manifest.get("status") != "source_delete_committed":
            raise AdapterError(
                f"Source outbox archive {row['archive_id']} has an invalid state; refusing to discard it."
            )
        if archive_state_callback:
            archive_state_callback(manifest)
        manifest["status"] = "committed"
        manifest["committed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        upload_manifest(job, manifest, temp_dir)
        if archive_state_callback:
            archive_state_callback(manifest)
        delete_source_outbox(connection, str(row["archive_id"]))
        recovered += 1
    return recovered


def process_row_job(
    job: Any,
    run_id: int,
    temp_dir: Path,
    archive: bool,
    archive_state_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    engine = engine_name(job)
    if engine != "postgresql":
        raise AdapterError(
            "Live archive/retention deletion is currently supported only for PostgreSQL, "
            "where Vaultline locks an exact primary-key set. Use dry-run preview or a full "
            "backup for MongoDB, MySQL/MariaDB, and SQL Server."
        )
    connection = connect_database(job)
    results: list[dict[str, Any]] = []
    try:
        if archive:
            recover_source_outbox(connection, job, temp_dir, archive_state_callback)
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
            # A raw DELETE fails on the whole batch if even one candidate row is
            # still referenced elsewhere (ORM-level cascade rules aren't real
            # database ON DELETE clauses). Excluding those rows up front lets the
            # rest of the batch through instead of losing it all to a handful of
            # stragglers still blocked at delete time by a genuine race.
            extra_where = exclusion_clause(connection, job, schema, table)
            if not archive:
                # Retention does not need row contents. Counting and deleting in
                # the database keeps a large cleanup bounded in worker memory.
                candidate_table = None
                primary_keys: list[str] = []
                if engine == "postgresql":
                    days = int(field(job, "retention_days", 7) or 7)
                    if days < 1:
                        raise AdapterError("Retention window must be at least one day.")
                    cutoff = retention_cutoff(connection, job, schema, table, column, days)
                    primary_keys = table_primary_keys(connection, job, schema, table)
                    candidate_table, eligible = lock_postgres_candidates(
                        connection,
                        job,
                        run_id,
                        schema,
                        table,
                        column,
                        cutoff,
                        extra_where,
                        primary_keys,
                    )
                else:
                    column, eligible, cutoff = count_old_rows(
                        connection, job, schema, table, column, extra_where=extra_where
                    )
                cursor = connection.cursor()
                try:
                    if candidate_table:
                        join = postgres_identity_join(primary_keys)
                        cursor.execute(
                            f"DELETE FROM {reference} AS source USING {candidate_table} AS candidate_keys "
                            f"WHERE {join}"
                        )
                    else:
                        cursor.execute(
                            f"DELETE FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}{extra_where}",
                            (cutoff,),
                        )
                    deleted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else eligible
                    if deleted != eligible:
                        raise AdapterError(
                            f"Retention/delete mismatch for {reference}: selected {eligible} row(s) "
                            f"but DELETE matched {deleted}. Transaction rolled back."
                        )
                    connection.commit()
                except Exception as error:
                    connection.rollback()
                    if not is_foreign_key_violation(error):
                        raise
                    results.append({
                        "kind": "deferred",
                        "table": reference,
                        "rows": eligible,
                        "deleted": 0,
                        "deferred": True,
                        "age_column": column,
                        "cutoff": cutoff,
                        "message": f"Deletion deferred because newer child rows still reference this table: {str(error)[:240]}",
                    })
                    continue
                results.append({"table": reference, "rows": eligible, "deleted": deleted, "age_column": column, "cutoff": cutoff})
                continue

            extension = (str(field(job, "archive_format", "Parquet")) or "Parquet").lower()
            path = temp_dir / f"{slug((schema + '.' if schema else '') + table)}.{extension}"
            days = int(field(job, "retention_days", 7) or 7)
            if days < 1:
                raise AdapterError("Retention window must be at least one day.")
            cutoff = retention_cutoff(connection, job, schema, table, column, days)
            primary_keys = table_primary_keys(connection, job, schema, table)
            candidate_table = None
            if engine == "postgresql":
                candidate_table, eligible = lock_postgres_candidates(
                    connection,
                    job,
                    run_id,
                    schema,
                    table,
                    column,
                    cutoff,
                    extra_where,
                    primary_keys,
                )
            else:
                column, eligible, cutoff = count_old_rows(
                    connection, job, schema, table, column, extra_where=extra_where
                )
            if not eligible:
                # End the PostgreSQL per-table transaction and release any
                # candidate locks/temp state before moving to the next table.
                connection.commit()
                results.append({"table": reference, "rows": 0, "deleted": 0})
                continue
            content_type, streamed, columns = stream_archive_rows(
                connection,
                job,
                schema,
                table,
                column,
                cutoff,
                path,
                extra_where=extra_where,
                candidate_table=candidate_table,
                primary_keys=primary_keys,
            )
            if streamed != eligible:
                connection.rollback()
                raise AdapterError(
                    f"Archive refused for {reference}: locked identity set has {eligible} row(s) "
                    f"but serialization produced {streamed}. No source rows were deleted."
                )
            key = data_key(job, run_id, schema, table, extension)
            uploaded = upload_to_r2(job, path, key, content_type)
            uploaded.update({"format": extension, "kind": "data", "table": reference, "rows": streamed, "age_column": column, "cutoff": cutoff})
            min_value, max_value = archive_bounds(
                connection,
                job,
                schema,
                table,
                column,
                cutoff,
                extra_where=extra_where,
                candidate_table=candidate_table,
                primary_keys=primary_keys,
            )
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
                    "sha256": uploaded["sha256"],
                    "checksum_algorithm": uploaded["checksum_algorithm"],
                    "verified": uploaded["verified"],
                    "verification_method": uploaded["verification_method"],
                    "verified_at": uploaded["verified_at"],
                },
                columns=columns,
                primary_keys=primary_keys,
                min_value=min_value,
                max_value=max_value,
            )
            manifest_uploaded = upload_manifest(job, manifest, temp_dir)
            if archive_state_callback:
                # If the durable local journal cannot record the ready archive,
                # fail closed before touching source rows.
                archive_state_callback(manifest)
            cursor = connection.cursor()
            try:
                committed_at = dt.datetime.now(dt.timezone.utc).isoformat()
                source_committed_manifest = {
                    **manifest,
                    "status": "source_delete_committed",
                    "deleted_rows": streamed,
                    "source_delete_committed_at": committed_at,
                }
                # The source-side outbox row and DELETE commit atomically. If
                # this process dies immediately after COMMIT, the next run can
                # still promote the already verified R2 manifest safely.
                write_source_outbox(connection, source_committed_manifest)
                if candidate_table:
                    join = postgres_identity_join(primary_keys)
                    cursor.execute(
                        f"DELETE FROM {reference} AS source USING {candidate_table} AS candidate_keys "
                        f"WHERE {join}"
                    )
                else:
                    cursor.execute(
                        f"DELETE FROM {reference} WHERE {quote_identifier(engine, column)} < {sql_placeholder(engine)}{extra_where}",
                        (cutoff,),
                    )
                deleted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else streamed
                if deleted != streamed:
                    raise AdapterError(
                        f"Archive/delete mismatch for {reference}: archived {streamed} row(s) "
                        f"but DELETE matched {deleted}. Transaction rolled back."
                    )
                connection.commit()
            except Exception as error:
                connection.rollback()
                if not is_foreign_key_violation(error):
                    manifest["status"] = "failed"
                    manifest["error_message"] = str(error)[:4000]
                    upload_manifest(job, manifest, temp_dir)
                    if archive_state_callback:
                        archive_state_callback(manifest)
                    raise
                manifest["error_message"] = f"Deletion deferred because newer child rows still reference this table: {str(error)[:240]}"
                # Keep this manifest in ready state. Consumers ignore it while
                # the source row remains hot; a later run can archive it again
                # after its remaining child rows become eligible.
                manifest_uploaded = upload_manifest(job, manifest, temp_dir)
                if archive_state_callback:
                    archive_state_callback(manifest)
                uploaded["kind"] = "data_deferred"
                uploaded["deleted"] = 0
                uploaded["manifest_key"] = manifest["manifest_key"]
                results.extend([
                    uploaded,
                    manifest_uploaded,
                    {
                        "kind": "deferred",
                        "table": reference,
                        "rows": streamed,
                        "deleted": 0,
                        "deferred": True,
                        "age_column": column,
                        "cutoff": cutoff,
                        "message": manifest["error_message"],
                    },
                ])
                continue
            manifest.update(source_committed_manifest)
            if archive_state_callback:
                # This local recovery marker is written after the source commit
                # and before the portable R2 manifest is promoted to committed.
                archive_state_callback(manifest)
            manifest["status"] = "committed"
            manifest["committed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            manifest_uploaded = upload_manifest(job, manifest, temp_dir)
            if archive_state_callback:
                archive_state_callback(manifest)
            delete_source_outbox(connection, manifest["archive_id"])
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
