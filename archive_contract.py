"""Versioned, project-agnostic cold-archive manifest contract.

The manager writes data objects first and a manifest with ``status=committed``
only after the source deletion has committed. Consumers should read committed
manifests only. The manifest is intentionally self-describing so a project
does not need access to Vaultline's SQLite database to read cold data.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any


ARCHIVE_CONTRACT_VERSION = 1


def safe_name(value: Any, fallback: str = "unknown") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("-._")
    return cleaned[:96] or fallback


def namespace(job: Any) -> str:
    configured = str(field(job, "archive_namespace", "")).strip()
    return safe_name(configured or field(job, "connection_name", "database"), "database")


def field(row: Any, name: str, default: Any = "") -> Any:
    try:
        value = row[name]
    except (KeyError, IndexError, TypeError):
        value = default
    return default if value is None else value


def archive_id(job: Any, run_id: int, schema: str | None, table: str) -> str:
    identity = "|".join([
        namespace(job),
        str(field(job, "engine", "")),
        str(field(job, "database_name", "")),
        schema or "",
        table,
        str(run_id),
    ])
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def archive_prefix(job: Any, run_id: int, schema: str | None, table: str) -> str:
    return "/".join([
        "archives",
        f"v{ARCHIVE_CONTRACT_VERSION}",
        namespace(job),
        safe_name(field(job, "connection_name", "database")),
        safe_name(field(job, "database_name", "database")),
        safe_name(schema or "public"),
        safe_name(table),
        f"run-{run_id}",
    ])


def manifest_key(job: Any, run_id: int, schema: str | None, table: str) -> str:
    return f"{archive_prefix(job, run_id, schema, table)}/manifest.json"


def data_key(job: Any, run_id: int, schema: str | None, table: str, extension: str) -> str:
    extension = extension.lower().lstrip(".")
    return f"{archive_prefix(job, run_id, schema, table)}/data.{extension}"


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes, dict, list, tuple)):
        try:
            return value.isoformat()
        except (AttributeError, ValueError):
            pass
    return value


def schema_hash(columns: list[str]) -> str:
    return hashlib.sha256(json.dumps(columns, sort_keys=True).encode("utf-8")).hexdigest()


def build_manifest(
    job: Any,
    run_id: int,
    schema: str | None,
    table: str,
    *,
    status: str,
    age_column: str,
    cutoff: Any,
    row_count: int,
    deleted_rows: int,
    data_object: dict[str, Any],
    columns: list[str],
    primary_keys: list[str] | None = None,
    min_value: Any = None,
    max_value: Any = None,
    error_message: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "contract_version": ARCHIVE_CONTRACT_VERSION,
        "archive_id": archive_id(job, run_id, schema, table),
        "status": status,
        "namespace": namespace(job),
        "engine": str(field(job, "engine", "")),
        "database_name": str(field(job, "database_name", "")),
        "connection_name": str(field(job, "connection_name", "")),
        "schema_name": schema or "public",
        "table_name": table,
        "age_column": age_column,
        "cutoff_value": json_value(cutoff),
        "min_value": json_value(min_value),
        "max_value": json_value(max_value),
        "row_count": int(row_count),
        "deleted_rows": int(deleted_rows),
        "format": str(field(job, "archive_format", "Parquet")).lower(),
        "columns": columns,
        "primary_keys": primary_keys or [],
        "schema_hash": schema_hash(columns),
        "data_objects": [data_object],
        "manifest_key": manifest_key(job, run_id, schema, table),
        "created_at": now,
        "committed_at": now if status == "committed" else None,
        "error_message": error_message[:4000],
    }
