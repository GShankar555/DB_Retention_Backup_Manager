"""Resolve service credentials from the process environment before legacy SQLite values."""

from __future__ import annotations

import os
from typing import Any


def value(record: Any, key: str, default: str = "") -> str:
    try:
        item = record[key]
    except (KeyError, IndexError, TypeError):
        item = default
    return str(item or default)


def connection_password(connection: Any) -> str:
    connection_id = value(connection, "connection_id") or value(connection, "id")
    if connection_id and connection_id.isdecimal():
        specific = os.getenv(f"VAULTLINE_CONNECTION_{connection_id}_PASSWORD")
        if specific is not None:
            return specific
    if (value(connection, "database_name") == os.getenv("DB_NAME")
            and value(connection, "username") == os.getenv("DB_USER")):
        source_password = os.getenv("DB_PASSWORD")
        if source_password is not None:
            return source_password
    return value(connection, "password")


def r2_credentials(job: Any) -> tuple[str, str]:
    job_id = value(job, "id")
    access = secret = None
    if job_id.isdecimal():
        access = os.getenv(f"VAULTLINE_JOB_{job_id}_R2_ACCESS_KEY")
        secret = os.getenv(f"VAULTLINE_JOB_{job_id}_R2_SECRET_KEY")
    if (value(job, "r2_account_id") == os.getenv("ARCHIVE_R2_ACCOUNT_ID")
            and value(job, "r2_bucket") == os.getenv("ARCHIVE_R2_BUCKET")):
        if access is None:
            access = os.getenv("ARCHIVE_R2_ACCESS_KEY")
        if secret is None:
            secret = os.getenv("ARCHIVE_R2_SECRET_KEY")
    return (access if access is not None else value(job, "r2_access_key"),
            secret if secret is not None else value(job, "r2_secret_key"))
