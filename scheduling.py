"""Shared helpers for evaluating and displaying Vaultline job schedules."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, CroniterBadDateError, croniter


def _value(job: Mapping[str, Any], key: str, default: Any = None) -> Any:
    try:
        value = job[key]
    except (KeyError, IndexError):
        return default
    return default if value is None else value


def _as_utc(value: datetime | None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _biweekly_candidate_is_valid(job: Mapping[str, Any], candidate: datetime) -> bool:
    if _value(job, "cadence") != "Biweekly":
        return True
    anchor = datetime.strptime(str(_value(job, "run_date", "")), "%Y-%m-%d").date()
    return candidate.date() >= anchor and (candidate.date() - anchor).days % 14 == 0


def job_is_due(job: Mapping[str, Any], now_utc: datetime) -> bool:
    """Return whether an enabled job should run during the supplied UTC minute."""
    if not bool(_value(job, "enabled", True)):
        return False
    zone = ZoneInfo(str(_value(job, "timezone", "UTC") or "UTC"))
    local = _as_utc(now_utc).astimezone(zone).replace(second=0, microsecond=0)
    return croniter.match(
        str(_value(job, "cron_expression", "")), local, precision_in_seconds=1
    ) and _biweekly_candidate_is_valid(job, local)


def next_scheduled_run(
    job: Mapping[str, Any], now_utc: datetime | None = None
) -> datetime | None:
    """Return the next scheduled occurrence in the job's configured timezone."""
    if not bool(_value(job, "enabled", True)):
        return None
    try:
        zone = ZoneInfo(str(_value(job, "timezone", "UTC") or "UTC"))
        local_now = _as_utc(now_utc).astimezone(zone)
        iterator = croniter(str(_value(job, "cron_expression", "")), local_now)
        # Biweekly cron expressions fire weekly and use run_date as their
        # two-week anchor. Other cadences return on the first iteration.
        for _ in range(1000):
            candidate = iterator.get_next(datetime)
            if _biweekly_candidate_is_valid(job, candidate):
                return candidate
    except (
        CroniterBadCronError,
        CroniterBadDateError,
        TypeError,
        ValueError,
        ZoneInfoNotFoundError,
    ):
        return None
    return None


def scheduled_job(job: Mapping[str, Any], now_utc: datetime | None = None) -> dict[str, Any] | None:
    """Return a template-friendly job dictionary containing its next run."""
    next_run = next_scheduled_run(job, now_utc)
    if next_run is None:
        return None
    result = dict(job)
    result["next_run"] = next_run
    result["next_run_utc"] = next_run.astimezone(timezone.utc)
    result["next_run_label"] = next_run.strftime("%d %b %Y · %H:%M %Z").lstrip("0")
    return result
