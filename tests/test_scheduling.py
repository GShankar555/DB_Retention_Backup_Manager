from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scheduling import job_is_due, next_scheduled_run, scheduled_job


class SchedulingTests(unittest.TestCase):
    def test_daily_next_run_uses_job_timezone(self):
        job = {
            "enabled": 1,
            "cadence": "Daily",
            "run_date": "2026-10-01",
            "timezone": "Asia/Kolkata",
            "cron_expression": "0 23 * * *",
        }

        result = next_scheduled_run(job, datetime(2026, 10, 14, 17, 0, tzinfo=timezone.utc))

        self.assertIsNotNone(result)
        self.assertEqual(result.isoformat(), "2026-10-14T23:00:00+05:30")

    def test_biweekly_next_run_respects_anchor_week(self):
        job = {
            "enabled": 1,
            "cadence": "Biweekly",
            "run_date": "2026-10-04",
            "timezone": "UTC",
            "cron_expression": "0 9 * * 0",
        }

        result = next_scheduled_run(job, datetime(2026, 10, 5, tzinfo=timezone.utc))

        self.assertIsNotNone(result)
        self.assertEqual(result.isoformat(), "2026-10-18T09:00:00+00:00")
        self.assertFalse(job_is_due(job, datetime(2026, 10, 11, 9, 0, tzinfo=timezone.utc)))
        self.assertTrue(job_is_due(job, datetime(2026, 10, 18, 9, 0, tzinfo=timezone.utc)))

    def test_disabled_job_has_no_next_run(self):
        job = {
            "enabled": 0,
            "cadence": "Daily",
            "run_date": "2026-10-01",
            "timezone": "UTC",
            "cron_expression": "0 0 * * *",
        }

        self.assertIsNone(next_scheduled_run(job, datetime(2026, 10, 14, tzinfo=timezone.utc)))
        self.assertIsNone(scheduled_job(job, datetime(2026, 10, 14, tzinfo=timezone.utc)))

    def test_invalid_schedule_is_not_presented_as_upcoming(self):
        job = {
            "enabled": 1,
            "cadence": "Daily",
            "run_date": "2026-10-01",
            "timezone": "UTC",
            "cron_expression": "not a cron expression",
        }

        self.assertIsNone(next_scheduled_run(job, datetime(2026, 10, 14, tzinfo=timezone.utc)))


if __name__ == "__main__":
    unittest.main()
