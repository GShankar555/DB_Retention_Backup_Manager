from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("VAULTLINE_ENV", "test")

from app import app, get_db, init_db


class DashboardScheduleTests(unittest.TestCase):
    def test_overview_uses_earliest_enabled_schedule_instead_of_newest_job(self):
        with tempfile.TemporaryDirectory() as directory:
            original_database = app.config["DATABASE"]
            app.config["DATABASE"] = Path(directory) / "dashboard-test.db"
            try:
                with app.app_context():
                    init_db()
                    db = get_db()
                    connection_id = db.execute(
                        """INSERT INTO connections
                           (name, engine, host, port, database_name, username, password)
                           VALUES ('test', 'PostgreSQL', 'localhost', 5432, 'source', 'user', '')"""
                    ).lastrowid
                    for name, enabled in (("Soon", 1), ("Later", 1), ("Disabled newest", 0)):
                        db.execute(
                            """INSERT INTO jobs
                               (name, job_type, connection_id, cadence, run_date, run_time,
                                timezone, cron_expression, enabled)
                               VALUES (?, 'backup', ?, 'Daily', '2001-01-01', '00:00',
                                       'UTC', '0 0 * * *', ?)""",
                            (name, connection_id, enabled),
                        )
                    db.commit()

                def fake_schedule(job):
                    if not job["enabled"]:
                        return None
                    hour = 1 if job["name"] == "Soon" else 2
                    result = dict(job)
                    result["next_run"] = datetime(2026, 10, 15, hour, tzinfo=timezone.utc)
                    result["next_run_utc"] = result["next_run"]
                    result["next_run_label"] = f"15 Oct 2026 · 0{hour}:00 UTC"
                    return result

                client = app.test_client()
                with client.session_transaction() as session:
                    session["authenticated"] = True
                    session["username"] = "vaultline@admin"
                with patch("app.scheduled_job", side_effect=fake_schedule):
                    response = client.get("/")

                body = response.get_data(as_text=True)
                self.assertEqual(response.status_code, 200)
                self.assertIn(
                    "<small>Next scheduled job</small><strong>15 Oct 2026 · 01:00 UTC</strong>",
                    body,
                )
                self.assertNotIn(
                    "<small>Next scheduled job</small><strong>2001-01-01</strong>",
                    body,
                )
                queue_start = body.index("Enabled jobs ordered by next run")
                self.assertLess(body.index("Soon", queue_start), body.index("Later", queue_start))
            finally:
                app.config["DATABASE"] = original_database


if __name__ == "__main__":
    unittest.main()
