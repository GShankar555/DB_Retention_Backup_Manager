from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("VAULTLINE_ENV", "test")

from app import app, get_db


class ConnectionTests(unittest.TestCase):
    def test_new_connection_form_accepts_and_saves_password(self):
        with tempfile.TemporaryDirectory() as directory:
            original_database = app.config["DATABASE"]
            app.config["DATABASE"] = Path(directory) / "connections-test.db"
            try:
                client = app.test_client()
                with client.session_transaction() as session:
                    session["authenticated"] = True
                    session["username"] = "vaultline@admin"
                    session["csrf_token"] = "connection-test-token"

                page = client.get("/connections")
                self.assertEqual(page.status_code, 200)
                self.assertIn(
                    'name="password" type="password" autocomplete="new-password"',
                    page.get_data(as_text=True),
                )

                response = client.post(
                    "/connections",
                    data={
                        "csrf_token": "connection-test-token",
                        "name": "reporting",
                        "engine": "PostgreSQL",
                        "host": "db.internal",
                        "port": "5432",
                        "database_name": "reporting_db",
                        "username": "reporter",
                        "password": "database-secret",
                        "ssl_mode": "require",
                    },
                )
                self.assertEqual(response.status_code, 302)

                with app.app_context():
                    saved = get_db().execute(
                        "SELECT password FROM connections WHERE name = ?", ("reporting",)
                    ).fetchone()
                    self.assertIsNotNone(saved)
                    self.assertEqual(saved["password"], "database-secret")
            finally:
                app.config["DATABASE"] = original_database


if __name__ == "__main__":
    unittest.main()
