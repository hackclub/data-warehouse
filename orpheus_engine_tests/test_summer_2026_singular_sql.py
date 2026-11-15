"""Compile and execute every Summer singular test on synthetic PostgreSQL data.

Run with uv run --extra dev pytest orpheus_engine_tests/test_summer_2026_singular_sql.py.
Docker is required. No warehouse credentials or production connections are used.
Data-test findings are allowed here; invalid SQL is not, even at warn severity.
"""
import json
import os
from pathlib import Path
import subprocess
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "orpheus_engine_dbt"


@pytest.fixture(scope="module")
def compiled_tests(tmp_path_factory):
    directory = tmp_path_factory.mktemp("dbt")
    container = subprocess.check_output([
        "docker", "run", "-d", "--rm", "-p", "127.0.0.1::5432",
        "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17-alpine",
    ], text=True).strip()
    try:
        for _ in range(60):
            if subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        else:
            pytest.fail("Fixture PostgreSQL did not become ready")

        def sql(statement):
            result = subprocess.run([
                "docker", "exec", "-i", container, "psql", "-h", "127.0.0.1", "-U", "postgres",
                "-v", "ON_ERROR_STOP=1", "-At",
            ], input=statement, text=True, capture_output=True)
            assert result.returncode == 0, result.stderr
            return result.stdout.strip()

        sql((ROOT / "orpheus_engine_tests/fixtures/summer_2026/singular_tests.sql").read_text())
        port = subprocess.check_output([
            "docker", "port", container, "5432/tcp",
        ], text=True).strip().rsplit(":", 1)[1]
        (directory / "profiles.yml").write_text(f"""orpheus_engine_dbt:
  target: fixture
  outputs:
    fixture:
      type: postgres
      host: 127.0.0.1
      port: {port}
      user: postgres
      password: fixture
      dbname: postgres
      schema: public
      threads: 1
""")
        result = subprocess.run([
            "dbt", "compile", "--project-dir", str(PROJECT),
            "--profiles-dir", str(directory), "--target-path", str(directory / "target"),
            "--log-path", str(directory / "logs"), "--no-partial-parse",
            "--select", "path:tests/summer_2026*",
        ], env={**os.environ, "HACK_CLUBBERS_LOCATION_SALT": "synthetic-fixture",
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}, text=True, capture_output=True)
        assert result.returncode == 0, result.stdout + result.stderr
        manifest = json.loads((directory / "target/manifest.json").read_text())
        tests = {node["name"]: node["compiled_code"]
                 for node in manifest["nodes"].values()
                 if node["resource_type"] == "test"
                 and node["original_file_path"].startswith("tests/summer_2026")}
        assert set(tests) == {path.stem for path in (PROJECT / "tests").glob("summer_2026*.sql")}
        yield tests, sql
    finally:
        subprocess.run(["docker", "stop", container], check=True, capture_output=True)


def test_every_summer_singular_test_executes(compiled_tests):
    tests, sql = compiled_tests
    for name, query in sorted(tests.items()):
        try:
            sql(query)
        except AssertionError as error:
            pytest.fail(f"{name}: {error}")


def test_half_life_warns_on_eastern_user_day(compiled_tests):
    tests, sql = compiled_tests
    # The UTC date changes between these rows, but the Eastern date does not.
    # Split sub-threshold entries must sum to the threshold on the Eastern day.
    query = tests["summer_2026_half_life_active_day_has_marker"]
    assert sql(f"SELECT user_email || ':' || activity_date FROM ({query}) findings") == "missing@example.invalid:2026-01-02"
