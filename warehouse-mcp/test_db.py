"""db.py against a real login, on a temporary Postgres on loopback with fake data.

Run it from the repo root. It needs the root environment for the login's real SQL, and
the tool's psycopg, so a plain root `pytest` skips this file:

    uv run --extra dev --with 'psycopg[binary]==3.3.4' --with psycopg-pool==3.3.1 \
        pytest warehouse-mcp/test_db.py

The versions are the ones in warehouse-mcp/uv.lock. The Dockerfile copies named files,
so this file never ships.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("psycopg_pool")

import psycopg
import psycopg2
from psycopg import errors

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db as warehouse_db
from orpheus_engine_tests.test_warehouse_agent_login import server  # noqa: F401  (fixture)
from scripts import provision_warehouse_agent_login as provision

# Run through _run: validate_sql refuses pg_sleep.
SLOW_QUERY = "SELECT pg_sleep(1), 'finished' AS marker"
SPILLS_TO_DISK = (
    "SELECT count(*) FROM (SELECT g FROM generate_series(1, 2000000) g ORDER BY g DESC OFFSET 0) s"
)


@pytest.fixture(scope="module", autouse=True)
def catalog_data(server):
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE synced.with_default "
            "(id serial PRIMARY KEY, note text DEFAULT 'fallback', made timestamptz DEFAULT now())"
        )
        cur.execute("INSERT INTO synced.with_default (note) VALUES ('a')")


def _on_database(url, monkeypatch, use):
    """Run use(db) on a real pool for this login."""
    monkeypatch.setenv("WAREHOUSE_COOLIFY_URL", url)

    async def run():
        database = warehouse_db.Database()
        await database.open()
        try:
            return await use(database)
        finally:
            await database.close()

    return asyncio.run(run())


async def _catalog_answers(database):
    return {
        "schemas": await database.list_schemas(),
        "columns": await database.list_columns("synced", "with_default"),
        "describe": await database.describe_schema("synced"),
    }


def test_catalog_tools_answer_the_same_as_under_a_superuser(server, monkeypatch):
    as_superuser = _on_database(server.admin_url(), monkeypatch, _catalog_answers)
    as_login = _on_database(server.mcp_url(), monkeypatch, _catalog_answers)
    assert as_login == as_superuser
    assert "synced" in as_login["schemas"]
    assert "synced.with_default" in as_login["describe"]
    assert "'fallback'" in str(as_login["columns"]), "column_default must stay visible"


def test_the_tool_logs_in_as_its_own_limited_login(server, monkeypatch):
    async def use(database):
        rows, _ = await database.execute_query(
            "SELECT current_user AS who, rolsuper, current_setting('lock_timeout') AS lock_timeout "
            "FROM pg_roles WHERE rolname = current_user"
        )
        return rows[0]

    assert _on_database(server.mcp_url(), monkeypatch, use) == {
        "who": provision.MCP_ROLE, "rolsuper": False, "lock_timeout": "30s",
    }


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE synced.before_role SET name = 'x'",
        "DROP TABLE synced.before_role",
        "CREATE TABLE synced.nope (id int)",
        "COPY (SELECT 1) TO PROGRAM 'true'",
        "SELECT pg_read_file('postgresql.conf', 0, 10)",
    ],
)
def test_privileges_refuse_what_the_blocklist_would_miss(server, statement):
    url = warehouse_db.make_readonly_url(server.mcp_url())
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SET default_transaction_read_only = off")
        with pytest.raises(errors.InsufficientPrivilege):
            conn.execute(statement)


@pytest.fixture
def tight_limits(server):
    """Small enough for a test to hit: prod's values take 30 seconds and 50GB."""
    role = provision.MCP_ROLE
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER ROLE {role} SET lock_timeout = '200ms'")
        cur.execute(f"ALTER ROLE {role} SET temp_file_limit = '1MB'")
    yield
    with server.admin() as conn, conn.cursor() as cur:
        for name in ("lock_timeout", "temp_file_limit"):
            cur.execute(f"ALTER ROLE {role} SET {name} = %s", (provision.ROLE_SETTINGS[name],))


def _retries(caplog) -> int:
    return sum("retrying" in record.getMessage() for record in caplog.records)


def test_lock_timeout_is_not_retried(server, monkeypatch, caplog, tight_limits):
    blocker = server.admin_connection(autocommit=False)
    try:
        with blocker.cursor() as cur:
            cur.execute("LOCK TABLE synced.before_role IN ACCESS EXCLUSIVE MODE")

        async def use(database):
            await database.execute_query("SELECT count(*) FROM synced.before_role")

        with pytest.raises(errors.LockNotAvailable):
            _on_database(server.mcp_url(), monkeypatch, use)
    finally:
        blocker.rollback()
        blocker.close()
    assert _retries(caplog) == 0


def test_temp_file_limit_is_not_retried(server, monkeypatch, caplog, tight_limits):
    async def use(database):
        await database.execute_query(SPILLS_TO_DISK)

    with pytest.raises(errors.ConfigurationLimitExceeded):
        _on_database(server.mcp_url(), monkeypatch, use)
    assert _retries(caplog) == 0


def _signal_running_query(server, function: str) -> None:
    """From the agent login, signal the query tool's running query."""
    conn = psycopg2.connect(server.agent_url())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for _ in range(200):
                cur.execute(
                    "SELECT pid FROM pg_stat_activity WHERE usename = %s "
                    "AND state = 'active' AND query LIKE '%%pg_sleep%%'",
                    (provision.MCP_ROLE,),
                )
                row = cur.fetchone()
                if row:
                    cur.execute(f"SELECT {function}(%s)", row)
                    assert cur.fetchone() == (True,)
                    return
                time.sleep(0.02)
        raise AssertionError("the query never started")
    finally:
        conn.close()


def test_cancelled_query_is_not_retried(server, monkeypatch, caplog):
    async def use(database):
        query = asyncio.create_task(database._run(SLOW_QUERY, None, 10))
        await asyncio.to_thread(_signal_running_query, server, "pg_cancel_backend")
        await query

    with pytest.raises(errors.QueryCanceled):
        _on_database(server.mcp_url(), monkeypatch, use)
    assert _retries(caplog) == 0


def test_terminated_query_is_silently_run_again(server, monkeypatch, caplog):
    """Pins why the runbook says to cancel a query-tool session and not terminate it."""

    async def use(database):
        query = asyncio.create_task(database._run(SLOW_QUERY, None, 10))
        await asyncio.to_thread(_signal_running_query, server, "pg_terminate_backend")
        return await query

    rows, _ = _on_database(server.mcp_url(), monkeypatch, use)
    assert rows[0]["marker"] == "finished"
    assert _retries(caplog) == 1


@pytest.mark.parametrize(
    "error, lost",
    [
        (psycopg.OperationalError("consuming input failed: unexpected eof"), True),
        (errors.AdminShutdown("x"), True),
        (errors.CrashShutdown("x"), True),
        (errors.CannotConnectNow("x"), True),
        (errors.ConnectionFailure("x"), True),
        (errors.QueryCanceled("x"), False),
        (errors.LockNotAvailable("x"), False),
        (errors.ConfigurationLimitExceeded("x"), False),
        (errors.DiskFull("x"), False),
    ],
)
def test_only_a_lost_connection_counts(error, lost):
    assert warehouse_db.is_connection_lost(error) is lost
