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
        (errors.ProtocolViolation("x"), False),
    ],
)
def test_only_a_lost_connection_counts(error, lost):
    assert warehouse_db.is_connection_lost(error) is lost


def test_attribution_comment_stays_single_line_and_safe():
    comment = warehouse_db._format_attribution_comment("Ada", "Lovelace")
    assert comment == "/* warehouse-mcp Ada Lovelace */"
    assert "\n" not in comment

    # Unicode letters, apostrophes, hyphens, periods, and digits are preserved.
    assert (
        warehouse_db._format_attribution_comment("José", "O'Brien-Jr. 2")
        == "/* warehouse-mcp José O'Brien-Jr. 2 */"
    )

    # Length is capped.
    long_comment = warehouse_db._format_attribution_comment("A" * 200, "B" * 200)
    assert len(long_comment) <= len("/* warehouse-mcp  */") + warehouse_db.MAX_ATTRIBUTION_NAME_LENGTH

    # Adversarial inputs cannot close or nest block comments, inject newlines/NULs, or escape.
    adversarial = [
        ("**//", "test"),
        ("/**/", "test"),
        ("*/", "/*"),
        ("Ada\n*/DROP", "/*Lovelace\r"),
        ("Bad\x00Name", "End\x00"),
        ("**//set default_transaction_read_only=off; commit; create table injected(); select 2 as ok", "--"),
    ]
    for first, last in adversarial:
        out = warehouse_db._format_attribution_comment(first, last)
        assert out.startswith("/* warehouse-mcp") and out.endswith(" */")
        inner = out[2:-2]
        assert "/*" not in inner
        assert "*/" not in inner
        assert "\n" not in inner and "\r" not in inner and "\x00" not in inner
        assert "*" not in inner and "/" not in inner and ";" not in inner


def test_execute_query_with_user_info_blocks_comment_escape_injection(server, monkeypatch):
    exploit_first = (
        "**//set default_transaction_read_only=off; commit; "
        "create table injected(); select 2 as ok"
    )

    async def use(database):
        rows, _ = await database.execute_query(
            "SELECT /* marker_mcp_single_line */ 1 AS ok",
            user_info=(exploit_first, "--"),
        )
        check_rows, _ = await database.execute_query(
            "SELECT to_regclass('injected') AS tbl"
        )
        return rows[0], check_rows[0]

    result, check = _on_database(server.mcp_url(), monkeypatch, use)
    assert result == {"ok": 1}
    assert check == {"tbl": None}


def test_freshness_is_found_and_read_through_sql_alone(server, monkeypatch):
    """No freshness tool: list_schemas shows warehouse_metadata and query reads the views."""
    from datetime import datetime, timedelta, timezone

    from orpheus_engine.defs.freshness.definitions import AssetFreshnessSpec, write_mirror_freshness

    now = datetime.now(timezone.utc)
    specs = [
        AssetFreshnessSpec("stardance", "stardance_warehouse_mirror", "sling", "frequent_15min_schedule", 15, 60),
        AssetFreshnessSpec("hcb", "hcb_warehouse_mirror", "sling", "materialize_all_assets", 360, 720),
    ]
    with server.admin() as conn:
        write_mirror_freshness(
            conn,
            specs,
            {"stardance_warehouse_mirror": now - timedelta(minutes=5), "hcb_warehouse_mirror": now - timedelta(hours=24)},
            recorded_at=now,
        )

    async def use(database):
        schemas = await database.list_schemas()
        rows, _ = await database.execute_query(
            "SELECT schema_name, status, is_stale FROM warehouse_metadata.mirror_freshness "
            "WHERE schema_name IN ('stardance', 'hcb') ORDER BY schema_name"
        )
        doc, _ = await database.execute_query(
            "SELECT obj_description('warehouse_metadata.mirror_freshness'::regclass, 'pg_class') AS doc"
        )
        return schemas, rows, doc[0]["doc"]

    schemas, rows, doc = _on_database(server.mcp_url(), monkeypatch, use)
    assert "warehouse_metadata" in schemas
    assert rows == [
        {"schema_name": "hcb", "status": "stale", "is_stale": True},
        {"schema_name": "stardance", "status": "fresh", "is_stale": False},
    ]
    assert "deleted at the source" in doc


def test_describe_schema_shows_the_freshness_views_and_their_comments(server, monkeypatch):
    """describe_schema, the step list_schemas points to, lists the views with their docs."""
    from datetime import datetime, timezone

    from orpheus_engine.defs.freshness.definitions import (
        FRESHNESS_TABLE,
        AssetFreshnessSpec,
        write_mirror_freshness,
    )

    now = datetime.now(timezone.utc)
    spec = AssetFreshnessSpec("stardance", "stardance_warehouse_mirror", "sling", "frequent_15min_schedule", 15, 60)
    with server.admin() as conn:
        write_mirror_freshness(conn, [spec], {"stardance_warehouse_mirror": now}, recorded_at=now)

    async def use(database):
        return await database.describe_schema("warehouse_metadata")

    described = _on_database(server.mcp_url(), monkeypatch, use)
    assert "## warehouse_metadata.mirror_freshness (view)" in described
    assert "## warehouse_metadata.asset_freshness (view)" in described
    assert f"## {FRESHNESS_TABLE}\n" in described
    assert "deleted at the source" in described
    assert "\n- status: " in described, "column comments must sit next to their column"


@pytest.mark.parametrize(
    "statement",
    [
        "SET statement_timeout = 0; SELECT 1",
        "SET default_transaction_read_only = off; SELECT 1",
        "SET ROLE postgres",
        "SELECT pg_sleep_for('1 second')",
        "SELECT pg_sleep_until(now() + interval '1 second')",
        "SELECT pg_ls_dir('.')",
        "SELECT pg_stat_file('postgresql.conf')",
        "SELECT lo_from_bytea(0, '\\x61')",
        "SELECT 'a--b'\n; SET statement_timeout = 0; SELECT 1",
        "-- don't skip next line\nSET statement_timeout = 0; SELECT 1",
        "SELECT /* /* */ 1 */ ; DROP TABLE x",
        # "$" continues an identifier, so x$a$ is not a dollar quote.
        "SELECT 1 AS x$a$; SET statement_timeout = 0; SELECT 1 AS y$a$",
        "SELECT 1 AS x$a$; SELECT pg_sleep(1); SELECT 1 AS y$a$",
        "SELECT 1 AS x$a$; COMMIT; SELECT 1 AS y$a$",
        "SELECT 1 AS \u00e9$a$; SET statement_timeout = 0; SELECT 1 AS \u00e9$a$",
        "SELECT 1 AS x_$a$ FROM t WHERE $a$ = $a$ OR pg_sleep(1) IS NULL",
        # A -- comment ends at a carriage return too.
        "SELECT 1 -- note\rSET statement_timeout = 0",
        # E only starts an escape string at the start of a token.
        "SELECT xe'\\' AS a; SET statement_timeout = 0; SELECT '\\'",
        "SELECT 1; SELECT 2",
        "COMMIT",
        "END",
        "BEGIN",
        "ROLLBACK",
        "ABORT",
        "START TRANSACTION READ WRITE",
        "SAVEPOINT s",
        "RELEASE SAVEPOINT s",
        "SELECT 1; COMMIT",
        # Quoted, qualified and escaped spellings of a forbidden function.
        'SELECT "pg_sleep"(0.3)',
        'SELECT pg_catalog."pg_sleep"(0.3)',
        'SELECT "pg_catalog" . /* x */ "pg_sleep"(0.3)',
        'SELECT U&"\\0070g_sleep"(0.3)',
        "SELECT U&\"!0070g_sleep\" UESCAPE '!' (0.3)",
        "SELECT u&'\\0070g_sleep'",
        "SELECT \"pg_terminate_backend\"(pid) FROM pg_stat_activity WHERE usename = 'x'",
        "SELECT pg_catalog.\"pg_cancel_backend\"(1)",
        "SELECT * FROM \"pg_sleep\"(1)",
        "SELECT * FROM pg_catalog.pg_sleep(1)",
        "SELECT PG_SLEEP(1)",
        "SELECT \"set_config\"('statement_timeout', '0', false)",
        "SELECT \"lo_from_bytea\"(0, 'abc'::bytea)",
        # Functions that run a query given as a string.
        "SELECT query_to_xml('select pg_sleep(1)', true, false, '')",
        "SELECT \"query_to_xml\"('select 1', true, false, '')",
        "SELECT * FROM ts_stat('select 1')",
        "SELECT ts_rewrite('a'::tsquery, 'SELECT ''a''::tsquery, ''b''::tsquery FROM (SELECT pg_sleep(1.5)) s')",
        "SELECT pg_catalog.\"ts_rewrite\"('a'::tsquery, $q$SELECT 'a'::tsquery, 'b'::tsquery$q$)",
        "SELECT TS_REWRITE('a'::tsquery, 'select 1')",
        "SELECT table_to_xml('t'::regclass, true, false, '')",
        "SELECT schema_to_xml('public', true, false, '')",
        "SELECT database_to_xml(true, false, '')",
        "SELECT \"table_to_xml_and_xmlschema\"('t'::regclass, true, false, '')",
        # Positional parameters have no values in this tool.
        "SELECT $1",
        "SELECT * FROM t WHERE id = $1",
    ],
)
def test_validate_sql_blocks_session_override_and_sleep_variants(statement):
    with pytest.raises(warehouse_db.SQLValidationError):
        warehouse_db.validate_sql(statement)


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT 1 -- don't break on apostrophe in comment\n",
        "SELECT /* outer /* inner DROP TABLE x */ comment */ 1",
        "SELECT E'it\\'s a DROP test'",
        "SELECT $$DROP TABLE x; SET statement_timeout=0$$",
        "SELECT $a$DROP; SET x$a$ AS ok",
        "SELECT $a$; SET$a$ AS v",
        'SELECT "set" FROM t',
        "SELECT x$a$ FROM t;",
        "SELECT CASE WHEN true THEN 1 ELSE 0 END AS flag;\n",
        "SELECT 1; -- trailing comment",
        "SELECT 'a;b' AS semicolon_in_string",
        # Names are case-sensitive when quoted: "PG_SLEEP" is not pg_sleep.
        'SELECT "PG_SLEEP" FROM t',
        "SELECT 'pg_sleep(1)' AS text_only",
        "SELECT 'pg_sleep'::regproc",
        'SELECT "my""col" FROM t',
        "SELECT x$1 FROM t",
    ],
)
def test_validate_sql_allows_safe_comments_strings_and_quoted_identifiers(statement):
    warehouse_db.validate_sql(statement)


def test_lexer_matches_postgres_on_dollar_quotes(server):
    """Ask PostgreSQL how it splits the text: one string literal, or code."""
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("SELECT $a$; SET$a$ AS v")
        assert cur.fetchone() == ("; SET",)
        cur.execute("SELECT 1 AS x$a$")
        assert [d.name for d in cur.description] == ["x$a$"]


def test_postgres_calls_functions_through_quoted_and_escaped_names(server):
    """Why the guard must resolve quoted and U& names: PostgreSQL runs them."""
    with server.admin() as conn, conn.cursor() as cur:
        for sql in (
            'SELECT "pg_sleep"(0)',
            'SELECT pg_catalog."pg_sleep"(0)',
            'SELECT U&"\\0070g_sleep"(0)',
            "SELECT U&\"!0070g_sleep\" UESCAPE '!' (0)",
        ):
            cur.execute(sql)
            assert cur.fetchone() is not None
            with pytest.raises(warehouse_db.SQLValidationError):
                warehouse_db.validate_sql(sql)


def test_postgres_runs_sql_inside_ts_rewrite_text(server):
    """Why ts_rewrite is forbidden: PostgreSQL executes its text argument."""
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("CREATE SEQUENCE IF NOT EXISTS synced.ts_rewrite_probe")
        cur.execute("SELECT nextval('synced.ts_rewrite_probe')")
        before = cur.fetchone()[0]
        cur.execute(
            "SELECT ts_rewrite('a'::tsquery, "
            "'SELECT ''a''::tsquery, ''b''::tsquery FROM (SELECT nextval(''synced.ts_rewrite_probe'')) s')"
        )
        cur.execute("SELECT last_value FROM synced.ts_rewrite_probe")
        after = cur.fetchone()[0]
        assert after == before + 1
    with pytest.raises(warehouse_db.SQLValidationError):
        warehouse_db.validate_sql("SELECT ts_rewrite('a'::tsquery, 'SELECT 1')")


def test_a_parameter_error_is_not_retried(server, monkeypatch, caplog):
    async def use(database):
        await database._run("SELECT $1", None, 10)

    with pytest.raises(errors.ProtocolViolation):
        _on_database(server.mcp_url(), monkeypatch, use)
    assert _retries(caplog) == 0


def test_session_changes_never_reach_the_next_query(server, monkeypatch):
    """Server side, without validate_sql: one pooled connection, two callers."""

    async def use(database):
        database._pool._max_size = 1
        before = (await database._run("SHOW statement_timeout", None, 10))[0][0]
        await database._run("SELECT set_config('statement_timeout', '0', false)", None, 10)
        await database._run("SET statement_timeout = 0", None, 10)
        with pytest.raises(psycopg.Error):
            await database._run("SET statement_timeout = 0; SELECT 1", None, 10)
        with pytest.raises(psycopg.Error):
            await database._run("COMMIT; SET statement_timeout = 0; SELECT 1", None, 10)
        await database._run("COMMIT", None, 10)
        await database._run("SELECT pg_advisory_lock(42)", None, 10)
        after = (await database._run("SHOW statement_timeout", None, 10))[0][0]
        ro = (await database._run("SHOW transaction_read_only", None, 10))[0][0]
        locks = (await database._run(
            "SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
            None, 10,
        ))[0][0]
        return before, after, ro, locks

    before, after, ro, locks = _on_database(server.mcp_url(), monkeypatch, use)
    assert after["statement_timeout"] == before["statement_timeout"] != "0"
    assert ro["transaction_read_only"] == "on"
    assert locks["n"] == 0


def test_query_runs_in_a_read_only_transaction(server, monkeypatch):
    async def use(database):
        with pytest.raises(errors.ReadOnlySqlTransaction):
            await database._run("SELECT lo_from_bytea(0, 'abc'::bytea)", None, 10)
        rows, columns = await database._run(
            "SELECT current_setting('transaction_read_only') AS ro WHERE %s", (True,), 10
        )
        return rows, columns

    assert _on_database(server.mcp_url(), monkeypatch, use) == ([{"ro": "on"}], ["ro"])


def test_temp_tables_cannot_be_written_even_with_read_only_mode_off(server, monkeypatch):
    """Server side, without validate_sql: the temp-table disk-fill path stays shut.

    The login keeps TEMP on the database (through PUBLIC), so only the READ ONLY,
    always-rolled-back transaction stops a caller from filling disk with temp tables.
    """

    async def use(database):
        database._pool._max_size = 1
        # Read-only mode off inside the query's own transaction, then a temp write.
        for sql in (
            "SELECT 1 AS a INTO TEMP TABLE filler",
            "CREATE TEMP TABLE filler AS SELECT g FROM generate_series(1, 10) g",
            "SET default_transaction_read_only = off",
        ):
            try:
                await database._run(sql, None, 10)
            except psycopg.Error:
                pass
        with pytest.raises(errors.ReadOnlySqlTransaction):
            await database._run("CREATE TEMP TABLE filler AS SELECT 1 AS a", None, 10)
        rows, _ = await database._run(
            "SELECT count(*) AS n FROM pg_class WHERE relpersistence = 't'", None, 10
        )
        return rows[0]["n"]

    assert _on_database(server.mcp_url(), monkeypatch, use) == 0
