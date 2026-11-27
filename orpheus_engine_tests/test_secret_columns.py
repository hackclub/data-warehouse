"""The recorded inventory of credential columns, and what keeps each one unreadable.

Config tests: every EXCLUDE_AT_SOURCE column is left out by its live sync, so a
full refresh cannot bring it back. Local Postgres tests (skipped without
initdb/pg_ctl): the purge job drops the copies, refuses to break a view, and hides
the QuickBooks token from the query tool's login while its owner still reads it;
and the Hackatime id reconciliation finds a row stranded below the watermark.
Everything runs on a throwaway loopback server with fake data.
"""

from __future__ import annotations

import importlib
import shutil
import socket
import subprocess
from collections import Counter
from contextlib import closing
from pathlib import Path

import psycopg2
import pytest

from orpheus_engine.defs.dlt.assets import AIRTABLE_SECRET_DROP_COLUMNS
from orpheus_engine.defs.secret_columns import inventory
from orpheus_engine.defs.secret_columns.inventory import (
    DLT_AIRTABLE,
    DROP_FROZEN_COPY,
    EXCLUDE_AT_SOURCE,
    HIDE_ROWS,
    SECRET_COLUMNS,
    SLING,
    TREATMENTS,
)
from orpheus_engine.defs.secret_columns.purge import purge
from orpheus_engine.defs.sling import assets as sling_assets
from scripts import provision_warehouse_agent_login as provision
from scripts.provision_warehouse_reader import _scram_sha256_verifier

# Schemas whose Sling config lives in sling/assets.py rather than programs/<schema>.py.
_CORE_CONFIGS = {
    "hackatime": "hackatime_replication_config",
    "hcer_public_github_data": "hcer_public_github_data_replication_config",
    "hcb": "hcb_replication_config",
}

# dlt loads an Airtable list field into a child table <table>__<field>(value).
_DLT_CHILD_TABLES = {("airtable_milkyway", "users__otp", "value"): ("users", "otp")}


def _sling_config(schema: str):
    if schema in _CORE_CONFIGS:
        return getattr(sling_assets, _CORE_CONFIGS[schema])
    module = importlib.import_module(f"orpheus_engine.defs.sling.programs.{schema}")
    assert not getattr(module, "__disabled__", False), f"{schema} mirror is disabled"
    return getattr(module, f"{schema}_replication_config")


def _stream_excludes(stream_options, column: str) -> bool:
    """True when the stream cannot copy `column`."""
    options = stream_options or {}
    if options.get("disabled"):
        return True
    if "sql" in options:
        return column not in options["sql"]
    select = options.get("select")
    if not select:
        return False
    if all(c.startswith("-") for c in select):
        return f"-{column}" in select
    return column not in select


# --- The inventory itself ---


def test_inventory_entries_are_unique_and_well_formed():
    counts = Counter(c.qualified for c in SECRET_COLUMNS)
    assert [name for name, n in counts.items() if n > 1] == []
    for column in SECRET_COLUMNS:
        assert column.treatment in TREATMENTS, column
        assert column.kind, column
        if column.treatment == EXCLUDE_AT_SOURCE:
            assert column.sync in (SLING, DLT_AIRTABLE), column
        else:
            assert column.sync is None, column


def test_the_contract_columns_are_in_the_inventory():
    names = {c.qualified for c in SECRET_COLUMNS}
    for required in (
        "hackatime.api_keys.token",
        "hackatime.oauth_access_tokens.token",
        "hackatime.oauth_access_tokens.refresh_token",
        "hackatime.oauth_access_grants.token",
        "hackatime.sign_in_tokens.token",
        "hackatime.admin_api_keys.token",
        "hcer_public_github_data.access_tokens.access_token",
        "quickbooks._qbo_oauth.refresh_token",
    ):
        assert required in names, required


def test_only_the_quickbooks_token_is_hidden_rather_than_dropped():
    hidden = [c.qualified for c in SECRET_COLUMNS if c.treatment == HIDE_ROWS]
    assert hidden == ["quickbooks._qbo_oauth.refresh_token"]
    assert inventory.HIDDEN_TABLES == (("quickbooks", "_qbo_oauth"),)


# --- Live syncs leave every EXCLUDE_AT_SOURCE column out ---


@pytest.mark.parametrize(
    "column",
    [c for c in SECRET_COLUMNS if c.sync == SLING],
    ids=lambda c: c.qualified,
)
def test_sling_config_excludes_inventory_column(column):
    config = _sling_config(column.schema)
    stream = f"public.{column.table}"
    streams = config["streams"]
    # A wildcard would copy the table with every column, so the table must be
    # named explicitly with its exclusion.
    assert stream in streams, f"{stream} must be listed explicitly to exclude {column.column}"
    assert _stream_excludes(streams[stream], column.column), column.qualified


@pytest.mark.parametrize(
    "column",
    [c for c in SECRET_COLUMNS if c.sync == DLT_AIRTABLE],
    ids=lambda c: c.qualified,
)
def test_dlt_airtable_drops_inventory_column(column):
    base = column.schema.removeprefix("airtable_")
    table, field = _DLT_CHILD_TABLES.get(
        (column.schema, column.table, column.column), (column.table, column.column)
    )
    assert field in AIRTABLE_SECRET_DROP_COLUMNS[base][table], column.qualified


def test_frozen_copies_have_no_live_sling_mirror():
    for column in SECRET_COLUMNS:
        if column.treatment != DROP_FROZEN_COPY:
            continue
        path = Path(sling_assets.__file__).parent / "programs" / f"{column.schema}.py"
        if path.exists():
            module = importlib.import_module(f"orpheus_engine.defs.sling.programs.{column.schema}")
            assert getattr(module, "__disabled__", False), (
                f"{column.schema} has a live mirror; use EXCLUDE_AT_SOURCE"
            )
        assert column.schema not in _CORE_CONFIGS, column


def test_hackatime_streams_exclude_tokens_and_indexes_skip_them():
    streams = sling_assets.hackatime_replication_config["streams"]
    assert streams["public.users"]["select"] == [
        "-github_access_token", "-hca_access_token", "-slack_access_token",
    ]
    assert streams["public.users"]["mode"] == "incremental"
    assert streams["public.oauth_access_tokens"]["select"] == [
        "-token", "-refresh_token", "-previous_refresh_token",
    ]
    # Unchanged streams keep their plain incremental config.
    assert "select" not in streams["public.heartbeats"]
    for schema, table, columns in sling_assets._hackatime_app_index_specs():
        excluded = set(inventory.excluded_columns(schema, table))
        assert not excluded & set(columns), (table, columns)


def test_stardance_daily_full_refresh_keeps_the_exclusion():
    from orpheus_engine.defs.sling.programs.stardance import _stardance_full_refresh_config

    full = _stardance_full_refresh_config()
    assert full["streams"]["public.lookout_sessions"]["select"] == ["-token"]


# --- Hackatime missing-id reconciliation: pure logic ---


def test_missing_ids_finds_ids_below_the_warehouse_max():
    assert sling_assets._missing_ids([1, 2, 3, 4, 5], [1, 2, 4, 5]) == [3]
    assert sling_assets._missing_ids([1, 2], [1, 2, 3]) == []


def test_backfill_config_upserts_by_id_without_secret_columns():
    config = sling_assets._hackatime_backfill_config(
        "users",
        "id",
        [1003, 12],
        ["id", "username", "github_access_token", "hca_access_token", "slack_access_token", "updated_at"],
    )
    stream = config["streams"]["public.users"]
    assert config["source"] == "HACKATIME_DB" and config["target"] == "WAREHOUSE_DB"
    assert stream["mode"] == "incremental"
    assert stream["primary_key"] == ["id"]
    assert "update_key" not in stream  # merge every returned row, ignore the watermark
    assert stream["object"] == "hackatime.users"
    assert stream["sql"] == (
        'select "id", "username", "updated_at" from public."users" where "id" in (1003, 12)'
    )
    assert "token" not in stream["sql"]


def test_backfill_config_rejects_non_integer_ids():
    with pytest.raises((TypeError, ValueError)):
        sling_assets._hackatime_backfill_config("users", "id", ["1); drop table x; --"], ["id"])


# --- Local Postgres ---


def _pg_bin(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return str(candidates[0]) if candidates else None


class Server:
    def __init__(self, port: int):
        self.port = port

    def connect(self, user: str, dbname: str = "warehouse"):
        conn = psycopg2.connect(host="127.0.0.1", port=self.port, user=user, dbname=dbname)
        conn.autocommit = True
        return conn


SYNC_ROLE = "fake_sync"  # stands in for the non-superuser sync login that owns the tables


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    initdb, pg_ctl = _pg_bin("initdb"), _pg_bin("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("local PostgreSQL binaries (initdb/pg_ctl) not installed")
    root = tmp_path_factory.mktemp("pg")
    pgdata = root / "pgdata"
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    subprocess.run(
        [initdb, "-D", str(pgdata), "-U", "postgres", "-A", "trust", "--no-instructions"],
        check=True, capture_output=True,
    )
    options = f"-F -p {port} -h 127.0.0.1 -k '' -c ssl=off"
    subprocess.run(
        [pg_ctl, "-D", str(pgdata), "-l", str(root / "log"), "-o", options, "-w", "start"],
        check=True, capture_output=True,
    )
    try:
        running = Server(port)
        with closing(running.connect("postgres", "postgres")) as conn, conn.cursor() as cur:
            cur.execute("CREATE DATABASE warehouse")
            cur.execute("CREATE DATABASE hackatime_source")
        with closing(running.connect("postgres")) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE ROLE {SYNC_ROLE} LOGIN NOSUPERUSER")
            cur.execute(f"GRANT CREATE ON DATABASE warehouse TO {SYNC_ROLE}")
            # The real role SQL for both limited logins (pg_read_all_data and all).
            agent, mcp = provision.LOGINS["agent"], provision.LOGINS["mcp"]
            cur.execute(provision.render_setup_sql(agent, _scram_sha256_verifier("agent-test")))
            cur.execute(provision.render_setup_sql(mcp, _scram_sha256_verifier("mcp-test")))
        yield running
    finally:
        subprocess.run([pg_ctl, "-D", str(pgdata), "-m", "immediate", "-w", "stop"], capture_output=True)


def _columns(cur, schema, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


@pytest.fixture(scope="module")
def synced(server):
    """Tables as the syncs leave them today, owned by the sync login."""
    with closing(server.connect(SYNC_ROLE)) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA hackatime")
        cur.execute(
            "CREATE TABLE hackatime.api_keys (id int PRIMARY KEY, user_id int, name text, "
            "token text, created_at timestamp)"
        )
        cur.execute("CREATE INDEX ON hackatime.api_keys (token)")
        cur.execute("INSERT INTO hackatime.api_keys VALUES (1, 1, 'editor', 'fake-key', now())")
        cur.execute(
            "CREATE TABLE hackatime.users (id int PRIMARY KEY, username text, "
            "github_access_token text, hca_access_token text, slack_access_token text, "
            "updated_at timestamp)"
        )
        cur.execute("INSERT INTO hackatime.users VALUES (1, 'a', 'fake-gh', 'fake-hca', 'fake-slack', now())")
        cur.execute("CREATE SCHEMA hcer_public_github_data")
        cur.execute(
            "CREATE TABLE hcer_public_github_data.access_tokens (id int, username text, access_token text)"
        )
        cur.execute("INSERT INTO hcer_public_github_data.access_tokens VALUES (1, 'a', 'fake-ghu')")
        # A view on a secret column: the purge must not drop it out from under the view.
        cur.execute("CREATE SCHEMA fabricate")
        cur.execute("CREATE TABLE fabricate.users (id int, session_token text, slack_user_token text)")
        cur.execute("CREATE VIEW fabricate.user_tokens AS SELECT id, session_token FROM fabricate.users")
        cur.execute("CREATE SCHEMA quickbooks")
        from orpheus_engine.defs.quickbooks_online.definitions import TOKEN_DDL

        cur.execute(TOKEN_DDL)
        cur.execute("INSERT INTO quickbooks._qbo_oauth VALUES ('client', 'fake-refresh', now())")
    return server


def test_query_tool_login_reads_tokens_before_the_purge(synced):
    # The finding: pg_read_all_data reads every mirrored credential.
    with closing(synced.connect(provision.MCP_ROLE)) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(token) FROM hackatime.api_keys")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(refresh_token) FROM quickbooks._qbo_oauth")
        assert cur.fetchone()[0] == 1


def test_column_revoke_does_not_hide_from_pg_read_all_data(synced):
    # Why the QuickBooks token uses row security: a column REVOKE is not enough.
    with closing(synced.connect(SYNC_ROLE)) as conn, conn.cursor() as cur:
        cur.execute(f"REVOKE SELECT (refresh_token) ON quickbooks._qbo_oauth FROM {provision.MCP_ROLE}")
    with closing(synced.connect(provision.MCP_ROLE)) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(refresh_token) FROM quickbooks._qbo_oauth")
        assert cur.fetchone()[0] == 1


def test_purge_drops_copies_hides_quickbooks_and_spares_views(synced):
    with closing(synced.connect(SYNC_ROLE)) as conn:
        report = purge(conn, log=lambda _: None)
        with conn.cursor() as cur:
            assert _columns(cur, "hackatime", "api_keys") == ["id", "user_id", "name", "created_at"]
            assert _columns(cur, "hackatime", "users") == ["id", "username", "updated_at"]
            assert _columns(cur, "hcer_public_github_data", "access_tokens") == ["id", "username"]
            # slack_user_token has no view on it, session_token does.
            assert _columns(cur, "fabricate", "users") == ["id", "session_token"]
            # The owner, the sync login, still reads and writes the token.
            cur.execute("SELECT count(refresh_token) FROM quickbooks._qbo_oauth")
            assert cur.fetchone()[0] == 1
            cur.execute("UPDATE quickbooks._qbo_oauth SET refresh_token = 'fake-rotated'")
            assert cur.rowcount == 1
    assert "hackatime.api_keys.token" in report.dropped
    assert "hcer_public_github_data.access_tokens.access_token" in report.dropped
    assert report.row_security_enabled == ["quickbooks._qbo_oauth"]
    assert len(report.failures) == 1
    assert "fabricate.users.session_token" in report.failures[0]
    assert "fabricate.user_tokens" in report.failures[0]

    for role in (provision.MCP_ROLE, provision.AGENT_ROLE):
        with closing(synced.connect(role)) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), count(refresh_token) FROM quickbooks._qbo_oauth")
            assert cur.fetchone() == (0, 0)
            cur.execute("SELECT count(*) FROM hackatime.users")
            assert cur.fetchone()[0] == 1  # the rest of the data is still readable

    # Idempotent: a second run changes nothing and reports the same view problem.
    with closing(synced.connect(SYNC_ROLE)) as conn:
        again = purge(conn, log=lambda _: None)
    assert again.dropped == [] and again.row_security_enabled == []
    assert again.failures == report.failures


def test_quickbooks_table_setup_turns_row_security_on(server):
    from orpheus_engine.defs.quickbooks_online import definitions as qbo

    with closing(server.connect(SYNC_ROLE)) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS quickbooks CASCADE")
    conn = server.connect(SYNC_ROLE)
    conn.autocommit = False
    try:
        qbo.ensure_schema_and_tables(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO quickbooks._qbo_oauth VALUES ('client', 'fake-refresh', now())")
            cur.execute("SELECT relrowsecurity FROM pg_class WHERE oid = 'quickbooks._qbo_oauth'::regclass")
            assert cur.fetchone()[0] is True
        conn.commit()
        qbo.ensure_schema_and_tables(conn)  # again: no error, no change
    finally:
        conn.close()
    with closing(server.connect(provision.MCP_ROLE)) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(refresh_token) FROM quickbooks._qbo_oauth")
        assert cur.fetchone()[0] == 0


def test_purge_reports_a_policy_on_the_hidden_table(server):
    with closing(server.connect(SYNC_ROLE)) as conn, conn.cursor() as cur:
        cur.execute(
            f"CREATE POLICY leak ON quickbooks._qbo_oauth FOR SELECT TO {provision.MCP_ROLE} USING (true)"
        )
        try:
            report = purge(conn, log=lambda _: None)
        finally:
            cur.execute("DROP POLICY leak ON quickbooks._qbo_oauth")
    assert any("quickbooks._qbo_oauth" in f and "policy" in f for f in report.failures)


def test_reconciliation_finds_a_row_below_the_watermark(server):
    source = f"postgresql://postgres@127.0.0.1:{server.port}/hackatime_source"
    warehouse = f"postgresql://postgres@127.0.0.1:{server.port}/warehouse"
    with closing(server.connect("postgres", "hackatime_source")) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE public.users (id int PRIMARY KEY, username text, updated_at timestamp)")
        cur.execute(
            "INSERT INTO public.users VALUES "
            "(1, 'a', '2026-11-20'), (2, 'b', '2026-11-26'), (3, 'c', '2026-10-07'), (4, 'd', '2026-11-27')"
        )
    with closing(server.connect("postgres")) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS hackatime")
        cur.execute("CREATE TABLE hackatime.users_reconcile (id int, username text, updated_at timestamp)")
        # The warehouse copy as an updated_at cursor leaves it: id 3's updated_at is
        # below the max the warehouse already holds, so the next incremental run
        # (updated_at > max) never selects it.
        cur.execute(
            "INSERT INTO hackatime.users_reconcile VALUES "
            "(1, 'a', '2026-11-20'), (2, 'b', '2026-11-26'), (4, 'd', '2026-11-27')"
        )
        cur.execute("SELECT max(updated_at) FROM hackatime.users_reconcile")
        watermark = cur.fetchone()[0]
    with closing(server.connect("postgres", "hackatime_source")) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM public.users WHERE updated_at > %s", (watermark,))
        assert cur.fetchall() == []  # what the incremental stream would copy

    missing = sling_assets._missing_ids(
        sling_assets._fetch_ids(source, "public", "users", "id"),
        sling_assets._fetch_ids(warehouse, "hackatime", "users_reconcile", "id"),
    )
    assert missing == [3]
    assert sling_assets._fetch_source_columns(source, "users") == ["id", "username", "updated_at"]


def test_verify_script_reports_what_the_query_tool_login_can_read(server):
    from scripts import verify_secret_columns

    with closing(server.connect(SYNC_ROLE)) as conn, conn.cursor() as cur:
        # The view kept fabricate.users.session_token in place (see the purge test).
        cur.execute("INSERT INTO fabricate.users VALUES (1, 'fake-session')")
    with closing(server.connect(provision.MCP_ROLE)) as conn:
        login, results, failures = verify_secret_columns.check(conn)
    assert login == provision.MCP_ROLE
    by_name = dict(results)
    assert by_name["hackatime.api_keys.token"] == "absent"
    assert by_name["quickbooks._qbo_oauth.refresh_token"] == "0 non-null readable"
    assert failures == [
        f"fabricate.users.session_token: 1 non-null values readable as {provision.MCP_ROLE}"
    ]
    assert len(results) == len(SECRET_COLUMNS)
    assert "fake-" not in repr(results) + repr(failures)  # names and counts only


def test_runbook_lists_exactly_the_inventory():
    runbook = (Path(__file__).resolve().parents[1] / "runbooks" / "secret-columns.md").read_text()
    inventory_section = runbook.split("## Inventory", 1)[1]
    import re

    listed = re.findall(r"^\| `([^`]+)` \|", inventory_section, re.MULTILINE)
    assert sorted(listed) == sorted(c.qualified for c in SECRET_COLUMNS)
