import shutil
import socket
import subprocess
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import psycopg2
import pytest

from orpheus_engine.defs.sling import reader_grant_drift as drift
from orpheus_engine.defs.sling.reader_grant_drift import (
    EXCLUDED_READABLE,
    NOT_CHECKED,
    NOT_GRANTED,
    STALE_STREAM,
    DriftFinding,
    ReaderProgram,
    check_reader_grant_drift,
    warehouse_reader_grant_drift_job,
)
from orpheus_engine.defs.sling.reader_grants import (
    TablePrivileges,
    _column_grant_mismatches,
    _replication_reader_grants,
)
from scripts.provision_warehouse_reader import _render_reader_grant_refresh_sql

READER_URL = "postgresql://hc_warehouse_reader:s3cret-pass@db.example.test:5432/app?sslmode=verify-full"
APP_URL = "postgresql://app_owner:s3cret-pass@db.example.test:5432/app?sslmode=verify-full"

CONFIG = {
    "defaults": {"mode": "full-refresh"},
    "streams": {
        "public.events": {"select": ["-venueAddress"]},
        "public.users": {"select": ["-token"]},
        "public.allowed": {"select": ["id"]},
        "public.whole": None,
        "public.custom": {"sql": "SELECT id FROM public.custom"},
        "public.off": {"select": ["-secret"], "disabled": True},
    },
}


def _tables(**tables):
    return {
        key.replace("__", "."): TablePrivileges(index, columns)
        for index, (key, columns) in enumerate(tables.items())
    }


def _clean_tables():
    return _tables(
        public__events={"id": True, "venueAddress": False},
        public__users={"id": True, "token": False},
        # Out of scope, so none of these can produce a finding.
        public__allowed={"id": True, "other": False},
        public__whole={"id": False},
    )


def test_column_grant_mismatches_compares_kept_with_live():
    live = {"id": True, "new": False, "secret": True}
    assert _column_grant_mismatches(live, ("id", "new")) == (("new",), ("secret",))
    assert _column_grant_mismatches(live, None) == (("new",), ())
    assert _column_grant_mismatches(live, ("id", "secret", "gone")) == (("gone",), ())


def test_compare_clean_program_ignores_streams_without_exclusions():
    assert drift._compare_program("haven", CONFIG, _clean_tables()) == []


def test_compare_reports_each_kind_of_mismatch_without_stopping():
    tables = _tables(
        public__events={"id": True, "googleUsername": False, "venueAddress": True},
    )
    assert drift._compare_program("haven", CONFIG, tables) == [
        DriftFinding("haven", NOT_GRANTED, "public.events.googleUsername"),
        DriftFinding("haven", EXCLUDED_READABLE, "public.events.venueAddress"),
        DriftFinding("haven", STALE_STREAM, "Stream 'public.users' does not exist in source catalog"),
    ]


def test_compare_reports_exclusion_of_a_column_that_is_gone():
    tables = _clean_tables()
    tables["public.users"] = TablePrivileges(9, {"id": True})
    assert drift._compare_program("haven", CONFIG, tables) == [
        DriftFinding(
            "haven", STALE_STREAM, "public.users: excluded column 'token' does not exist in source"
        )
    ]


def test_disabled_col_stream_alone_is_not_drift():
    config = {"streams": {"public.off": {"select": ["-secret"], "disabled": True}}}
    assert drift._col_streams(config) == {}
    assert drift._compare_program("haven", config, {}) == []


def test_col_streams_selects_only_active_exclusions():
    config = {
        **CONFIG,
        "streams": {**CONFIG["streams"], "public.*": {"select": ["-secret"]}},
    }
    assert set(drift._col_streams(config)) == {"public.events", "public.users"}
    assert set(drift._col_streams({"defaults": {"select": ["-x"]}, "streams": {"a.b": None}})) == {"a.b"}


def _fake_program(name, env_var, config=CONFIG, **attrs):
    return SimpleNamespace(
        **{
            f"{name}_db_connection": SimpleNamespace(
                connection_string=SimpleNamespace(env_var_name=env_var)
            ),
            f"{name}_replication_config": config,
        },
        **attrs,
    )


@pytest.fixture
def fake_programs(monkeypatch):
    modules = {
        "haven": _fake_program("haven", "HAVEN_K8S_URL_SLING"),
        "pixl": _fake_program("pixl", "PIXL_K8S_URL"),
        "appdb": _fake_program("appdb", "APPDB_URL"),
        "unset": _fake_program("unset", "UNSET_URL"),
        "allowlist": _fake_program(
            "allowlist", "ALLOWLIST_URL", {"streams": {"public.a": {"select": ["id"]}}}
        ),
        "retired": _fake_program("retired", "RETIRED_URL", __disabled__=True),
        "notmirror": SimpleNamespace(),
    }
    monkeypatch.setattr(
        drift.pkgutil, "iter_modules", lambda _path: [(None, name, False) for name in modules]
    )
    monkeypatch.setattr(
        drift.importlib, "import_module", lambda name: modules[name.rsplit(".", 1)[-1]]
    )
    return modules


def test_discovery_keeps_only_reader_programs_with_col_streams(fake_programs):
    programs, skipped = drift._discover_reader_programs(
        {
            # The program's own variable, not the derived _SLING copy.
            "HAVEN_K8S_URL": READER_URL,
            "HAVEN_K8S_URL_SLING": "postgresql://wrong@wrong/wrong",
            "PIXL_K8S_URL": READER_URL,
            "APPDB_URL": APP_URL,
            "ALLOWLIST_URL": READER_URL,
            "RETIRED_URL": READER_URL,
        }
    )
    assert [p.name for p in programs] == ["haven", "pixl"]
    assert programs[0].url == READER_URL
    assert set(skipped) == {"appdb", "unset", "allowlist"}
    assert "s3cret" not in str(skipped)


def test_one_bad_connection_does_not_stop_the_others(fake_programs, monkeypatch):
    def fetch(url):
        if "bad" in url:
            raise psycopg2.OperationalError(f"could not connect: {url}")
        return _clean_tables()

    monkeypatch.setattr(drift, "_fetch_program_tables", fetch)
    report = check_reader_grant_drift(
        {
            "HAVEN_K8S_URL": READER_URL.replace("db.example", "bad.example"),
            "PIXL_K8S_URL": READER_URL,
        }
    )
    assert report.checked == ["pixl"]
    assert report.findings == [
        DriftFinding("haven", NOT_CHECKED, "connection or catalog query (OperationalError)")
    ]


def _run_job(monkeypatch, report):
    monkeypatch.setattr(drift, "check_reader_grant_drift", lambda: report)
    result = warehouse_reader_grant_drift_job.execute_in_process(raise_on_error=False)
    failures = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    return result, failures


def test_job_fails_with_names_in_the_message_the_runbook_query_reads(monkeypatch):
    report = drift.DriftReport(
        checked=["haven"],
        findings=[DriftFinding("haven", NOT_GRANTED, "public.events.googleUsername")],
    )
    result, failures = _run_job(monkeypatch, report)
    assert not result.success
    error = failures[0].step_failure_data.error
    assert failures[0].step_key == "check_warehouse_reader_grants"
    assert "haven: public.events.googleUsername: kept column not granted" in error.message


@pytest.mark.parametrize(
    "report",
    [
        drift.DriftReport(skipped={"haven": "URL not set"}),
        drift.DriftReport(),
    ],
)
def test_job_fails_when_no_program_was_checked(monkeypatch, report):
    result, failures = _run_job(monkeypatch, report)
    assert not result.success
    assert "No program was checked" in failures[0].step_failure_data.error.message


def test_job_fails_on_real_discovery_with_no_urls(monkeypatch):
    monkeypatch.setattr(drift, "check_reader_grant_drift", lambda real=check_reader_grant_drift: real({}))
    result = warehouse_reader_grant_drift_job.execute_in_process(raise_on_error=False)
    assert not result.success


def test_job_passes_when_clean(monkeypatch):
    report = drift.DriftReport(checked=["haven"], skipped={"other": "URL not set"})
    result, failures = _run_job(monkeypatch, report)
    assert result.success and not failures


def test_failure_text_never_contains_credentials(fake_programs, monkeypatch):
    def fetch(_url):
        raise psycopg2.OperationalError(f"could not connect to {READER_URL}")

    monkeypatch.setattr(drift, "_fetch_program_tables", fetch)
    report = check_reader_grant_drift({"HAVEN_K8S_URL": READER_URL, "APPDB_URL": APP_URL})
    text = str([str(f) for f in report.findings]) + str(report.skipped)
    assert "haven" in text
    for secret in ("s3cret-pass", "db.example.test", "postgresql://", "app_owner"):
        assert secret not in text


def test_job_and_schedule_are_registered():
    from orpheus_engine.definitions import defs

    job = defs.get_job_def("warehouse_reader_grant_drift_job")
    assert job.tags["dagster/max_runtime"]
    schedule = defs.get_schedule_def("warehouse_reader_grant_drift_schedule")
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING
    assert schedule.cron_schedule == "20 6 * * *"


def test_real_programs_are_discovered_and_scoped():
    programs, skipped = drift._discover_reader_programs({})
    assert programs == []
    for name in ("haven", "pixl", "fallen", "attend"):
        assert skipped[name] == "URL not set"
    from orpheus_engine.defs.sling.programs.haven import haven_replication_config

    assert "public.events" in drift._col_streams(haven_replication_config)
    assert "public.tickets" not in drift._col_streams(haven_replication_config)


def _pg_bin(name):
    found = shutil.which(name)
    if found:
        return found
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return str(candidates[0]) if candidates else None


@pytest.fixture
def loopback_postgres(tmp_path):
    """A throwaway server on loopback with synthetic tables; removed with tmp_path."""
    initdb, pg_ctl = _pg_bin("initdb"), _pg_bin("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("local PostgreSQL binaries (initdb/pg_ctl) not installed")
    pgdata = tmp_path / "pgdata"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    subprocess.run(
        [initdb, "-D", str(pgdata), "-U", "postgres", "-A", "trust", "--no-instructions"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            pg_ctl, "-D", str(pgdata), "-l", str(tmp_path / "pg.log"),
            "-o", f"-F -p {port} -h 127.0.0.1 -k '' -c ssl=off", "-w", "start",
        ],
        check=True,
        capture_output=True,
    )
    try:
        yield port
    finally:
        subprocess.run(
            [pg_ctl, "-D", str(pgdata), "-m", "immediate", "stop"], check=False, capture_output=True
        )


def test_real_catalog_query_and_refresh_against_column_grants(loopback_postgres):
    port = loopback_postgres
    admin = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
    admin.autocommit = True
    url = f"postgresql://hc_warehouse_reader@127.0.0.1:{port}/postgres?sslmode=disable"
    config = {
        "streams": {
            "public.events": {"select": ["-venueAddress"]},
            "public.Users": {"select": ["-token"]},
            "public.allowed": {"select": ["id"]},
        }
    }
    with admin.cursor() as cur:
        cur.execute(
            "CREATE ROLE hc_warehouse_reader LOGIN NOINHERIT; "
            'CREATE TABLE public.events (id text, "venueAddress" text); '
            'CREATE TABLE public."Users" (id text, token text); '
            "CREATE TABLE public.allowed (id text, other text); "
            "INSERT INTO public.events VALUES ('row-value', 'row-address');"
        )

    def apply_refresh():
        source = {
            key: list(table.columns)
            for key, table in drift._fetch_program_tables(url).items()
        }
        grants = _replication_reader_grants(config, source)
        with admin.cursor() as cur:
            cur.execute(_render_reader_grant_refresh_sql(grants))

    def findings():
        return drift._compare_program("demo", config, drift._fetch_program_tables(url))

    apply_refresh()
    assert findings() == []

    # An app migration adds a column on a -col table and on an allow-list table.
    with admin.cursor() as cur:
        cur.execute(
            'ALTER TABLE public.events ADD COLUMN "googleUsername" text; '
            "ALTER TABLE public.allowed ADD COLUMN later text;"
        )
    assert findings() == [DriftFinding("demo", NOT_GRANTED, "public.events.googleUsername")]

    # Someone grants an excluded column, on a mixed-case table too.
    with admin.cursor() as cur:
        cur.execute('GRANT SELECT (token) ON public."Users" TO hc_warehouse_reader')
    assert DriftFinding("demo", EXCLUDED_READABLE, "public.Users.token") in findings()

    # The refresh fixes both and the same login still works.
    apply_refresh()
    assert findings() == []

    with admin.cursor() as cur:
        cur.execute("DROP TABLE public.events")
    assert findings() == [
        DriftFinding("demo", STALE_STREAM, "Stream 'public.events' does not exist in source catalog")
    ]
    admin.close()


def test_check_session_refuses_writes_and_has_timeouts(loopback_postgres, monkeypatch):
    """Runs on the session _fetch_program_tables opens, not on one the test sets up."""
    port = loopback_postgres
    admin = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        # The login may create temp tables, so only the read-only session can refuse this.
        cur.execute("CREATE ROLE hc_warehouse_reader LOGIN")
    url = f"postgresql://hc_warehouse_reader@127.0.0.1:{port}/postgres?sslmode=disable"
    probed = {}

    def probe_session(cur):
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            cur.execute("CREATE TEMP TABLE should_fail (id int)")
        cur.execute("SHOW statement_timeout")
        probed["statement_timeout"] = cur.fetchone()[0]
        probed["connect_timeout"] = cur.connection.info.dsn_parameters["connect_timeout"]
        probed["autocommit"] = cur.connection.autocommit
        return {}

    monkeypatch.setattr(drift, "_fetch_catalog_privileges", probe_session)
    assert drift._fetch_program_tables(url) == {}
    assert probed == {
        "statement_timeout": drift.STATEMENT_TIMEOUT,
        "connect_timeout": str(drift.CONNECT_TIMEOUT_SECONDS),
        "autocommit": True,
    }
    admin.close()


def test_full_check_against_real_server_reports_real_connection_failure(
    loopback_postgres, monkeypatch
):
    port = loopback_postgres
    admin = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(
            "CREATE ROLE hc_warehouse_reader LOGIN; "
            "CREATE TABLE public.events (id text, secret text); "
            "GRANT SELECT (id) ON public.events TO hc_warehouse_reader;"
        )
    admin.close()
    config = {"streams": {"public.events": {"select": ["-secret"]}}}
    good = f"postgresql://hc_warehouse_reader@127.0.0.1:{port}/postgres?sslmode=disable"
    bad = f"postgresql://hc_warehouse_reader@127.0.0.1:{port}/no_such_db?sslmode=disable"
    monkeypatch.setattr(
        drift,
        "_discover_reader_programs",
        lambda _env: ([ReaderProgram("good", good, config), ReaderProgram("bad", bad, config)], {}),
    )
    report = check_reader_grant_drift({})
    assert report.checked == ["good"]
    assert [f.kind for f in report.findings] == [NOT_CHECKED]
    assert "no_such_db" not in str(report.findings[0])
