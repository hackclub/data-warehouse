"""
Daily read-only check that hc_warehouse_reader's column grants still match the
replication configs (runbooks/prod-health-monitoring.md §9).

A `-col` stream asks Postgres for every column the reader can see, so a column
an app migration adds after the grants were made is left out of the warehouse
and the mirror stays green. This job compares the grants derived from each
config with has_column_privilege and fails, naming programs, tables and columns,
so the drift shows up in the §2 failure query. It never grants anything: someone
first decides whether the new column is sensitive.

Only `-col` streams are checked. An allow-list stream names its columns, so a new
column cannot change what it copies. Whole-table and sql: streams are out of scope.
"""

import importlib
import os
import pkgutil
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import urlparse

import dagster as dg

from . import programs as programs_pkg
from .reader_grants import (
    READER_ROLE,
    _column_grant_mismatches,
    _connect_pg,
    _fetch_catalog_privileges,
    _replication_reader_grants,
)

CONNECT_TIMEOUT_SECONDS = 15
STATEMENT_TIMEOUT = "30s"

NOT_GRANTED = "kept column not granted, so it is missing from the warehouse"
EXCLUDED_READABLE = "excluded column is readable by the reader"
STALE_STREAM = "config no longer matches the source"
NOT_CHECKED = "could not check"


@dataclass(frozen=True)
class ReaderProgram:
    name: str
    url: str
    replication_config: Mapping[str, Any]


@dataclass(frozen=True)
class DriftFinding:
    program: str
    kind: str
    detail: str

    def __str__(self) -> str:
        return f"{self.program}: {self.detail}: {self.kind}"


@dataclass
class DriftReport:
    checked: list[str] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    findings: list[DriftFinding] = field(default_factory=list)


def _source_env_var(connection: Any) -> str | None:
    """The program's own env var; _sling_connection_url's derived _SLING copy
    drops sslrootcert=system, which psycopg2 needs."""
    env_var_name = getattr(connection.connection_string, "env_var_name", None)
    return env_var_name.removesuffix("_SLING") if env_var_name else None


def _url_login(url: str) -> str | None:
    try:
        return urlparse(url).username
    except ValueError:
        return None


def _col_streams(replication_config: Mapping[str, Any]) -> dict[str, Any]:
    """The active fixed-name streams whose select has a `-col` exclusion."""
    defaults = replication_config.get("defaults") or {}
    selected: dict[str, Any] = {}
    for stream_name, stream_config in (replication_config.get("streams") or {}).items():
        merged = {**defaults, **(stream_config or {})}
        if "*" in stream_name or merged.get("disabled") or merged.get("sql") is not None:
            continue
        if any(str(col).startswith("-") for col in merged.get("select") or ()):
            selected[stream_name] = stream_config
    return selected


def _discover_reader_programs(
    environ: Mapping[str, str],
) -> tuple[list[ReaderProgram], dict[str, str]]:
    """Enabled program modules that log in as the reader and have `-col` streams,
    plus why each other program was skipped. Credentials never appear in reasons."""
    found: list[ReaderProgram] = []
    skipped: dict[str, str] = {}
    for _, name, _ in pkgutil.iter_modules(programs_pkg.__path__):
        module = importlib.import_module(f"{programs_pkg.__name__}.{name}")
        connection = getattr(module, f"{name}_db_connection", None)
        config = getattr(module, f"{name}_replication_config", None)
        if getattr(module, "__disabled__", False) or connection is None or config is None:
            continue
        env_var = _source_env_var(connection)
        url = environ.get(env_var) if env_var else None
        if not url:
            skipped[name] = "URL not set"
        elif _url_login(url) != READER_ROLE:
            skipped[name] = f"does not log in as {READER_ROLE}"
        elif not _col_streams(config):
            skipped[name] = "no -col streams"
        else:
            found.append(ReaderProgram(name, url, config))
    return found, skipped


def _compare_program(
    program: str,
    replication_config: Mapping[str, Any],
    tables: Mapping[str, Any],
) -> list[DriftFinding]:
    """Compare one program's `-col` streams with its live catalog, one stream at
    a time so a stale stream does not hide drift in the others."""
    defaults = replication_config.get("defaults") or {}
    source_columns = {key: list(table.columns) for key, table in tables.items()}
    findings: list[DriftFinding] = []
    for stream_name, stream_config in _col_streams(replication_config).items():
        try:
            grants = _replication_reader_grants(
                {"defaults": defaults, "streams": {stream_name: stream_config}},
                source_columns,
            )
        except ValueError as exc:
            findings.append(DriftFinding(program, STALE_STREAM, str(exc)))
            continue
        not_granted, readable = _column_grant_mismatches(
            tables[stream_name].columns, grants[stream_name]
        )
        findings.extend(
            DriftFinding(program, NOT_GRANTED, f"{stream_name}.{col}") for col in not_granted
        )
        findings.extend(
            DriftFinding(program, EXCLUDED_READABLE, f"{stream_name}.{col}") for col in readable
        )
    return findings


READS_EVERY_COLUMN = "reads every column (pg_read_all_data), so only the -col lists exclude columns"


class ReadsEveryColumn(Exception):
    """The login is a member of pg_read_all_data: column grants cannot narrow it,
    so comparing them would report every excluded column as drift."""


def _fetch_program_tables(url: str) -> dict[str, Any]:
    conn = _connect_pg(url, connect_timeout=CONNECT_TIMEOUT_SECONDS)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (STATEMENT_TIMEOUT,))
            cur.execute("SELECT pg_has_role(current_user, 'pg_read_all_data', 'MEMBER')")
            if cur.fetchone()[0]:
                raise ReadsEveryColumn()
            return _fetch_catalog_privileges(cur)
    finally:
        conn.close()


def check_reader_grant_drift(environ: Mapping[str, str] | None = None) -> DriftReport:
    """Check every reader program; one program's failure does not stop the rest."""
    programs, skipped = _discover_reader_programs(os.environ if environ is None else environ)
    report = DriftReport(skipped=skipped)
    for program in programs:
        try:
            tables = _fetch_program_tables(program.url)
        except ReadsEveryColumn:
            report.skipped[program.name] = READS_EVERY_COLUMN
            continue
        except Exception as exc:  # noqa: BLE001 - reported per program below
            # Only the type: driver messages can carry the host and login.
            report.findings.append(
                DriftFinding(program.name, NOT_CHECKED, f"connection or catalog query ({type(exc).__name__})")
            )
            continue
        report.checked.append(program.name)
        report.findings.extend(_compare_program(program.name, program.replication_config, tables))
    return report


@dg.op
def check_warehouse_reader_grants(context: dg.OpExecutionContext) -> None:
    report = check_reader_grant_drift()
    for name, reason in sorted(report.skipped.items()):
        context.log.info(f"Skipped {name}: {reason}")
    context.log.info(f"Checked {len(report.checked)} programs: {', '.join(report.checked)}")

    problems = [str(finding) for finding in report.findings]
    if not report.checked:
        problems.insert(
            0,
            f"No program was checked: {len(report.skipped)} skipped, so the {READER_ROLE} "
            "URLs may not have loaded.",
        )
    if problems:
        summary = "\n".join(problems)
        context.log.error(summary)
        raise dg.Failure(
            description=(
                f"hc_warehouse_reader grant drift, {len(problems)} problem(s). "
                f"See runbooks/prod-health-monitoring.md section 9.\n{summary}"
            )
        )


@dg.job(
    name="warehouse_reader_grant_drift_job",
    tags={"dagster/max_runtime": "1800"},  # 30 min; each program is bounded by timeouts
)
def warehouse_reader_grant_drift_job():
    check_warehouse_reader_grants()


# Runs daily at 06:20 ET, after the janitor (05:45) and offset from the :30
# all-assets and :10/:15 sync ticks.
warehouse_reader_grant_drift_schedule = dg.ScheduleDefinition(
    name="warehouse_reader_grant_drift_schedule",
    job=warehouse_reader_grant_drift_job,
    cron_schedule="20 6 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
