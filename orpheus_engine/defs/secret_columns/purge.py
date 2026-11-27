"""Remove the warehouse copies of inventory credentials, and keep them removed.

Idempotent: it reads the catalog first and changes only what is still there, so
after the first clean run it is a few catalog reads. It never uses CASCADE: a
column that a view depends on is reported, not dropped, so no view vanishes
without someone deciding. See runbooks/secret-columns.md.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Callable, Iterable

import psycopg2
from psycopg2 import errors, sql

from .inventory import DROPPED_COLUMNS, HIDDEN_TABLES, SecretColumn

LOCK_TIMEOUT = "10s"
STATEMENT_TIMEOUT = "120s"

_DROP_CANDIDATES_SQL = """
WITH wanted(schema_name, table_name, column_name) AS (
    SELECT * FROM unnest(%s::text[], %s::text[], %s::text[])
)
SELECT w.schema_name, w.table_name, w.column_name,
       COALESCE((
           SELECT array_agg(DISTINCT v.oid::regclass::text)
           FROM pg_depend d
           JOIN pg_rewrite r ON r.oid = d.objid
           JOIN pg_class v ON v.oid = r.ev_class
           WHERE d.classid = 'pg_rewrite'::regclass
             AND d.refclassid = 'pg_class'::regclass
             AND d.refobjid = c.oid
             AND d.refobjsubid = a.attnum
             AND v.oid <> c.oid
       ), '{}') AS dependent_views
FROM wanted w
JOIN pg_namespace n ON n.nspname = w.schema_name
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = w.table_name
               AND c.relkind IN ('r', 'p')
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = w.column_name
                   AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY 1, 2, 3
"""

_ROW_SECURITY_SQL = """
SELECT c.relrowsecurity, c.relforcerowsecurity,
       (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p')
"""


@dataclass
class PurgeReport:
    dropped: list[str] = field(default_factory=list)
    row_security_enabled: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


def drop_candidates(cur, columns: Iterable[SecretColumn] = DROPPED_COLUMNS):
    """(schema, table, column, dependent views) for inventory columns still present."""
    columns = list(columns)
    cur.execute(
        _DROP_CANDIDATES_SQL,
        (
            [c.schema for c in columns],
            [c.table for c in columns],
            [c.column for c in columns],
        ),
    )
    return [(s, t, col, list(deps or [])) for s, t, col, deps in cur.fetchall()]


def row_security_problem(cur, schema: str, table: str) -> tuple[str | None, bool]:
    """(problem, needs_enable) for a HIDE_ROWS table; a missing table is no problem."""
    cur.execute(_ROW_SECURITY_SQL, (schema, table))
    row = cur.fetchone()
    if row is None:
        return None, False
    enabled, forced, policies = row
    if forced:
        return (
            f"{schema}.{table}: FORCE ROW LEVEL SECURITY is on, which would lock out "
            "its own sync; turn it off",
            False,
        )
    if policies:
        return (
            f"{schema}.{table}: has {policies} row security policy(ies); a policy can "
            "show the rows to another login, so review and remove it",
            False,
        )
    return None, not enabled


def ensure_row_security(cur, schema: str, table: str) -> bool:
    """Enable row-level security with no policy on schema.table, if it is off.

    Returns True when it changed something. The table owner (the sync login)
    still sees every row; pg_read_all_data logins see none. Raises on a FORCE
    or a policy, since either would defeat the purpose or break the sync.
    """
    problem, needs_enable = row_security_problem(cur, schema, table)
    if problem:
        raise RuntimeError(problem)
    if not needs_enable:
        return False
    cur.execute(
        sql.SQL("ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
    )
    return True


def purge(conn, log: Callable[[str], None] = print) -> PurgeReport:
    """Drop present inventory columns and hide HIDE_ROWS tables. conn must autocommit."""
    report = PurgeReport()
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('lock_timeout', %s, false)", (LOCK_TIMEOUT,))
        cur.execute("SELECT set_config('statement_timeout', %s, false)", (STATEMENT_TIMEOUT,))
        for schema, table, column, dependents in drop_candidates(cur):
            name = f"{schema}.{table}.{column}"
            if dependents:
                report.failures.append(
                    f"{name}: not dropped, used by {', '.join(sorted(dependents))}; "
                    "change those views first"
                )
                continue
            try:
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(column)
                    )
                )
            except (errors.LockNotAvailable, errors.QueryCanceled) as error:
                report.failures.append(f"{name}: not dropped, {type(error).__name__}; the next run retries")
                continue
            except psycopg2.Error as error:
                report.failures.append(f"{name}: not dropped, {type(error).__name__}: {error.pgerror or error}")
                continue
            report.dropped.append(name)
            log(f"dropped {name}")

        for schema, table in HIDDEN_TABLES:
            name = f"{schema}.{table}"
            try:
                if ensure_row_security(cur, schema, table):
                    report.row_security_enabled.append(name)
                    log(f"enabled row-level security on {name}")
            except RuntimeError as error:
                report.failures.append(str(error))
            except psycopg2.Error as error:
                report.failures.append(f"{name}: row security not enabled, {type(error).__name__}")
    return report


def connect_warehouse():
    url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not url:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    conn = psycopg2.connect(url, application_name="secret-column-purge")
    conn.autocommit = True
    return conn
