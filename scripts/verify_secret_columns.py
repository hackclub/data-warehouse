#!/usr/bin/env python3
"""Check, as a given login, that no inventory credential column is readable.

    PYTHONPATH=. .venv/bin/python scripts/verify_secret_columns.py --env-file <file>

The file holds WAREHOUSE_COOLIFY_URL for the login to check (use the query tool's,
hc_warehouse_mcp, for the contract check). For each column in
orpheus_engine/defs/secret_columns/inventory.py it reports `absent` (dropped), or the
number of non-null values that login can read. Output is names and counts only,
never values. Exit 0 when every column is absent or reads 0 non-null values.

Render the one-time admin SQL for columns the sync login cannot drop (tables
another login owns), read-only, to a 0600 file for bin/warehouse-admin-psql -f:

    PYTHONPATH=. .venv/bin/python scripts/verify_secret_columns.py --env-file <file> \\
        --render-admin-sql <scratch.sql> --sync-role hc_warehouse_dagster

See runbooks/secret-columns.md.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

import psycopg2
from psycopg2 import sql

from orpheus_engine.defs.secret_columns.inventory import SECRET_COLUMNS
from orpheus_engine.defs.secret_columns.purge import (
    drop_candidates,
    not_owned_columns,
    render_admin_drop_sql,
)
from scripts.provision_warehouse_reader import _parse_env_text

URL_ENV_VAR = "WAREHOUSE_COOLIFY_URL"


def check(conn) -> tuple[str, list[tuple[str, str]], list[str]]:
    """(login, [(column, result)], failures) for every inventory column."""
    results: list[tuple[str, str]] = []
    failures: list[str] = []
    with conn.cursor() as cur:
        cur.execute("SELECT current_user")
        login = cur.fetchone()[0]
        present = {(s, t, c) for s, t, c, _ in drop_candidates(cur, SECRET_COLUMNS)}
        for column in SECRET_COLUMNS:
            key = (column.schema, column.table, column.column)
            if key not in present:
                results.append((column.qualified, "absent"))
                continue
            try:
                cur.execute(
                    sql.SQL("SELECT count({}) FROM {}.{}").format(
                        sql.Identifier(column.column),
                        sql.Identifier(column.schema),
                        sql.Identifier(column.table),
                    )
                )
                readable = cur.fetchone()[0]
            except psycopg2.errors.InsufficientPrivilege:
                conn.rollback()
                results.append((column.qualified, "permission denied"))
                continue
            results.append((column.qualified, f"{readable} non-null readable"))
            if readable:
                failures.append(f"{column.qualified}: {readable} non-null values readable as {login}")
    return login, results, failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--render-admin-sql", type=Path, help="write the admin DROP SQL here")
    parser.add_argument("--sync-role", help="the login the purge job runs as")
    args = parser.parse_args(argv)
    if bool(args.render_admin_sql) != bool(args.sync_role):
        parser.error("--render-admin-sql and --sync-role go together")
    url = _parse_env_text(args.env_file.read_text()).get(URL_ENV_VAR)
    if not url:
        print(f"{URL_ENV_VAR} is not in {args.env_file}", file=sys.stderr)
        return 2
    conn = psycopg2.connect(
        url,
        application_name="verify-secret-columns",
        options="-c default_transaction_read_only=on -c statement_timeout=300000",
    )
    conn.autocommit = True
    try:
        login, results, failures = check(conn)
        if args.render_admin_sql:
            with conn.cursor() as cur:
                remainder = not_owned_columns(cur, args.sync_role)
            fd = os.open(args.render_admin_sql, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            os.fchmod(fd, 0o600)  # O_CREAT's mode does not apply to an existing file
            with os.fdopen(fd, "w") as handle:
                handle.write(render_admin_drop_sql(remainder))
            for column in remainder:
                print(f"admin step: {column.qualified} (owner {column.owner})")
            print(f"Wrote {len(remainder)} DROP COLUMN statement(s) to {args.render_admin_sql}")
    finally:
        conn.close()
    for name, result in results:
        print(f"{name}: {result}")
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    print(f"{login}: {len(results)} inventory columns checked, {len(failures)} readable")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
