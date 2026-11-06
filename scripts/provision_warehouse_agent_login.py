#!/usr/bin/env python3
"""Provision and verify hc_warehouse_agent, the agents' everyday warehouse login.

Create it, or give it a new password (running this again does that):

    PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_agent_login.py \\
        --env-file <file with the current URL> \\
        --render-sql-file <scratch.sql> --write-env-file <staged.env>
    bin/warehouse-admin-psql --reason "create agent login" -f <scratch.sql>

The plaintext password goes only to --write-env-file, with mode 0600. The SQL holds
a SCRAM verifier, so log_statement=ddl never logs the password. Delete the SQL file
after applying it.

Check a login that is already there, using the URL in --env-file:

    PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_agent_login.py \\
        --env-file <staged.env> --verify

See runbooks/prod-health-monitoring.md §10.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from pathlib import Path
from typing import Sequence
from urllib.parse import quote, urlparse, urlunparse

import psycopg2
from psycopg2 import errors

from scripts.provision_warehouse_reader import (
    _parse_env_text,
    _quote_ident,
    _quote_literal,
    _SCRAM_RE,
    _scram_sha256_verifier,
    _update_env_file_content,
)

AGENT_ROLE = "hc_warehouse_agent"
URL_ENV_VAR = "WAREHOUSE_COOLIFY_URL"
CONNECTION_LIMIT = 20
PREDEFINED_ROLES = ("pg_read_all_data", "pg_read_all_stats")
ROLE_SETTINGS = {
    # A backstop only: the login can turn it off. The missing privileges are the block.
    "default_transaction_read_only": "on",
    # An agent query must not sit in the lock queue behind a TRUNCATE, blocking
    # everyone who queues behind it.
    "lock_timeout": "30s",
    # One long reader can freeze the warehouse (§3 of the runbook).
    "statement_timeout": "30min",
    "idle_in_transaction_session_timeout": "5min",
    # Caps sort and hash spill files per process. Postgres does not count temporary
    # tables against it, and only a superuser can raise it.
    "temp_file_limit": "50GB",
}


def render_setup_sql(scram_verifier: str) -> str:
    """One transaction that creates the role, or brings an existing one to this state."""
    if not _SCRAM_RE.match(scram_verifier):
        raise ValueError("scram_verifier must be a SCRAM-SHA-256 verifier string")
    role = _quote_ident(AGENT_ROLE)
    lines = [
        "BEGIN;",
        "DO $$ BEGIN",
        f"  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = {_quote_literal(AGENT_ROLE)}) THEN",
        f"    CREATE ROLE {role};",
        "  END IF;",
        "END $$;",
        f"ALTER ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION "
        f"NOBYPASSRLS INHERIT CONNECTION LIMIT {CONNECTION_LIMIT} "
        f"PASSWORD {_quote_literal(scram_verifier)};",
        f"GRANT {', '.join(PREDEFINED_ROLES)} TO {role};",
    ]
    lines += [
        f"ALTER ROLE {role} SET {name} = {_quote_literal(value)};"
        for name, value in ROLE_SETTINGS.items()
    ]
    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def build_agent_url(source_url: str, password: str) -> str:
    """The same server, database and options as source_url, as the agent login."""
    parsed = urlparse(source_url)
    if not parsed.hostname or not parsed.path or parsed.path == "/":
        raise ValueError("source_url must include a host and a database name")
    host = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
    netloc = f"{quote(AGENT_ROLE, safe='')}:{quote(password, safe='')}@{host}"
    return urlunparse(parsed._replace(netloc=netloc))


def _write_private(path: Path, content: str) -> None:
    """Write through a new 0600 file, so the content is never readable by others."""
    temp_path = path.with_name(f".{path.name}.{secrets.token_hex(4)}")
    fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(content)
    os.replace(temp_path, path)


def _attribute_failures(cur) -> list[str]:
    failures = []
    cur.execute("SELECT current_user")
    if cur.fetchone()[0] != AGENT_ROLE:
        return [f"logged in as another role, not {AGENT_ROLE}"]
    cur.execute(
        """
        SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls,
               rolconnlimit
        FROM pg_roles WHERE rolname = current_user
        """
    )
    *powers, limit = cur.fetchone()
    if any(powers):
        failures.append("the login has a power attribute such as SUPERUSER")
    if limit != CONNECTION_LIMIT:
        failures.append(f"connection limit is {limit}, not {CONNECTION_LIMIT}")
    cur.execute(
        """
        SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid
        WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user)
        ORDER BY 1
        """
    )
    granted = tuple(row[0] for row in cur.fetchall())
    if granted != PREDEFINED_ROLES:
        failures.append(f"role memberships are {list(granted)}, not {list(PREDEFINED_ROLES)}")
    cur.execute("SELECT unnest(rolconfig) FROM pg_roles WHERE rolname = current_user")
    configured = dict(row[0].split("=", 1) for row in cur.fetchall())
    if configured != ROLE_SETTINGS:
        failures.append(f"role settings are {configured}, not {ROLE_SETTINGS}")
    return failures


def _probe_refused(conn, label: str, sql: str, params=None) -> str | None:
    """Run sql in a rolled-back read-write transaction; only a privilege error passes."""
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ WRITE")
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute(sql, params)
        return f"{label}: was allowed"
    except errors.InsufficientPrivilege:
        return None
    except psycopg2.Error as error:
        return f"{label}: inconclusive ({type(error).__name__})"
    finally:
        conn.rollback()


def verify_agent_login(url: str) -> list[str]:
    """Failures for the login at url, as text with no row data. Empty means it passed.

    The refusal checks run only once the attributes prove they will be refused: on
    a superuser, the terminate check would end a real session.
    """
    conn = psycopg2.connect(url, application_name="verify-agent-login")
    try:
        with conn.cursor() as cur:
            failures = _attribute_failures(cur)
            if failures:
                return failures
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE backend_type = 'client backend' AND usename <> current_user "
                "AND query IS NOT NULL AND query NOT IN ('', '<insufficient privilege>')"
            )
            other_sessions_query_text = cur.fetchone()[0]
            cur.execute(
                """
                SELECT a.pid FROM pg_stat_activity a JOIN pg_roles r ON r.oid = a.usesysid
                WHERE r.rolsuper AND a.backend_type = 'client backend' LIMIT 1
                """
            )
            row = cur.fetchone()
            superuser_pid = row[0] if row else None
            cur.execute(
                """
                SELECT format('%I.%I', n.nspname, c.relname), a.attname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                WHERE c.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg\\_%'
                ORDER BY c.reltuples, 1, a.attnum LIMIT 1
                """
            )
            table_row = cur.fetchone()
        conn.rollback()

        if not other_sessions_query_text:
            failures.append("could not see another login's query text")
        probe_name = f"agent_login_probe_{secrets.token_hex(4)}"
        probes = [
            ("CREATE SCHEMA", f"CREATE SCHEMA {probe_name}", None),
            ("COPY TO PROGRAM", "COPY (SELECT 1) TO PROGRAM 'true'", None),
            ("read a server file", "SELECT pg_read_file('postgresql.conf', 0, 1)", None),
        ]
        if table_row:
            table, column = table_row
            column = _quote_ident(column)
            # WHERE false, so no row can change even if the check were to pass.
            probes.append(
                ("UPDATE a table", f"UPDATE {table} SET {column} = {column} WHERE false", None)
            )
        else:
            failures.append("found no table to test UPDATE on")
        if superuser_pid is not None:
            probes.append(
                ("terminate a superuser session", "SELECT pg_terminate_backend(%s)", (superuser_pid,))
            )
        else:
            failures.append("found no superuser session to test terminate on")
        for label, sql, params in probes:
            failure = _probe_refused(conn, label, sql, params)
            if failure:
                failures.append(failure)
        return failures
    finally:
        conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"Provision and verify {AGENT_ROLE}.")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--render-sql-file", type=Path)
    parser.add_argument("--write-env-file", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args(argv)

    if bool(args.render_sql_file) != bool(args.write_env_file):
        parser.error("--render-sql-file and --write-env-file go together, so a new password is never lost")
    if not args.verify and not args.render_sql_file:
        parser.error("nothing to do: use --render-sql-file and --write-env-file, or --verify")

    url = _parse_env_text(args.env_file.read_text()).get(URL_ENV_VAR)
    if not url:
        print(f"{URL_ENV_VAR} is not in {args.env_file}", file=sys.stderr)
        return 2

    if args.render_sql_file:
        password = secrets.token_urlsafe(32)
        new_url = build_agent_url(url, password)
        existing = args.write_env_file.read_text() if args.write_env_file.exists() else ""
        _write_private(
            args.write_env_file, _update_env_file_content(existing, {URL_ENV_VAR: new_url})
        )
        _write_private(args.render_sql_file, render_setup_sql(_scram_sha256_verifier(password)))
        print(f"Wrote {args.render_sql_file} and {URL_ENV_VAR} in {args.write_env_file}")

    if args.verify:
        failures = verify_agent_login(url)
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        if failures:
            return 1
        print(f"{AGENT_ROLE}: attributes, read access and every refusal check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
