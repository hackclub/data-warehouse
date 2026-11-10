#!/usr/bin/env python3
"""Provision and verify the warehouse's two limited read logins.

--login agent is hc_warehouse_agent, the agents' everyday login. --login mcp is
hc_warehouse_mcp, the login of the warehouse-mcp query tool. Create the agent login
first: the query tool's SQL grants its role to the agent login.

Create one, or give it a new password (running this again does that):

    PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_agent_login.py \\
        --login <agent|mcp> --env-file <file with the current URL> \\
        --render-sql-file <scratch.sql> --write-env-file <staged.env>
    bin/warehouse-admin-psql --reason "create <agent|mcp> login" -f <scratch.sql>

The plaintext password goes only to --write-env-file, with mode 0600. The SQL holds
a SCRAM verifier, so log_statement=ddl never logs the password. Delete the SQL file
after applying it.

Check a login that is already there, using the URL in --env-file. Checking mcp checks
both logins, and needs the agent login's URL too:

    PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_agent_login.py \\
        --login agent --env-file <agent.env> --verify
    PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_agent_login.py \\
        --login mcp --env-file <staged.env> --agent-env-file <agent.env> --verify

See runbooks/prod-health-monitoring.md §10.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from dataclasses import dataclass
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
MCP_ROLE = "hc_warehouse_mcp"
URL_ENV_VAR = "WAREHOUSE_COOLIFY_URL"
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


@dataclass(frozen=True)
class Login:
    role: str
    connection_limit: int


LOGINS = {
    "agent": Login(AGENT_ROLE, connection_limit=20),
    # warehouse-mcp's pool (max_size in warehouse-mcp/db.py) is 5. A deploy overlap runs
    # the old container's 5 too, and 2 more cover sockets cut but still open on the server.
    "mcp": Login(MCP_ROLE, connection_limit=12),
}

# Postgres lets a role signal the sessions of any role whose privileges it inherits, so
# this lets agents cancel a stuck query-tool session and nothing else. pg_signal_backend
# would let an exposed agent URL hit every non-superuser login. The query tool's role
# owns nothing and has no grant the agent login lacks, and SET FALSE stops the agent
# login becoming it.
CANCEL_GRANT_OPTIONS = "INHERIT TRUE, SET FALSE, ADMIN FALSE"


def render_setup_sql(login: Login, scram_verifier: str) -> str:
    """One transaction that creates the role, or brings an existing one to this state."""
    if not _SCRAM_RE.match(scram_verifier):
        raise ValueError("scram_verifier must be a SCRAM-SHA-256 verifier string")
    role = _quote_ident(login.role)
    lines = [
        "BEGIN;",
        "DO $$ BEGIN",
        f"  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = {_quote_literal(login.role)}) THEN",
        f"    CREATE ROLE {role};",
        "  END IF;",
        "END $$;",
        f"ALTER ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION "
        f"NOBYPASSRLS INHERIT CONNECTION LIMIT {login.connection_limit} "
        f"PASSWORD {_quote_literal(scram_verifier)};",
        f"GRANT {', '.join(PREDEFINED_ROLES)} TO {role};",
    ]
    lines += [
        f"ALTER ROLE {role} SET {name} = {_quote_literal(value)};"
        for name, value in ROLE_SETTINGS.items()
    ]
    if login.role == MCP_ROLE:
        lines.append(
            f"GRANT {_quote_ident(MCP_ROLE)} TO {_quote_ident(AGENT_ROLE)} "
            f"WITH {CANCEL_GRANT_OPTIONS};"
        )
    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def build_login_url(source_url: str, login: Login, password: str) -> str:
    """The same server, database and options as source_url, as this login."""
    parsed = urlparse(source_url)
    if not parsed.hostname or not parsed.path or parsed.path == "/":
        raise ValueError("source_url must include a host and a database name")
    host = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
    netloc = f"{quote(login.role, safe='')}:{quote(password, safe='')}@{host}"
    return urlunparse(parsed._replace(netloc=netloc))


def _write_private(path: Path, content: str) -> None:
    """Write through a new 0600 file, so the content is never readable by others."""
    temp_path = path.with_name(f".{path.name}.{secrets.token_hex(4)}")
    fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(content)
    os.replace(temp_path, path)


def _expected_memberships(login: Login) -> set[tuple]:
    """(role, member, admin, inherit, set) for every membership the login is part of."""
    return {
        *((name, login.role, False, True, True) for name in PREDEFINED_ROLES),
        (MCP_ROLE, AGENT_ROLE, False, True, False),
    }


def _attribute_failures(cur, login: Login) -> list[str]:
    failures = []
    cur.execute("SELECT current_user")
    if cur.fetchone()[0] != login.role:
        return [f"logged in as another role, not {login.role}"]
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
    if limit != login.connection_limit:
        failures.append(f"connection limit is {limit}, not {login.connection_limit}")
    cur.execute(
        """
        SELECT r.rolname, u.rolname, m.admin_option, m.inherit_option, m.set_option
        FROM pg_auth_members m
        JOIN pg_roles r ON r.oid = m.roleid
        JOIN pg_roles u ON u.oid = m.member
        WHERE current_user IN (r.rolname, u.rolname)
        """
    )
    memberships = set(cur.fetchall())
    expected = _expected_memberships(login)
    if memberships != expected:
        failures.append(f"role memberships are {sorted(memberships)}, not {sorted(expected)}")
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


def verify_login(login: Login, url: str) -> list[str]:
    """Failures for the login at url, as text with no row data. Empty means it passed.

    The refusal checks run only once the attributes prove they will be refused: on
    a superuser, the terminate check would end a real session.
    """
    conn = psycopg2.connect(url, application_name="verify-warehouse-login")
    try:
        with conn.cursor() as cur:
            failures = _attribute_failures(cur, login)
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
            # Idle, so nothing is lost even if the check were to pass.
            cur.execute(
                """
                SELECT a.pid FROM pg_stat_activity a JOIN pg_roles r ON r.oid = a.usesysid
                WHERE NOT r.rolsuper AND a.backend_type = 'client backend' AND a.state = 'idle'
                  AND NOT pg_has_role(current_user, a.usesysid, 'USAGE')
                LIMIT 1
                """
            )
            row = cur.fetchone()
            other_login_pid = row[0] if row else None
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
        probe_name = f"login_probe_{secrets.token_hex(4)}"
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
        if other_login_pid is not None:
            probes.append(
                ("cancel another login's session", "SELECT pg_cancel_backend(%s)", (other_login_pid,))
            )
        else:
            failures.append("found no idle session of another non-superuser login to test cancel on")
        for label, sql, params in probes:
            failure = _probe_refused(conn, label, sql, params)
            if failure:
                failures.append(failure)
        return failures
    finally:
        conn.close()


def verify_agent_can_cancel_mcp(agent_url: str, mcp_url: str) -> list[str]:
    """Cancel a throwaway idle query-tool session from the agent login."""
    target = psycopg2.connect(mcp_url, application_name="verify-cancel-target")
    agent = None
    try:
        with target.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            pid = cur.fetchone()[0]
        target.rollback()
        agent = psycopg2.connect(agent_url, application_name="verify-cancel")
        with agent.cursor() as cur:
            cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
            if cur.fetchone()[0] is not True:
                return ["the agent login could not cancel a query-tool session"]
        return []
    except errors.InsufficientPrivilege:
        return ["the agent login was refused cancelling a query-tool session"]
    finally:
        if agent is not None:
            agent.close()
        target.close()


def _url_from(env_file: Path) -> str | None:
    return _parse_env_text(env_file.read_text()).get(URL_ENV_VAR)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision and verify a limited warehouse login.")
    parser.add_argument("--login", choices=sorted(LOGINS), required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--agent-env-file", type=Path, help="the agent login's URL, to check mcp")
    parser.add_argument("--render-sql-file", type=Path)
    parser.add_argument("--write-env-file", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args(argv)

    if bool(args.render_sql_file) != bool(args.write_env_file):
        parser.error("--render-sql-file and --write-env-file go together, so a new password is never lost")
    if not args.verify and not args.render_sql_file:
        parser.error("nothing to do: use --render-sql-file and --write-env-file, or --verify")
    if bool(args.agent_env_file) != (args.verify and args.login == "mcp"):
        parser.error("--agent-env-file goes with --login mcp --verify, and only with that")

    login = LOGINS[args.login]
    url = _url_from(args.env_file)
    if not url:
        print(f"{URL_ENV_VAR} is not in {args.env_file}", file=sys.stderr)
        return 2
    agent_url = _url_from(args.agent_env_file) if args.agent_env_file else None
    if args.agent_env_file and not agent_url:
        print(f"{URL_ENV_VAR} is not in {args.agent_env_file}", file=sys.stderr)
        return 2

    if args.render_sql_file:
        password = secrets.token_urlsafe(32)
        new_url = build_login_url(url, login, password)
        existing = args.write_env_file.read_text() if args.write_env_file.exists() else ""
        _write_private(
            args.write_env_file, _update_env_file_content(existing, {URL_ENV_VAR: new_url})
        )
        _write_private(args.render_sql_file, render_setup_sql(login, _scram_sha256_verifier(password)))
        print(f"Wrote {args.render_sql_file} and {URL_ENV_VAR} in {args.write_env_file}")

    if args.verify:
        failures = verify_login(login, url)
        if agent_url:
            failures += verify_login(LOGINS["agent"], agent_url)
            # Only once both logins are proven limited, as with the refusal checks.
            failures = failures or verify_agent_can_cancel_mcp(agent_url, url)
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        if failures:
            return 1
        print(f"{login.role}: attributes, read access and every refusal check passed")
        if agent_url:
            print(f"{AGENT_ROLE}: the same checks passed, and it can cancel a {MCP_ROLE} session")
    return 0


if __name__ == "__main__":
    sys.exit(main())
