#!/usr/bin/env python3
"""Render and check PostgreSQL direct-access & statement audit logging SQL.

Reads ``warehouse-postgres-docker/logging.conf`` as the single source of truth
for cluster logging settings, adapts ``log_connections`` for PostgreSQL 18
(``'all'`` vs ``'on'`` on PG 17), renders idempotent, reload-only
``ALTER SYSTEM`` / ``ALTER ROLE`` / ``pg_reload_conf()`` SQL for apply or
rollback, and provides a read-only ``check`` subcommand to verify live
``pg_settings``, ``pg_db_role_setting``, and ``pg_file_settings`` errors.

This script contains no hostnames, container IDs, or credentials. Operators
pipe rendered SQL into ``psql`` over an authorized admin channel, or run
``check --dsn-env <VAR>`` (or ``--dsn`` for local/throwaway tests).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOGGING_CONF = REPO_ROOT / "warehouse-postgres-docker" / "logging.conf"

# Parameters that can be changed with ALTER SYSTEM + pg_reload_conf() without
# a postmaster restart. logging_collector is postmaster-context and defaults to
# off; check() verifies it alongside the reloadable keys.
RELOADABLE_KEYS = (
    "log_destination",
    "log_connections",
    "log_disconnections",
    "log_statement",
    "log_parameter_max_length",
    "log_parameter_max_length_on_error",
    "log_line_prefix",
)

VALID_LOG_STATEMENT_LEVELS = frozenset({"none", "ddl", "mod", "all"})
ROLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def parse_pg_conf(text: str) -> dict[str, str]:
    """Parse simple ``key = value`` PostgreSQL config lines."""
    settings: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            value = value[1:-1]
        settings[key] = value
    return settings


def expected_settings(pg_major: int, conf_path: Path = DEFAULT_LOGGING_CONF) -> dict[str, str]:
    """Return expected ``pg_settings`` values for ``pg_major`` (17 or 18)."""
    if pg_major not in (17, 18):
        raise ValueError(f"Unsupported PostgreSQL major version: {pg_major} (expected 17 or 18)")
    settings = parse_pg_conf(conf_path.read_text(encoding="utf-8"))
    if pg_major >= 18:
        settings["log_connections"] = "all"
    return settings


def parse_role_exclusions(items: list[str]) -> list[tuple[str, str]]:
    """Parse ``role=level`` CLI items and validate identifiers and levels."""
    parsed: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --exclude-role {item!r}: expected role=level")
        role, level = item.split("=", 1)
        role = role.strip()
        level = level.strip().lower()
        if not ROLE_NAME_RE.match(role):
            raise ValueError(f"Invalid PostgreSQL role name: {role!r}")
        if level not in VALID_LOG_STATEMENT_LEVELS:
            raise ValueError(
                f"Invalid log_statement level {level!r} for role {role!r}; "
                f"expected one of {sorted(VALID_LOG_STATEMENT_LEVELS)}"
            )
        if role in seen:
            raise ValueError(f"Duplicate --exclude-role for {role!r}")
        seen.add(role)
        parsed.append((role, level))
    return parsed


def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_apply_sql(
    pg_major: int,
    role_exclusions: list[tuple[str, str]] | None = None,
    conf_path: Path = DEFAULT_LOGGING_CONF,
) -> str:
    """Render idempotent, reload-only SQL to enable access and statement logging.

    Note: when applying role exclusions on a live database with active sessions
    of an excluded role, run ``ALTER ROLE ... SET log_statement`` first and
    wait for pre-existing sessions of that role to end before reloading
    cluster-wide ``log_statement = 'all'``.
    """
    settings = expected_settings(pg_major, conf_path=conf_path)
    lines = [
        f"-- Apply PostgreSQL {pg_major} access and statement audit logging (reload-only, no restart)",
    ]
    for role, level in role_exclusions or []:
        lines.append(
            f"ALTER ROLE {_quote_ident(role)} SET log_statement = {_quote_literal(level)};"
        )
    for key in RELOADABLE_KEYS:
        if key not in settings:
            raise KeyError(f"Missing required key {key!r} in {conf_path}")
        lines.append(f"ALTER SYSTEM SET {key} = {_quote_literal(settings[key])};")
    lines.append("SELECT pg_reload_conf();")
    return "\n".join(lines) + "\n"


def render_rollback_sql(
    role_exclusions: list[tuple[str, str]] | None = None,
    reset_connection_logging: bool = False,
) -> str:
    """Render reload-only SQL to roll back statement (and optionally connection) logging."""
    keys_to_reset = (
        RELOADABLE_KEYS
        if reset_connection_logging
        else (
            "log_statement",
            "log_parameter_max_length",
            "log_parameter_max_length_on_error",
        )
    )
    lines = [
        "-- Roll back PostgreSQL audit logging changes (reload-only, no restart)",
    ]
    for key in keys_to_reset:
        lines.append(f"ALTER SYSTEM RESET {key};")
    for role, _ in role_exclusions or []:
        lines.append(f"ALTER ROLE {_quote_ident(role)} RESET log_statement;")
    lines.append("SELECT pg_reload_conf();")
    return "\n".join(lines) + "\n"


def parse_setconfig(setconfig: list[str] | None) -> dict[str, str]:
    """Parse PostgreSQL ``pg_db_role_setting.setconfig`` text[] entries."""
    parsed: dict[str, str] = {}
    for entry in setconfig or []:
        if "=" not in entry:
            continue
        k, v = entry.split("=", 1)
        parsed[k.strip()] = v.strip()
    return parsed


def evaluate_settings(
    live_settings: dict[str, str],
    pg_major: int,
    live_role_overrides: dict[str, dict[str, str]] | None = None,
    expected_exclusions: list[tuple[str, str]] | None = None,
    file_errors: list[str] | None = None,
    conf_path: Path = DEFAULT_LOGGING_CONF,
) -> list[str]:
    """Compare live ``pg_settings``, role overrides, and ``pg_file_settings`` errors.

    Returns a list of human-readable drift/failure messages (empty means pass).
    """
    expected = expected_settings(pg_major, conf_path=conf_path)
    errors: list[str] = []
    for key, want in expected.items():
        got = live_settings.get(key)
        if got != want:
            errors.append(f"{key}: expected {want!r}, got {got!r}")

    want_roles = dict(expected_exclusions or [])
    got_roles: dict[str, str] = {}
    for role, cfg in (live_role_overrides or {}).items():
        if "log_statement" in cfg:
            got_roles[role] = cfg["log_statement"]

    for role, want_level in sorted(want_roles.items()):
        got_level = got_roles.get(role)
        if got_level != want_level:
            errors.append(
                f"role {role!r} log_statement: expected {want_level!r}, got {got_level!r}"
            )
    for role, got_level in sorted(got_roles.items()):
        if role not in want_roles:
            errors.append(
                f"unexpected role override on {role!r}: log_statement={got_level!r}"
            )

    for err in file_errors or []:
        errors.append(f"pg_file_settings error: {err}")
    return errors


def check_live_database(
    dsn: str,
    pg_major: int,
    expected_exclusions: list[tuple[str, str]] | None = None,
    conf_path: Path = DEFAULT_LOGGING_CONF,
) -> dict[str, object]:
    """Read-only check of live PostgreSQL logging settings and role overrides."""
    import psycopg2

    expected = expected_settings(pg_major, conf_path=conf_path)
    keys = sorted(expected.keys())

    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, setting FROM pg_settings WHERE name = ANY(%s)",
                (keys,),
            )
            live_settings = {name: setting for name, setting in cur.fetchall()}

            cur.execute(
                """
                SELECT COALESCE(r.rolname, '<all_roles>'), s.setconfig
                FROM pg_db_role_setting s
                LEFT JOIN pg_roles r ON r.oid = s.setrole
                WHERE s.setconfig::text LIKE '%%log_%%'
                """
            )
            live_role_overrides: dict[str, dict[str, str]] = {}
            for role_name, setconfig in cur.fetchall():
                live_role_overrides[role_name] = parse_setconfig(setconfig)

            file_errors: list[str] = []
            try:
                cur.execute(
                    """
                    SELECT sourcefile, sourceline, name, error
                    FROM pg_file_settings
                    WHERE error IS NOT NULL
                    """
                )
                for src, line, name, err in cur.fetchall():
                    file_errors.append(f"{src}:{line} ({name}): {err}")
            except psycopg2.errors.InsufficientPrivilege:
                # Non-superuser readers cannot read pg_file_settings; skip cleanly.
                pass
    finally:
        conn.close()

    errors = evaluate_settings(
        live_settings=live_settings,
        pg_major=pg_major,
        live_role_overrides=live_role_overrides,
        expected_exclusions=expected_exclusions,
        file_errors=file_errors,
        conf_path=conf_path,
    )
    return {
        "ok": len(errors) == 0,
        "pg_major": pg_major,
        "settings": live_settings,
        "role_overrides": live_role_overrides,
        "errors": errors,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render and check PostgreSQL access & statement audit logging."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    render_p = sub.add_parser("render", help="Print reload-only apply or rollback SQL.")
    render_p.add_argument(
        "--pg-major",
        type=int,
        choices=(17, 18),
        required=True,
        help="Target PostgreSQL major version (17 or 18).",
    )
    render_p.add_argument(
        "--exclude-role",
        action="append",
        default=[],
        metavar="ROLE=LEVEL",
        help="Proposed/approved per-role log_statement override (e.g. hc_warehouse_dagster=ddl).",
    )
    render_p.add_argument(
        "--rollback",
        action="store_true",
        help="Render rollback SQL instead of apply SQL.",
    )
    render_p.add_argument(
        "--reset-connection-logging",
        action="store_true",
        help="With --rollback, also reset log_connections, log_disconnections, and log_line_prefix.",
    )
    render_p.add_argument(
        "--conf",
        type=Path,
        default=DEFAULT_LOGGING_CONF,
        help="Path to logging.conf (default: warehouse-postgres-docker/logging.conf).",
    )

    check_p = sub.add_parser(
        "check",
        help="Read-only verification of live pg_settings and pg_db_role_setting.",
    )
    dsn_group = check_p.add_mutually_exclusive_group(required=True)
    dsn_group.add_argument(
        "--dsn",
        help="PostgreSQL connection string (prefer --dsn-env outside local throwaway tests).",
    )
    dsn_group.add_argument(
        "--dsn-env",
        metavar="ENV_VAR",
        help="Environment variable holding the PostgreSQL connection string.",
    )
    check_p.add_argument(
        "--pg-major",
        type=int,
        choices=(17, 18),
        required=True,
        help="Expected PostgreSQL major version (17 or 18).",
    )
    check_p.add_argument(
        "--exclude-role",
        action="append",
        default=[],
        metavar="ROLE=LEVEL",
        help="Expected per-role log_statement override (e.g. hc_warehouse_dagster=ddl).",
    )
    check_p.add_argument(
        "--conf",
        type=Path,
        default=DEFAULT_LOGGING_CONF,
        help="Path to logging.conf (default: warehouse-postgres-docker/logging.conf).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    exclusions = parse_role_exclusions(args.exclude_role)

    if args.command == "render":
        if args.rollback:
            sql = render_rollback_sql(
                role_exclusions=exclusions,
                reset_connection_logging=args.reset_connection_logging,
            )
        else:
            sql = render_apply_sql(
                pg_major=args.pg_major,
                role_exclusions=exclusions,
                conf_path=args.conf,
            )
        sys.stdout.write(sql)
        return 0

    if args.command == "check":
        dsn = args.dsn if args.dsn else os.environ.get(args.dsn_env, "")
        if not dsn:
            sys.stderr.write(f"ERROR: environment variable {args.dsn_env!r} is empty or unset\n")
            return 2
        result = check_live_database(
            dsn=dsn,
            pg_major=args.pg_major,
            expected_exclusions=exclusions,
            conf_path=args.conf,
        )
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return 0 if result["ok"] else 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
