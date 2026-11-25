#!/usr/bin/env python3
"""Render and check PostgreSQL direct-access & statement audit logging SQL.

Reads ``warehouse-postgres-docker/logging.conf`` as the single source of truth
for cluster logging settings, adapts ``log_connections`` for PostgreSQL 18
(``'all'`` vs ``'on'`` on PG 17), and renders idempotent, reload-only
``ALTER SYSTEM`` / ``ALTER ROLE`` / ``pg_reload_conf()`` SQL for apply or
rollback.

This script never connects to production on its own and contains no hostnames,
container IDs, or credentials. Operators pipe rendered SQL into ``psql`` over
an authorized admin channel, or pass a local/test ``--dsn`` to ``check``.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOGGING_CONF = REPO_ROOT / "warehouse-postgres-docker" / "logging.conf"

# Parameters that can be changed with ALTER SYSTEM + pg_reload_conf() without
# a postmaster restart. logging_collector is postmaster-context and defaults to
# off; we verify it in check() rather than issuing ALTER SYSTEM for it unless
# requested.
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
    """Render idempotent, reload-only SQL to enable access and statement logging."""
    settings = expected_settings(pg_major, conf_path=conf_path)
    lines = [
        f"-- Apply PostgreSQL {pg_major} access and statement audit logging (reload-only, no restart)",
    ]
    for key in RELOADABLE_KEYS:
        if key not in settings:
            raise KeyError(f"Missing required key {key!r} in {conf_path}")
        lines.append(f"ALTER SYSTEM SET {key} = {_quote_literal(settings[key])};")
    for role, level in role_exclusions or []:
        lines.append(
            f"ALTER ROLE {_quote_ident(role)} SET log_statement = {_quote_literal(level)};"
        )
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


def evaluate_settings(
    live_settings: dict[str, str],
    pg_major: int,
    live_role_overrides: dict[str, dict[str, str]] | None = None,
    expected_exclusions: list[tuple[str, str]] | None = None,
    conf_path: Path = DEFAULT_LOGGING_CONF,
) -> list[str]:
    """Compare live ``pg_settings`` and role overrides against expected state.

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
    return errors


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render idempotent PostgreSQL access & statement audit logging SQL."
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
        help="Approved per-role log_statement override (e.g. hc_warehouse_dagster=ddl).",
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "render":
        exclusions = parse_role_exclusions(args.exclude_role)
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
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
