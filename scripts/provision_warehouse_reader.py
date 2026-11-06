#!/usr/bin/env python3
"""
Derive least-privilege hc_warehouse_reader column grants from a Sling program's
replication_config, render single-transaction SCRAM-SHA-256 role setup SQL,
verify reader privileges over the public Orchard endpoint, and compare pre/post
row counts without printing credentials.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import importlib
import json
import math
import os
import re
import secrets
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

PROGRAM_ENV_VARS: dict[str, str] = {
    "attend": "ATTEND_COOLIFY_URL",
    "fallen": "FALLEN_K8S_URL",
    "horizons": "HORIZONS_K8S_URL",
    "midnight": "MIDNIGHT_K8S_URL",
}

_SCRAM_RE = re.compile(
    r"^SCRAM-SHA-256\$\d+:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$"
)
_SQL_STRING_LITERAL_RE = re.compile(r"'[^']*'")
_SQL_AS_ALIAS_RE = re.compile(r"\bas\s+[A-Za-z_][A-Za-z0-9_]*\b", re.IGNORECASE)
_SQL_FROM_CLAUSE_RE = re.compile(
    r"\bfrom\s+[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\b", re.IGNORECASE
)
_SQL_FUNC_CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_SQL_IDENT_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_SQL_KEYWORDS = {"select", "from", "where", "and", "or", "not", "null", "true", "false"}


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier offline without requiring a live psycopg2 connection."""
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Quote a SQL string literal offline without requiring a live psycopg2 connection."""
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError("Invalid SQL literal")
    escaped = value.replace("'", "''")
    if "\\" in escaped:
        return "E'" + escaped.replace("\\", "\\\\") + "'"
    return "'" + escaped + "'"


def _scram_sha256_verifier(
    password: str, salt: bytes | None = None, iterations: int = 4096
) -> str:
    """Build a PostgreSQL SCRAM-SHA-256 verifier locally so the plaintext password
    never enters SQL statements or pod exec audit logs."""
    if not password or "\x00" in password or iterations < 1:
        raise ValueError("Invalid password or iteration count")
    salt_bytes = salt if salt is not None else os.urandom(16)
    if len(salt_bytes) < 8:
        raise ValueError("Salt must be at least 8 bytes")
    salted = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt_bytes, iterations, dklen=32
    )
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    b64 = lambda b: base64.b64encode(b).decode("ascii")
    return f"SCRAM-SHA-256${iterations}:{b64(salt_bytes)}${b64(stored_key)}:{b64(server_key)}"


def _extract_sql_stream_columns(
    stream_name: str, sql_text: str, table_columns: Sequence[str]
) -> tuple[str, ...]:
    """Validate and return the source columns referenced by a custom sql: stream."""
    if "*" in sql_text:
        raise ValueError(f"{stream_name}: wildcard SELECT * is not allowed in sql stream")
    cleaned = _SQL_FROM_CLAUSE_RE.sub(
        " ", _SQL_AS_ALIAS_RE.sub(" ", _SQL_STRING_LITERAL_RE.sub(" ", sql_text))
    )
    func_names = {m.group(1).lower() for m in _SQL_FUNC_CALL_RE.finditer(cleaned)}
    table_col_set = set(table_columns)
    referenced: list[str] = []
    for token in _SQL_IDENT_RE.findall(cleaned):
        if token.lower() in _SQL_KEYWORDS or token.lower() in func_names:
            continue
        if token not in table_col_set:
            raise ValueError(f"{stream_name}: sql stream references unknown column {token!r}")
        if token not in referenced:
            referenced.append(token)
    if not referenced:
        raise ValueError(f"{stream_name}: sql stream references no source columns")
    return tuple(referenced)


def _replication_reader_grants(
    replication_config: Mapping[str, Any],
    source_columns: Mapping[str, Sequence[str]],
) -> dict[str, tuple[str, ...] | None]:
    """Derive exact table (None) or column tuple SELECT grants for a replication_config."""
    defaults = replication_config.get("defaults") or {}
    streams = replication_config.get("streams") or {}
    if not streams:
        raise ValueError("replication_config defines no streams")

    grants: dict[str, tuple[str, ...] | None] = {}
    for stream_name, stream_config in streams.items():
        if "*" in stream_name:
            raise ValueError(
                f"Wildcard stream {stream_name!r} is not compatible with fixed reader grants"
            )
        merged = {**defaults, **(stream_config or {})}
        if merged.get("disabled"):
            continue
        if "." not in stream_name or stream_name not in source_columns:
            raise ValueError(f"Stream {stream_name!r} does not exist in source catalog")

        table_cols = list(source_columns[stream_name])
        if not table_cols:
            raise ValueError(f"Stream {stream_name!r} has no columns in source catalog")
        table_col_set = set(table_cols)

        if merged.get("sql") is not None:
            grants[stream_name] = _extract_sql_stream_columns(
                stream_name, str(merged["sql"]), table_cols
            )
            continue

        select_list = merged.get("select")
        if not select_list:
            grants[stream_name] = None
            continue

        negatives = [str(col)[1:] for col in select_list if str(col).startswith("-")]
        positives = [str(col) for col in select_list if not str(col).startswith("-")]
        if negatives and positives:
            raise ValueError(f"{stream_name}: cannot mix positive and negative select entries")

        if negatives:
            for col_name in negatives:
                if col_name not in table_col_set:
                    raise ValueError(
                        f"{stream_name}: excluded column {col_name!r} does not exist in source"
                    )
            excluded = set(negatives)
            kept = tuple(col for col in table_cols if col not in excluded)
            if not kept:
                raise ValueError(f"{stream_name}: all columns were excluded")
            grants[stream_name] = kept
        else:
            for col_name in positives:
                if col_name not in table_col_set:
                    raise ValueError(
                        f"{stream_name}: selected column {col_name!r} does not exist in source"
                    )
            grants[stream_name] = tuple(dict.fromkeys(positives))

    if not grants:
        raise ValueError("No active streams found in replication_config")
    return grants


def _render_reader_setup_sql(
    db_name: str,
    scram_verifier: str,
    grants: Mapping[str, Sequence[str] | None],
    role_name: str = "hc_warehouse_reader",
    conn_limit: int = 4,
) -> str:
    """Render a single-transaction SQL script provisioning hc_warehouse_reader."""
    if not _SCRAM_RE.match(scram_verifier):
        raise ValueError("scram_verifier must be a SCRAM-SHA-256 verifier string")
    if conn_limit < 1 or not grants:
        raise ValueError("conn_limit must be positive and grants must not be empty")

    q_role, l_role = _quote_ident(role_name), _quote_literal(role_name)
    q_db, l_scram = _quote_ident(db_name), _quote_literal(scram_verifier)
    schemas = sorted({stream.split(".", 1)[0] for stream in grants})
    role_opts = (
        f"LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION "
        f"NOBYPASSRLS CONNECTION LIMIT {int(conn_limit)} PASSWORD {l_scram}"
    )

    lines = [
        "BEGIN;",
        "DO $$",
        "BEGIN",
        f"  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {l_role}) THEN",
        f"    CREATE ROLE {q_role} {role_opts};",
        "  ELSE",
        f"    ALTER ROLE {q_role} WITH {role_opts};",
        "  END IF;",
        "END",
        "$$;",
        f"ALTER ROLE {q_role} SET default_transaction_read_only = on;",
        f"REVOKE ALL PRIVILEGES ON DATABASE {q_db} FROM {q_role};",
    ]
    for schema_name in schemas:
        q_schema = _quote_ident(schema_name)
        for obj in ("SCHEMA", "ALL TABLES IN SCHEMA", "ALL SEQUENCES IN SCHEMA", "ALL FUNCTIONS IN SCHEMA"):
            lines.append(f"REVOKE ALL PRIVILEGES ON {obj} {q_schema} FROM {q_role};")

    lines.extend([
        "DO $$",
        "BEGIN",
        "  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '_orchard') THEN",
        f"    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA \"_orchard\" FROM {q_role}';",
        f"    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"_orchard\" FROM {q_role}';",
        f"    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA \"_orchard\" FROM {q_role}';",
        "  END IF;",
        "END",
        "$$;",
        f"GRANT CONNECT ON DATABASE {q_db} TO {q_role};",
    ])
    for schema_name in schemas:
        lines.append(f"GRANT USAGE ON SCHEMA {_quote_ident(schema_name)} TO {q_role};")

    for stream_name, cols in grants.items():
        schema_name, table_name = stream_name.split(".", 1)
        q_table = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        if cols is None:
            lines.append(f"GRANT SELECT ON TABLE {q_table} TO {q_role};")
        else:
            if not cols:
                raise ValueError(f"{stream_name}: column grant list cannot be empty")
            col_list = ", ".join(_quote_ident(col) for col in cols)
            lines.append(f"GRANT SELECT ({col_list}) ON TABLE {q_table} TO {q_role};")

    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def _check_row_count_parity(
    replication_config: Mapping[str, Any],
    pre_counts: Mapping[str, tuple[int, int | None]],
    post_counts: Mapping[str, tuple[int, int | None]],
) -> list[str]:
    """Compare pre-switch and post-switch (source_rows, warehouse_rows) counts."""
    defaults = replication_config.get("defaults") or {}
    allow_empty = bool((replication_config.get("env") or {}).get("SLING_ALLOW_EMPTY"))
    failures: list[str] = []

    for stream_name, stream_config in (replication_config.get("streams") or {}).items():
        merged = {**defaults, **(stream_config or {})}
        if merged.get("disabled"):
            continue
        if stream_name not in pre_counts or stream_name not in post_counts:
            failures.append(f"{stream_name}: missing baseline or post-sync count")
            continue

        pre_src, pre_wh = pre_counts[stream_name]
        post_src, post_wh = post_counts[stream_name]
        tol = max(5, math.ceil(0.005 * max(post_src, 0)))

        if post_wh is None:
            if post_src == 0 and not allow_empty:
                continue
            failures.append(f"{stream_name}: warehouse table missing after sync (post_src={post_src})")
            continue

        if merged.get("mode", "full-refresh") == "incremental":
            baseline_wh = pre_wh or 0
            if post_wh < baseline_wh:
                failures.append(f"{stream_name}: incremental count regressed ({baseline_wh} -> {post_wh})")
            if abs((post_wh - post_src) - (baseline_wh - pre_src)) > tol:
                failures.append(
                    f"{stream_name}: incremental gap drifted beyond tolerance "
                    f"(pre={pre_src}/{pre_wh}, post={post_src}/{post_wh}, tol={tol})"
                )
        else:
            if abs(post_src - post_wh) > tol:
                failures.append(
                    f"{stream_name}: full-refresh count mismatch (post_src={post_src}, post_wh={post_wh}, tol={tol})"
                )
            floor = min(pre_wh if pre_wh is not None else post_src, post_src)
            if post_wh + tol < floor:
                failures.append(
                    f"{stream_name}: full-refresh count dropped below baseline (pre_wh={pre_wh}, post_wh={post_wh})"
                )
    return failures


def _build_reader_url(
    source_url: str, password: str, role_name: str = "hc_warehouse_reader"
) -> str:
    parsed = urlparse(source_url)
    if not parsed.hostname or not parsed.path or parsed.path == "/":
        raise ValueError("source_url must include hostname and database name")
    host_port = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
    netloc = f"{quote(role_name, safe='')}:{quote(password, safe='')}@{host_port}"
    query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_pairs.setdefault("sslmode", "verify-full")
    query_pairs.setdefault("sslrootcert", "system")
    return urlunparse(parsed._replace(netloc=netloc, query=urlencode(query_pairs)))


def _parse_env_text(content: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = (part.strip() for part in line.split("=", 1))
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        env[key] = val
    return env


def _update_env_file_content(content: str, updates: Mapping[str, str]) -> str:
    remaining = dict(updates)
    out_lines: list[str] = []
    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, old_rhs = raw_line.split("=", 1)
            key_clean = key.strip()
            if key_clean in remaining:
                new_val = remaining.pop(key_clean)
                rhs = old_rhs.strip()
                q = rhs[0] if len(rhs) >= 2 and rhs[0] == rhs[-1] and rhs[0] in ("'", '"') else ""
                out_lines.append(f"{key}={q}{new_val}{q}")
                continue
        out_lines.append(raw_line)
    for key_clean, new_val in remaining.items():
        out_lines.append(f"{key_clean}={new_val}")
    return "\n".join(out_lines) + ("\n" if content.endswith("\n") or not content else "")


def _fetch_live_catalog(conn_url: str) -> tuple[str, dict[str, list[str]]]:
    import psycopg2

    conn = psycopg2.connect(conn_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
            cur.execute(
                "SELECT table_schema || '.' || table_name, column_name "
                "FROM information_schema.columns "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name, ordinal_position"
            )
            catalog: dict[str, list[str]] = {}
            for table_key, col_name in cur.fetchall():
                catalog.setdefault(table_key, []).append(col_name)
            return db_name, catalog
    finally:
        conn.close()


def _verify_live_reader(
    reader_url: str,
    replication_config: Mapping[str, Any],
    source_columns: Mapping[str, Sequence[str]],
    role_name: str = "hc_warehouse_reader",
) -> None:
    import psycopg2

    grants = _replication_reader_grants(replication_config, source_columns)
    conn = psycopg2.connect(reader_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            if cur.fetchone()[0] != "on":
                raise RuntimeError("Expected transaction_read_only='on'")
            cur.execute(
                "SELECT rolconnlimit, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = %s",
                (role_name,),
            )
            if cur.fetchone() != (4, False, False, False):
                raise RuntimeError(f"Unexpected role flags for {role_name}")
            cur.execute(
                "SELECT count(*) FROM information_schema.role_table_grants "
                "WHERE grantee = %s AND privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')",
                (role_name,),
            )
            if cur.fetchone()[0] != 0:
                raise RuntimeError("Unexpected write table grants")
            cur.execute(
                "SELECT count(*) FROM information_schema.role_column_grants "
                "WHERE grantee = %s AND privilege_type IN ('INSERT','UPDATE','REFERENCES')",
                (role_name,),
            )
            if cur.fetchone()[0] != 0:
                raise RuntimeError("Unexpected write column grants")

            for table_key, all_cols in source_columns.items():
                if not table_key.startswith("public."):
                    continue
                cur.execute("SELECT has_any_column_privilege(%s, %s, 'SELECT')", (role_name, table_key))
                has_any = cur.fetchone()[0]
                if table_key in grants:
                    if not has_any:
                        raise RuntimeError(f"Missing SELECT privilege on mirrored {table_key}")
                    kept_cols = set(grants[table_key] or all_cols)
                    for col in all_cols:
                        cur.execute(
                            "SELECT has_column_privilege(%s, %s, %s, 'SELECT')",
                            (role_name, table_key, col),
                        )
                        if cur.fetchone()[0] != (col in kept_cols):
                            raise RuntimeError(f"Column privilege mismatch on {table_key}.{col}")
                elif has_any:
                    raise RuntimeError(f"Unexpected SELECT privilege on unmirrored {table_key}")

            defaults = replication_config.get("defaults") or {}
            for stream_name, stream_cfg in (replication_config.get("streams") or {}).items():
                merged = {**defaults, **(stream_cfg or {})}
                if merged.get("disabled"):
                    continue
                if merged.get("sql"):
                    q = f"SELECT * FROM ({merged['sql']}) AS _q LIMIT 1"
                else:
                    s_name, t_name = stream_name.split(".", 1)
                    cols = grants[stream_name]
                    col_expr = "*" if cols is None else ", ".join(_quote_ident(c) for c in cols)
                    q = f"SELECT {col_expr} FROM {_quote_ident(s_name)}.{_quote_ident(t_name)} LIMIT 1"
                cur.execute(q)
                cur.fetchall()
    finally:
        conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision and verify hc_warehouse_reader roles.")
    parser.add_argument("--program", required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--env-var")
    parser.add_argument("--catalog-json", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--render-sql-file", type=Path)
    parser.add_argument("--write-env-file", type=Path)
    parser.add_argument("--verify-reader", action="store_true")
    args = parser.parse_args(argv)

    mod = importlib.import_module(f"orpheus_engine.defs.sling.programs.{args.program}")
    config = getattr(mod, f"{args.program}_replication_config")
    env_var = args.env_var or PROGRAM_ENV_VARS.get(args.program, f"{args.program.upper()}_K8S_URL")
    env_map = _parse_env_text(args.env_file.read_text(encoding="utf-8")) if args.env_file else {}

    if args.catalog_json:
        payload = json.loads(args.catalog_json.read_text(encoding="utf-8"))
        db_name, source_columns = payload["db_name"], payload["columns"]
    else:
        conn_url = env_map.get(env_var) or os.getenv(env_var)
        if not conn_url:
            raise SystemExit(f"Missing {env_var} for {args.program}")
        db_name, source_columns = _fetch_live_catalog(conn_url)

    grants = _replication_reader_grants(config, source_columns)
    table_grants = sum(1 for cols in grants.values() if cols is None)
    col_grants = sum(1 for cols in grants.values() if cols is not None)

    if args.check:
        print(
            f"{args.program}: {len(grants)} streams ({table_grants} table SELECT, "
            f"{col_grants} column SELECT), 0 drift errors"
        )
    if args.render_sql_file:
        password = secrets.token_urlsafe(32)
        sql_text = _render_reader_setup_sql(db_name, _scram_sha256_verifier(password), grants)
        old_umask = os.umask(0o077)
        try:
            args.render_sql_file.write_text(sql_text, encoding="utf-8")
            if args.write_env_file:
                src_url = env_map.get(env_var) or os.getenv(env_var)
                if not src_url:
                    raise SystemExit(f"Missing {env_var} to build reader URL")
                reader_url = _build_reader_url(src_url, password)
                updated = _update_env_file_content(
                    args.write_env_file.read_text(encoding="utf-8"), {env_var: reader_url}
                )
                args.write_env_file.write_text(updated, encoding="utf-8")
        finally:
            os.umask(old_umask)
        print(
            f"{args.program}: wrote SQL to {args.render_sql_file} "
            f"({table_grants} table grants, {col_grants} column grants)"
        )
    if args.verify_reader:
        reader_url = env_map.get(env_var) or os.getenv(env_var)
        if not reader_url:
            raise SystemExit(f"Missing {env_var} for --verify-reader")
        _verify_live_reader(reader_url, config, source_columns)
        print(f"{args.program}: verified hc_warehouse_reader privileges and all {len(grants)} streams")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
