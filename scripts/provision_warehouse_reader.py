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
        "DECLARE",
        "  r record;",
        "BEGIN",
        f"  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {l_role}) THEN",
        f"    CREATE ROLE {q_role} {role_opts};",
        "  ELSE",
        f"    ALTER ROLE {q_role} WITH {role_opts};",
        "    FOR r IN",
        "      SELECT role_r.rolname",
        "      FROM pg_catalog.pg_auth_members m",
        "      JOIN pg_catalog.pg_roles member_r ON member_r.oid = m.member",
        "      JOIN pg_catalog.pg_roles role_r ON role_r.oid = m.roleid",
        f"      WHERE member_r.rolname = {l_role}",
        "    LOOP",
        f"      EXECUTE format('REVOKE %I FROM %I', r.rolname, {l_role});",
        "    END LOOP;",
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
    if query_pairs.get("sslmode") == "verify-full":
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


def _ensure_system_ssl_certs() -> None:
    """Populate SSL_CERT_FILE and SSL_CERT_DIR from system CA paths when unset
    so psycopg2-binary's bundled OpenSSL resolves sslrootcert=system."""
    import ssl

    verify_paths = ssl.get_default_verify_paths()
    if not os.environ.get("SSL_CERT_FILE"):
        for candidate in (
            verify_paths.cafile,
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/ssl/cert.pem",
            "/etc/pki/tls/certs/ca-bundle.crt",
        ):
            if candidate and os.path.isfile(candidate):
                os.environ["SSL_CERT_FILE"] = candidate
                break
    if not os.environ.get("SSL_CERT_DIR"):
        for candidate in (
            verify_paths.capath,
            "/etc/ssl/certs",
            "/etc/pki/tls/certs",
        ):
            if candidate and os.path.isdir(candidate):
                os.environ["SSL_CERT_DIR"] = candidate
                break


def _connect_pg(conn_url: str):
    import psycopg2

    _ensure_system_ssl_certs()
    return psycopg2.connect(conn_url)


def _fetch_live_catalog(conn_url: str) -> tuple[str, dict[str, list[str]]]:
    """Read table and column names from pg_catalog (pg_class / pg_attribute)
    rather than information_schema.columns so ungranted tables and excluded
    columns remain visible even when connected as hc_warehouse_reader."""
    conn = _connect_pg(conn_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
            cur.execute(
                "SELECT n.nspname || '.' || c.relname, a.attname "
                "FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
                "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') "
                "  AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
                "  AND n.nspname NOT LIKE 'pg_toast%' "
                "  AND n.nspname NOT LIKE 'pg_temp%' "
                "  AND a.attnum > 0 "
                "  AND NOT a.attisdropped "
                "ORDER BY n.nspname, c.relname, a.attnum"
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
    conn = _connect_pg(reader_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            if cur.fetchone()[0] != "on":
                raise RuntimeError("Expected transaction_read_only='on'")
            cur.execute(
                "SELECT rolconnlimit, rolsuper, rolcreatedb, rolcreaterole, "
                "rolreplication, rolbypassrls "
                "FROM pg_roles WHERE rolname = %s",
                (role_name,),
            )
            if cur.fetchone() != (4, False, False, False, False, False):
                raise RuntimeError(f"Unexpected role flags for {role_name}")
            cur.execute(
                "SELECT r.rolname "
                "FROM pg_catalog.pg_auth_members m "
                "JOIN pg_catalog.pg_roles u ON u.oid = m.member "
                "JOIN pg_catalog.pg_roles r ON r.oid = m.roleid "
                "WHERE u.rolname = %s "
                "ORDER BY r.rolname",
                (role_name,),
            )
            memberships = [row[0] for row in cur.fetchall()]
            if memberships:
                raise RuntimeError(
                    f"Unexpected role memberships for {role_name}: {memberships}"
                )
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
                cur.execute(
                    "SELECT has_table_privilege(%s, %s, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') "
                    "    OR has_any_column_privilege(%s, %s, 'INSERT, UPDATE, REFERENCES')",
                    (role_name, table_key, role_name, table_key),
                )
                if cur.fetchone()[0]:
                    raise RuntimeError(f"Unexpected write privilege on {table_key}")

                if not table_key.startswith("public."):
                    continue
                cur.execute(
                    "SELECT has_any_column_privilege(%s, %s, 'SELECT')",
                    (role_name, table_key),
                )
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
                            raise RuntimeError(
                                f"Column privilege mismatch on {table_key}.{col}"
                            )
                elif has_any:
                    raise RuntimeError(
                        f"Unexpected SELECT privilege on unmirrored {table_key}"
                    )

            first_stream, first_cols = next(iter(grants.items()))
            s_name, t_name = first_stream.split(".", 1)
            probe_col = (first_cols or source_columns[first_stream])[0]
            q_table = f"{_quote_ident(s_name)}.{_quote_ident(t_name)}"
            q_col = _quote_ident(probe_col)
            insert_sql = (
                f"INSERT INTO {q_table} ({q_col}) "
                f"SELECT {q_col} FROM {q_table} WHERE false"
            )

            cur.execute("SAVEPOINT _verify_ro_insert")
            try:
                cur.execute(insert_sql)
            except psycopg2.Error as exc:
                cur.execute("ROLLBACK TO SAVEPOINT _verify_ro_insert")
                if exc.pgcode not in ("25006", "42501"):
                    raise RuntimeError(
                        f"INSERT failed with unexpected SQLSTATE {exc.pgcode}"
                    ) from exc
            else:
                cur.execute("ROLLBACK TO SAVEPOINT _verify_ro_insert")
                raise RuntimeError(f"INSERT into {first_stream} unexpectedly succeeded")
            finally:
                cur.execute("RELEASE SAVEPOINT _verify_ro_insert")

            conn.rollback()
            cur.execute("SET TRANSACTION READ WRITE")
            cur.execute("SAVEPOINT _verify_acl_insert")
            try:
                cur.execute(insert_sql)
            except psycopg2.Error as exc:
                cur.execute("ROLLBACK TO SAVEPOINT _verify_acl_insert")
                if exc.pgcode != "42501":
                    raise RuntimeError(
                        f"INSERT with transaction_read_only=off failed with unexpected SQLSTATE {exc.pgcode}"
                    ) from exc
            else:
                cur.execute("ROLLBACK TO SAVEPOINT _verify_acl_insert")
                raise RuntimeError(
                    f"INSERT into {first_stream} succeeded when transaction_read_only=off"
                )
            finally:
                cur.execute("RELEASE SAVEPOINT _verify_acl_insert")
                conn.rollback()

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
            conn.rollback()
    finally:
        conn.close()


def _resolve_warehouse_table(
    program: str,
    stream_name: str,
    merged_stream_config: Mapping[str, Any],
) -> tuple[str, str]:
    _, src_table = stream_name.split(".", 1)
    obj_template = str(merged_stream_config.get("object") or f"{program}.{{stream_table}}")
    rendered = obj_template.replace("{stream_table}", src_table)
    if "." not in rendered:
        return program, rendered
    wh_schema, wh_table = rendered.split(".", 1)
    return wh_schema, wh_table


def _fetch_live_row_counts(
    source_url: str,
    warehouse_url: str,
    program: str,
    replication_config: Mapping[str, Any],
) -> dict[str, tuple[int, int | None]]:
    defaults = replication_config.get("defaults") or {}
    streams = replication_config.get("streams") or {}
    counts: dict[str, tuple[int, int | None]] = {}

    src_conn = _connect_pg(source_url)
    wh_conn = _connect_pg(warehouse_url)
    try:
        with src_conn.cursor() as src_cur, wh_conn.cursor() as wh_cur:
            for stream_name, stream_cfg in streams.items():
                merged = {**defaults, **(stream_cfg or {})}
                if merged.get("disabled"):
                    continue
                if merged.get("sql"):
                    src_q = f"SELECT count(*) FROM ({merged['sql']}) AS _src_cnt"
                else:
                    s_name, t_name = stream_name.split(".", 1)
                    src_q = f"SELECT count(*) FROM {_quote_ident(s_name)}.{_quote_ident(t_name)}"
                src_cur.execute(src_q)
                src_count = int(src_cur.fetchone()[0])

                wh_schema, wh_table = _resolve_warehouse_table(program, stream_name, merged)
                wh_cur.execute(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM pg_catalog.pg_class c "
                    "  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "  WHERE n.nspname = %s AND c.relname = %s "
                    "    AND c.relkind IN ('r', 'p', 'v', 'm', 'f')"
                    ")",
                    (wh_schema, wh_table),
                )
                if not wh_cur.fetchone()[0]:
                    wh_count: int | None = None
                else:
                    wh_cur.execute(
                        f"SELECT count(*) FROM {_quote_ident(wh_schema)}.{_quote_ident(wh_table)}"
                    )
                    wh_count = int(wh_cur.fetchone()[0])
                counts[stream_name] = (src_count, wh_count)
        return counts
    finally:
        src_conn.close()
        wh_conn.close()


def _write_counts_json(
    path: Path, program: str, counts: Mapping[str, tuple[int, int | None]]
) -> None:
    payload = {
        "program": program,
        "counts": {k: [v[0], v[1]] for k, v in counts.items()},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_counts_json(path: Path) -> dict[str, tuple[int, int | None]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload["counts"] if isinstance(payload, dict) and "counts" in payload else payload
    return {
        str(k): (int(v[0]), None if v[1] is None else int(v[1]))
        for k, v in raw.items()
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision and verify hc_warehouse_reader roles.")
    parser.add_argument("--program", required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--env-var")
    parser.add_argument("--warehouse-env-var", default="WAREHOUSE_COOLIFY_URL")
    parser.add_argument("--catalog-json", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--render-sql-file", type=Path)
    parser.add_argument("--write-env-file", type=Path)
    parser.add_argument("--verify-reader", action="store_true")
    parser.add_argument("--record-counts-json", type=Path)
    parser.add_argument("--compare-counts-json", type=Path)
    parser.add_argument("--post-counts-json", type=Path)
    args = parser.parse_args(argv)

    if bool(args.render_sql_file) != bool(args.write_env_file):
        parser.error(
            "--render-sql-file and --write-env-file must be provided together "
            "so a generated password is never discarded"
        )
    if args.post_counts_json and not args.compare_counts_json:
        parser.error("--post-counts-json requires --compare-counts-json")
    if not any(
        (
            args.check,
            args.render_sql_file,
            args.verify_reader,
            args.record_counts_json,
            args.compare_counts_json,
        )
    ):
        parser.error(
            "At least one action flag (--check, --render-sql-file, --verify-reader, "
            "--record-counts-json, --compare-counts-json) is required"
        )

    mod = importlib.import_module(f"orpheus_engine.defs.sling.programs.{args.program}")
    config = getattr(mod, f"{args.program}_replication_config")
    env_var = args.env_var or PROGRAM_ENV_VARS.get(args.program, f"{args.program.upper()}_K8S_URL")
    env_map = _parse_env_text(args.env_file.read_text(encoding="utf-8")) if args.env_file else {}

    if args.check or args.render_sql_file or args.verify_reader:
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
            assert args.write_env_file is not None
            if not args.write_env_file.is_file():
                raise SystemExit(
                    f"Target --write-env-file does not exist: {args.write_env_file}"
                )
            if not args.render_sql_file.parent.is_dir():
                raise SystemExit(
                    f"Parent directory for --render-sql-file does not exist: {args.render_sql_file.parent}"
                )
            target_env_text = args.write_env_file.read_text(encoding="utf-8")
            target_env_map = _parse_env_text(target_env_text)
            src_url = (
                env_map.get(env_var)
                or target_env_map.get(env_var)
                or os.getenv(env_var)
            )
            if not src_url:
                raise SystemExit(f"Missing {env_var} to build reader URL")
            password = secrets.token_urlsafe(32)
            reader_url = _build_reader_url(src_url, password)
            updated_env = _update_env_file_content(
                target_env_text, {env_var: reader_url}
            )
            sql_text = _render_reader_setup_sql(
                db_name, _scram_sha256_verifier(password), grants
            )
            old_umask = os.umask(0o077)
            try:
                args.write_env_file.write_text(updated_env, encoding="utf-8")
                args.render_sql_file.write_text(sql_text, encoding="utf-8")
            finally:
                os.umask(old_umask)
            print(
                f"{args.program}: wrote SQL to {args.render_sql_file} and updated {args.write_env_file} "
                f"({table_grants} table grants, {col_grants} column grants)"
            )
        if args.verify_reader:
            reader_url = env_map.get(env_var) or os.getenv(env_var)
            if not reader_url:
                raise SystemExit(f"Missing {env_var} for --verify-reader")
            _verify_live_reader(reader_url, config, source_columns)
            print(
                f"{args.program}: verified hc_warehouse_reader privileges and all {len(grants)} streams"
            )

    live_counts: dict[str, tuple[int, int | None]] | None = None
    if args.record_counts_json or (args.compare_counts_json and not args.post_counts_json):
        src_url = env_map.get(env_var) or os.getenv(env_var)
        if not src_url:
            raise SystemExit(f"Missing {env_var} for row-count query")
        wh_url = env_map.get(args.warehouse_env_var) or os.getenv(args.warehouse_env_var)
        if not wh_url:
            raise SystemExit(
                f"Missing {args.warehouse_env_var} for warehouse row-count query"
            )
        live_counts = _fetch_live_row_counts(src_url, wh_url, args.program, config)

    if args.record_counts_json:
        assert live_counts is not None
        _write_counts_json(args.record_counts_json, args.program, live_counts)
        print(
            f"{args.program}: recorded row counts for {len(live_counts)} streams to {args.record_counts_json}"
        )

    if args.compare_counts_json:
        pre_counts = _read_counts_json(args.compare_counts_json)
        post_counts = (
            _read_counts_json(args.post_counts_json)
            if args.post_counts_json
            else live_counts
        )
        assert post_counts is not None
        failures = _check_row_count_parity(config, pre_counts, post_counts)
        if failures:
            for msg in failures:
                print(f"{args.program}: PARITY FAILURE: {msg}")
            raise SystemExit(
                f"{args.program}: row-count parity failed ({len(failures)} stream(s))"
            )
        print(
            f"{args.program}: row-count parity passed for {len(post_counts)} streams"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
