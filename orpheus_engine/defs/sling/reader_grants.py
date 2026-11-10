"""
What hc_warehouse_reader should be able to read, and what it can read.

Shared by scripts/provision_warehouse_reader.py and the daily drift job
(reader_grant_drift.py), so both derive grants and compare privileges one way.
Only catalog metadata is read here, never row data.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

READER_ROLE = "hc_warehouse_reader"

_SQL_STRING_LITERAL_RE = re.compile(r"'[^']*'")
_SQL_AS_ALIAS_RE = re.compile(r"\bas\s+[A-Za-z_][A-Za-z0-9_]*\b", re.IGNORECASE)
_SQL_FROM_CLAUSE_RE = re.compile(
    r"\bfrom\s+[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\b", re.IGNORECASE
)
_SQL_FUNC_CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_SQL_IDENT_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_SQL_KEYWORDS = {"select", "from", "where", "and", "or", "not", "null", "true", "false"}


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


def _connect_pg(conn_url: str, **connect_kwargs: Any):
    import psycopg2

    _ensure_system_ssl_certs()
    return psycopg2.connect(conn_url, **connect_kwargs)


@dataclass(frozen=True)
class TablePrivileges:
    """One table's OID and, per column in table order, whether the connected
    login can SELECT it."""

    oid: int
    columns: Mapping[str, bool]


def _fetch_catalog_privileges(cur) -> dict[str, TablePrivileges]:
    """Read every table, its columns and the connected login's SELECT privilege
    in one query.

    Uses pg_catalog (pg_class / pg_attribute) rather than
    information_schema.columns so ungranted tables and excluded columns stay
    visible when connected as hc_warehouse_reader. has_column_privilege is also
    true for a column covered by a table-level grant.
    """
    cur.execute(
        "SELECT n.nspname || '.' || c.relname, c.oid, a.attname, "
        "       has_column_privilege(c.oid, a.attnum, 'SELECT') "
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
    tables: dict[str, TablePrivileges] = {}
    for table_key, table_oid, col_name, can_select in cur.fetchall():
        table = tables.setdefault(table_key, TablePrivileges(int(table_oid), {}))
        table.columns[col_name] = bool(can_select)
    return tables


def _column_grant_mismatches(
    column_privileges: Mapping[str, bool],
    kept_columns: Sequence[str] | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Compare a table's live privileges with its derived grant (None is a whole
    table grant). Returns (kept columns not readable, other columns readable)."""
    kept = tuple(column_privileges) if kept_columns is None else tuple(kept_columns)
    kept_set = set(kept)
    not_granted = tuple(col for col in kept if not column_privileges.get(col, False))
    unexpected = tuple(
        col for col, can_select in column_privileges.items()
        if can_select and col not in kept_set
    )
    return not_granted, unexpected


def _fetch_live_catalog(conn_url: str) -> tuple[str, dict[str, list[str]]]:
    """Return the database name and every table's column names, for any login."""
    conn = _connect_pg(conn_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
            tables = _fetch_catalog_privileges(cur)
            return db_name, {key: list(table.columns) for key, table in tables.items()}
    finally:
        conn.close()
