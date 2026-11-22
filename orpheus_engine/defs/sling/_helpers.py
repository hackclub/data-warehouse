from dagster import EnvVar, AssetExecutionContext
from typing import Mapping, Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import hashlib
import os
import psycopg2
from psycopg2 import sql


def _sling_connection_url(env_var_name: str) -> EnvVar:
    """
    Sling's Postgres driver (lib/pq) treats sslrootcert=system as a literal file
    path and passes channel_binding through as a server GUC parameter, while
    libpq/psycopg support both natively. When either is present, expose a
    process-local derived env var with those parameters removed; Sling still
    uses TLS (and system roots for sslmode=verify-full) while psycopg2/dbt/dlt
    keep the original env var untouched.
    """
    raw_url = os.getenv(env_var_name)
    if not raw_url:
        return EnvVar(env_var_name)

    parsed = urlparse(raw_url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    cleaned_query_pairs = [
        (key, value)
        for key, value in query_pairs
        if not (
            (key == "sslrootcert" and value == "system")
            or key == "channel_binding"
        )
    ]
    if len(cleaned_query_pairs) == len(query_pairs):
        return EnvVar(env_var_name)

    derived_env_var_name = f"{env_var_name}_SLING"
    cleaned_url = urlunparse(parsed._replace(query=urlencode(cleaned_query_pairs)))
    os.environ.setdefault(derived_env_var_name, cleaned_url)
    return EnvVar(derived_env_var_name)

def _safe_index_name(*parts: str) -> str:
    raw_name = "_".join(["idx", *parts])
    digest = hashlib.sha1(raw_name.encode("utf-8")).hexdigest()[:8]
    return f"{raw_name[:52]}_{digest}"


def _replication_index_specs(
    replication_config: Mapping[str, Any],
) -> list[tuple[str, str, tuple[str, ...]]]:
    """Derive (schema, table, columns) target index specs for a replication
    config's incremental streams: one index on the primary key (merge upserts)
    and one on the update key (MAX(update_key) cursor lookups).

    Wildcard streams ("public.*") can't be enumerated statically, so only
    explicitly listed streams are covered.
    """
    defaults = replication_config.get("defaults", {})
    specs: list[tuple[str, str, tuple[str, ...]]] = []
    for stream_name, stream_config in replication_config.get("streams", {}).items():
        if "*" in stream_name:
            continue
        merged = {**defaults, **(stream_config or {})}
        if merged.get("disabled") or merged.get("mode") != "incremental":
            continue

        source_table = stream_name.split(".", 1)[-1]
        target = merged.get("object", "").replace("{stream_table}", source_table)
        if "." not in target:
            continue
        schema_name, table_name = target.split(".", 1)

        primary_key = list(merged.get("primary_key") or [])
        update_key = merged.get("update_key")
        if primary_key:
            specs.append((schema_name, table_name, tuple(primary_key)))
        if update_key and [update_key] != primary_key:
            specs.append((schema_name, table_name, (update_key,)))
    return specs


def _drop_invalid_indexes(
    context: AssetExecutionContext,
    schema_name: str,
) -> None:
    """Drop INVALID indexes left by failed CREATE INDEX CONCURRENTLY attempts.

    A concurrent index build that is interrupted (timeout, deadlock, crash)
    leaves a non-functional INVALID index behind. IF NOT EXISTS sees the name
    and skips, so re-running the create is a no-op — the broken index is
    permanent unless explicitly dropped.
    """
    warehouse_url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        return

    conn = psycopg2.connect(warehouse_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_index i ON i.indexrelid = c.oid
                WHERE n.nspname = %s AND NOT i.indisvalid
            """, (schema_name,))
            invalid = [row[0] for row in cur.fetchall()]
            if not invalid:
                return
            context.log.warning(
                "Dropping %d INVALID indexes in %s: %s",
                len(invalid), schema_name, ", ".join(invalid),
            )
            for idx_name in invalid:
                cur.execute(sql.SQL("DROP INDEX IF EXISTS {}.{}").format(
                    sql.Identifier(schema_name), sql.Identifier(idx_name),
                ))
    finally:
        conn.close()


def _ensure_target_indexes(
    context: AssetExecutionContext,
    index_specs: list[tuple[str, str, tuple[str, ...]]],
) -> None:
    """CREATE INDEX CONCURRENTLY IF NOT EXISTS for (schema, table, columns)
    specs, skipping tables no sync has created yet."""
    warehouse_url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL is required for the target index preflight")

    if not index_specs:
        return

    conn = psycopg2.connect(warehouse_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            for schema_name in sorted({schema for schema, _, _ in index_specs}):
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
                )
            for schema_name, table_name, columns in index_specs:
                cursor.execute(
                    "SELECT to_regclass(%s)",
                    (sql.Identifier(schema_name, table_name).as_string(conn),),
                )
                if cursor.fetchone()[0] is None:
                    continue

                cursor.execute(
                    """
                    SELECT attname
                    FROM pg_attribute
                    WHERE attrelid = to_regclass(%s)
                      AND attnum > 0
                      AND NOT attisdropped
                    """,
                    (sql.Identifier(schema_name, table_name).as_string(conn),),
                )
                existing_columns = {row[0] for row in cursor.fetchall()}
                missing_columns = [
                    column for column in columns if column not in existing_columns
                ]
                if missing_columns:
                    context.log.warning(
                        "Skipping index on %s.%s(%s): missing column(s) %s. "
                        "The source schema likely renamed or dropped them.",
                        schema_name,
                        table_name,
                        ", ".join(columns),
                        ", ".join(missing_columns),
                    )
                    continue

                index_name = _safe_index_name(schema_name, table_name, *columns)
                context.log.info(
                    "Ensuring target index %s on %s.%s(%s)",
                    index_name,
                    schema_name,
                    table_name,
                    ", ".join(columns),
                )
                cursor.execute(
                    sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}.{} ({})").format(
                        sql.Identifier(index_name),
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    )
                )
    finally:
        conn.close()


def _ensure_incremental_target_indexes(
    context: AssetExecutionContext,
    replication_config: Mapping[str, Any],
) -> None:
    """Keep Sling incremental cursor lookups and merges off full-table scans."""
    _ensure_target_indexes(context, _replication_index_specs(replication_config))


