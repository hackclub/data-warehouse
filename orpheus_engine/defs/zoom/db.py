import json
import os

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


SCHEMA_NAME = "zoom"


def get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def ensure_table(conn, table_name: str, columns_sql: str, indexes: list[str] | None = None):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(SCHEMA_NAME)
        ))
        cur.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} ({columns_sql})")
        for idx_sql in (indexes or []):
            cur.execute(idx_sql)
    conn.commit()


def _dedupe_on_conflict_key(columns: list[str], rows: list[tuple],
                            conflict_column: str) -> list[tuple]:
    """
    Collapse rows that share a conflict key, keeping the LAST occurrence.

    Postgres raises "ON CONFLICT DO UPDATE command cannot affect row a second
    time" (CardinalityViolation) when a single INSERT ... ON CONFLICT statement
    carries two rows with the same conflict target. execute_values splits rows
    into pages, so a duplicate pair only has to land in the same page to abort
    the whole statement -- which makes the failure look intermittent.

    Callers are still responsible for choosing a conflict key that is actually
    the natural key of the row. This is a safety net for sources that genuinely
    repeat a key within one pull (Zoom does this for poll responses), NOT a
    substitute for a correct key: deduping on a too-narrow key silently discards
    distinct records.
    """
    key_names = [c.strip().strip('"') for c in conflict_column.split(",")]
    try:
        key_idx = [columns.index(k) for k in key_names]
    except ValueError:
        # Conflict target isn't a plain column of this insert (e.g. an
        # expression index). Can't compute keys, so leave the batch untouched.
        return rows

    deduped: dict[tuple, tuple] = {}
    for row in rows:
        deduped[tuple(row[i] for i in key_idx)] = row
    return list(deduped.values())


def upsert_rows(conn, table_name: str, columns: list[str], rows: list[tuple],
                conflict_column: str, update_columns: list[str] | None = None,
                template: str | None = None):
    if not rows:
        return
    rows = _dedupe_on_conflict_key(columns, rows, conflict_column)
    cols = ", ".join(columns)
    placeholders = template or ("(" + ", ".join(["%s"] * len(columns)) + ")")
    if update_columns:
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        conflict_clause = f"ON CONFLICT ({conflict_column}) DO UPDATE SET {update_set}"
    else:
        conflict_clause = f"ON CONFLICT ({conflict_column}) DO NOTHING"
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {SCHEMA_NAME}.{table_name} ({cols}) VALUES %s {conflict_clause}",
            rows,
            template=placeholders,
            page_size=500,
        )
    conn.commit()


def clean_json(obj) -> str:
    return json.dumps(obj or {}).replace("\\u0000", "")
