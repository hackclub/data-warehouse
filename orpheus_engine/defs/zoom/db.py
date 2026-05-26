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


def upsert_rows(conn, table_name: str, columns: list[str], rows: list[tuple],
                conflict_column: str, update_columns: list[str] | None = None,
                template: str | None = None):
    if not rows:
        return
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
