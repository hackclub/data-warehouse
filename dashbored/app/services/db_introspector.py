import psycopg2

from .sensitive import is_sensitive


class ConnectionError(Exception):
    pass


EXCLUDED_TABLES = [
    "schema_migrations",
    "ar_internal_metadata",
    "_prisma_migrations",
    "pg_stat_statements",
    "pg_stat_statements_info",
]

EXCLUDED_PREFIXES = ["solid_queue_", "solid_cache_", "good_job_"]

CONNECT_TIMEOUT = 5
STATEMENT_TIMEOUT = "30s"


SYSTEM_SCHEMAS = frozenset([
    "pg_catalog", "information_schema", "pg_toast", "pg_temp_1", "pg_toast_temp_1",
])


class DbIntrospector:
    def __init__(self, url: str):
        self.url = url

    def introspect(self) -> list[dict]:
        conn = self._connect()
        try:
            tables = self._fetch_tables(conn)
            columns = self._fetch_columns(conn)
            primary_keys = self._fetch_primary_keys(conn)
            row_counts = self._fetch_row_counts(conn)

            return [
                {
                    "name": name,
                    "schema": schema,
                    "columns": [
                        {
                            "name": c["column_name"],
                            "type": c["data_type"],
                            "udt": c["udt_name"],
                            "nullable": c["is_nullable"] == "YES",
                            "default": c["column_default"],
                            "sensitive": is_sensitive(c["column_name"]),
                        }
                        for c in columns.get((schema, name), [])
                    ],
                    "primary_key": primary_keys.get((schema, name), []),
                    "row_count": row_counts.get((schema, name), 0),
                    "excluded": _is_excluded(name),
                    "has_updated_at": any(
                        c["column_name"] == "updated_at"
                        for c in columns.get((schema, name), [])
                    ),
                    "has_created_at": any(
                        c["column_name"] == "created_at"
                        for c in columns.get((schema, name), [])
                    ),
                }
                for schema, name in tables
            ]
        except psycopg2.Error as e:
            raise ConnectionError(f"Could not read database schema: {e}")
        finally:
            conn.close()

    def _connect(self):
        try:
            conn = psycopg2.connect(self.url, connect_timeout=CONNECT_TIMEOUT)
        except psycopg2.Error as e:
            raise ConnectionError(f"Could not connect to database: {e}")
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s", (STATEMENT_TIMEOUT,))
        except psycopg2.Error as e:
            conn.close()
            raise ConnectionError(f"Could not connect to database: {e}")
        return conn

    def _fetch_tables(self, conn) -> list[tuple[str, str]]:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schemaname, tablename FROM pg_tables
                WHERE schemaname NOT IN %s
                UNION ALL
                SELECT schemaname, matviewname FROM pg_matviews
                WHERE schemaname NOT IN %s
                ORDER BY 1, 2
            """, (tuple(SYSTEM_SCHEMAS), tuple(SYSTEM_SCHEMAS)))
            return [(r[0], r[1]) for r in cur.fetchall()]

    def _fetch_columns(self, conn) -> dict[tuple[str, str], list[dict]]:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name, column_name, data_type, udt_name,
                       is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema NOT IN %s
                ORDER BY table_schema, table_name, ordinal_position
            """, (tuple(SYSTEM_SCHEMAS),))
            result: dict[tuple[str, str], list[dict]] = {}
            for row in cur.fetchall():
                key = (row[0], row[1])
                if key not in result:
                    result[key] = []
                result[key].append({
                    "column_name": row[2],
                    "data_type": row[3],
                    "udt_name": row[4],
                    "is_nullable": row[5],
                    "column_default": row[6],
                })
            return result

    def _fetch_primary_keys(self, conn) -> dict[tuple[str, str], list[str]]:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_schema, tc.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema NOT IN %s
            """, (tuple(SYSTEM_SCHEMAS),))
            result: dict[tuple[str, str], list[str]] = {}
            for schema, table, col in cur.fetchall():
                result.setdefault((schema, table), []).append(col)
            return result

    def _fetch_row_counts(self, conn) -> dict[tuple[str, str], int]:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nspname, relname, reltuples::bigint AS count
                FROM pg_class
                JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                WHERE nspname NOT IN %s AND relkind IN ('r', 'm')
            """, (tuple(SYSTEM_SCHEMAS),))
            return {(r[0], r[1]): max(r[2], 0) for r in cur.fetchall()}


def _is_excluded(name: str) -> bool:
    if name in EXCLUDED_TABLES:
        return True
    return any(name.startswith(p) for p in EXCLUDED_PREFIXES)
