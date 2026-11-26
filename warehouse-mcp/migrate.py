"""Apply pending migrations to the MCP auth and audit database."""

import asyncio
import hashlib
import os
from pathlib import Path

from dotenv import load_dotenv
from psycopg import AsyncConnection
from psycopg.rows import tuple_row

MIGRATIONS = Path(__file__).with_name("migrations")


async def apply_auth_migrations(conn: AsyncConnection) -> list[str]:
    files = sorted(MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not files:
        raise RuntimeError("No MCP auth migrations found")

    applied = []
    async with conn.transaction():
        # Serialize both CLI runs and app startups in the same database.
        await conn.execute("SELECT pg_advisory_xact_lock(5740523001)")
        async with conn.cursor(row_factory=tuple_row) as cursor:
            await cursor.execute("SELECT to_regnamespace('mcp_auth') IS NOT NULL")
            has_schema = (await cursor.fetchone())[0]
        if not has_schema:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS mcp_auth")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS mcp_auth.schema_migrations (
                version text PRIMARY KEY,
                sha256 text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
        """)
        async with conn.cursor(row_factory=tuple_row) as cursor:
            await cursor.execute("SELECT version, sha256 FROM mcp_auth.schema_migrations")
            recorded = dict(await cursor.fetchall())
        for path in files:
            contents = path.read_bytes()
            checksum = hashlib.sha256(contents).hexdigest()
            if path.name in recorded:
                if recorded[path.name] != checksum:
                    raise RuntimeError(f"Applied MCP auth migration has changed: {path.name}")
                continue
            await conn.execute(contents.decode("utf-8"))
            await conn.execute(
                "INSERT INTO mcp_auth.schema_migrations (version, sha256) VALUES (%s, %s)",
                (path.name, checksum),
            )
            applied.append(path.name)
    return applied


async def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))
    dsn = os.environ.get("MCP_AUTH_DATABASE_URL")
    if not dsn:
        raise SystemExit("MCP_AUTH_DATABASE_URL is required")
    async with await AsyncConnection.connect(dsn, connect_timeout=10) as conn:
        applied = await apply_auth_migrations(conn)
    print("Applied " + ", ".join(applied) if applied else "MCP auth schema is up to date")


if __name__ == "__main__":
    asyncio.run(main())
