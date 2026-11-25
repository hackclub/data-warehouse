"""
HTTP MCP server for read-only data warehouse queries.

Provides tools for:
- query: Execute SQL and cache results
- get_rows: Paginate cached results
- grep_rows: Search within cached results
- list_schemas: List database schemas
- describe_schema: Get schema documentation with sample data
"""

import asyncio
import contextlib
import contextvars
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import Tool, TextContent
from psycopg_pool import PoolTimeout
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

from auth_store import digest
from oauth import OAuth

import psycopg
from db import get_database, init_database, close_database, SQLValidationError
from cache import get_cache

# Load environment variables
load_dotenv(Path(__file__).with_name(".env"))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Current user context (firstname, lastname) for SQL query attribution
_current_user: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar('current_user', default=None)
# Short SHA-256 prefix of the authenticated bearer token (never the token itself)
_current_token_fp: contextvars.ContextVar[str | None] = contextvars.ContextVar('current_token_fp', default=None)

_current_identity: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "current_identity", default=None
)
_current_auth: contextvars.ContextVar[OAuth | None] = contextvars.ContextVar(
    "current_auth", default=None
)
_current_session_hash: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_session_hash", default=None
)

AUDIT_SQL_MAX_CHARS = 4000


def _token_fingerprint(token: str) -> str:
    return digest(token)[:12]


def _format_caller(user_info: tuple[str, str] | None) -> str:
    if not user_info:
        return "unknown"
    parts = [p.strip() for p in user_info if p and p.strip()]
    return " ".join(parts) if parts else "unknown"


async def _emit_audit_log(
    *,
    tool: str,
    arguments: dict[str, Any],
    user_info: tuple[str, str] | None,
    token_fp: str | None,
    outcome: str,
    duration_ms: int,
    row_count: int | None = None,
    query_id: str | None = None,
    error: str | None = None,
    event: str = "mcp_tool_call",
    request_id: str | None = None,
) -> None:
    record: dict[str, Any] = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "caller": _format_caller(user_info),
        "token_fingerprint": token_fp or "none",
        "outcome": outcome,
        "duration_ms": duration_ms,
    }
    identity = _current_identity.get()
    if request_id:
        record["request_id"] = request_id
    if identity:
        record["caller_subject"] = identity["subject"]
        record["caller_email"] = identity["email"]
    if tool == "query" and isinstance(arguments.get("sql"), str):
        sql = arguments["sql"]
        record["sql"] = sql[:AUDIT_SQL_MAX_CHARS]
        if len(sql) > AUDIT_SQL_MAX_CHARS:
            record["sql_truncated"] = True
    for key in ("offset", "limit", "preview_rows"):
        if isinstance(arguments.get(key), int):
            record[key] = arguments[key]
    if tool == "grep_rows" and isinstance(arguments.get("pattern"), str):
        record["pattern"] = arguments["pattern"][:AUDIT_SQL_MAX_CHARS]
    for argument, field in (("schema_name", "schema"), ("table_name", "table")):
        if isinstance(arguments.get(argument), str):
            record[field] = arguments[argument][:500]
    if query_id is not None:
        record["query_id"] = query_id
    elif isinstance(arguments.get("query_id"), str):
        record["query_id"] = arguments["query_id"][:500]
    if row_count is not None:
        record["row_count"] = row_count
    if error is not None:
        record["error"] = error[:500]
    # PostgreSQL JSONB cannot store NUL or unpaired surrogate code points.
    record = {
        key: value.replace("\x00", "\ufffd").encode("utf-8", "replace").decode()
        if isinstance(value, str) else value
        for key, value in record.items()
    }
    if event == "mcp_tool_call":
        logger.info("mcp_audit %s", json.dumps(record, separators=(",", ":")))

    auth = _current_auth.get()
    if auth:
        try:
            await auth.store.audit(record)
        except (psycopg.Error, PoolTimeout):
            logger.error("Could not write the MCP audit record to Postgres")
            raise RuntimeError("Request audit is temporarily unavailable") from None


# Create MCP server
FRESHNESS_HINT = (
    "Before trusting recent data, query warehouse_metadata.mirror_freshness "
    "(one row per schema: status, last_synced_at, age_hours, is_stale) or "
    "warehouse_metadata.asset_freshness (one row per asset). It shows sync "
    "time only: incremental mirrors may keep rows deleted at the source."
)

mcp = Server("warehouse-mcp", instructions=FRESHNESS_HINT)

# HTTP transport - messages path (root for Streamable HTTP compatibility)
session_manager = StreamableHTTPSessionManager(app=mcp, stateless=True)


def format_rows_for_display(rows: list[dict[str, Any]], columns: list[str], truncate_values: bool = False) -> str:
    """Format rows as a readable table string.

    Args:
        rows: List of row dictionaries
        columns: Column names in display order
        truncate_values: If True, truncate cell values to 100 characters (for previews)
    """
    if not rows:
        return "(no rows)"

    max_value_len = 100 if truncate_values else None

    # Convert all values to strings, handling None and optional truncation
    str_rows = []
    for row in rows:
        str_row = {}
        for k, v in row.items():
            if v is None:
                str_row[k] = "NULL"
            else:
                s = str(v)
                if max_value_len and len(s) > max_value_len:
                    s = s[:max_value_len] + "..."
                str_row[k] = s
        str_rows.append(str_row)

    # Calculate column widths based on actual content
    widths = {col: len(col) for col in columns}
    for row in str_rows:
        for col in columns:
            widths[col] = max(widths[col], len(row.get(col, "")))

    # Build table
    lines = []

    # Header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("-" * len(header))

    # Rows
    for row in str_rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col])
            for col in columns
        )
        lines.append(line)

    return "\n".join(lines)


@mcp.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="query",
            description=(
                "Execute a read-only SQL query against the data warehouse. "
                "Returns a query_id for fetching more rows, plus a preview of results. "
                "Use LIMIT in your SQL for large tables. Max 10,000 rows cached per query.\n\n"
                + FRESHNESS_HINT
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute"
                    },
                    "preview_rows": {
                        "type": "integer",
                        "description": "Number of preview rows to return (default: 50)",
                        "default": 50
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_rows",
            description=(
                "Get rows from a cached query result using pagination. "
                "Use the query_id returned from the query tool."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query_id": {
                        "type": "string",
                        "description": "Query ID from a previous query"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Starting row index (default: 0)",
                        "default": 0
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return (default: 50)",
                        "default": 50
                    }
                },
                "required": ["query_id"]
            }
        ),
        Tool(
            name="grep_rows",
            description=(
                "Search for rows matching a regex pattern in a cached query result. "
                "Searches across all columns. Case-insensitive."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query_id": {
                        "type": "string",
                        "description": "Query ID from a previous query"
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum matching rows to return (default: 100)",
                        "default": 100
                    }
                },
                "required": ["query_id", "pattern"]
            }
        ),
        Tool(
            name="list_schemas",
            description=(
                "List all available schemas in the data warehouse.\n\n"
                "Key schemas:\n"
                "- warehouse_metadata: Sync freshness (mirror_freshness per schema, asset_freshness per asset)\n"
                "- airtable_unified_ysws_projects_db: Shipped projects and NPS on programs\n"
                "- public_unified_analytics: User histories and events on users\n"
                "- loops.audience: User profile fields (geocoded country, etc.)\n"
                "- public_hackatime_analytics: Coding activity (preferred over hackatime schema)"
            ),
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="describe_schema",
            description=(
                "Get detailed documentation for a schema including tables, columns, "
                "and sample data. Limited to 1000 columns per table. "
                "Use list_columns for tables with more columns."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema to describe"
                    }
                },
                "required": ["schema_name"]
            }
        ),
        Tool(
            name="list_columns",
            description=(
                "List all columns for a specific table with pagination. "
                "Use this for wide tables with >100 columns that get truncated in describe_schema."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Starting column index (default: 0)",
                        "default": 0
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum columns to return (default: 100)",
                        "default": 100
                    }
                },
                "required": ["schema_name", "table_name"]
            }
        )
    ]


@mcp.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    db = get_database()
    cache = get_cache()
    user_info = _current_user.get()
    token_fp = _current_token_fp.get()
    identity = _current_identity.get()
    subject = identity["subject"] if identity else None
    started = time.monotonic()
    outcome = "ok"
    row_count: int | None = None
    query_id_out: str | None = None
    error_out: str | None = None
    request_id = uuid4().hex
    auth = _current_auth.get()
    session_hash = _current_session_hash.get()

    if auth:
        await _emit_audit_log(
            tool=name, arguments=arguments, user_info=user_info, token_fp=token_fp,
            outcome="started", duration_ms=0, event="mcp_tool_started", request_id=request_id,
        )

    try:
        if name == "query":
            sql = arguments["sql"]
            preview_rows = arguments.get("preview_rows", 50)

            # Execute query with user attribution
            rows, columns = await db.execute_query(sql, user_info=user_info)
            
            # Cache results
            entry = cache.store(sql, rows, columns, subject=subject)
            row_count = entry.row_count
            query_id_out = entry.query_id
            
            # Format response
            preview = rows[:preview_rows]
            table = format_rows_for_display(preview, columns, truncate_values=True)
            
            result = (
                f"Query ID: {entry.query_id}\n"
                f"Total rows: {entry.row_count}\n"
                f"Columns: {', '.join(columns)}\n\n"
                f"Preview ({len(preview)} of {entry.row_count} rows):\n{table}"
            )
            
            if entry.row_count > preview_rows:
                result += f"\n\nUse get_rows(query_id='{entry.query_id}', offset={preview_rows}) for more rows."
            
            return [TextContent(type="text", text=result)]
        
        elif name == "get_rows":
            query_id = arguments["query_id"]
            offset = arguments.get("offset", 0)
            limit = arguments.get("limit", 50)
            
            result = cache.get_rows(query_id, offset, limit, subject=subject)
            if result is None:
                outcome = "not_found"
                return [TextContent(
                    type="text",
                    text=f"Query ID '{query_id}' not found or expired. Run a new query."
                )]
            
            rows, total = result
            row_count = len(rows)
            entry = cache.get(query_id, subject=subject)
            columns = entry.columns if entry else []
            
            table = format_rows_for_display(rows, columns)
            
            response = (
                f"Rows {offset + 1}-{offset + len(rows)} of {total}:\n{table}"
            )
            
            if offset + len(rows) < total:
                response += f"\n\nMore rows available. Use offset={offset + limit} to continue."
            
            return [TextContent(type="text", text=response)]
        
        elif name == "grep_rows":
            query_id = arguments["query_id"]
            pattern = arguments["pattern"]
            limit = arguments.get("limit", 100)
            
            result = cache.grep_rows(query_id, pattern, limit, subject=subject)
            if result is None:
                outcome = "not_found"
                return [TextContent(
                    type="text",
                    text=f"Query ID '{query_id}' not found or expired. Run a new query."
                )]
            
            rows, total_matches = result
            row_count = len(rows)
            entry = cache.get(query_id, subject=subject)
            columns = entry.columns if entry else []
            
            table = format_rows_for_display(rows, columns)
            
            response = (
                f"Found {total_matches} rows matching '{pattern}'.\n"
                f"Showing {len(rows)} matches:\n{table}"
            )
            
            return [TextContent(type="text", text=response)]
        
        elif name == "list_schemas":
            schemas = await db.list_schemas()
            row_count = len(schemas)
            
            response = "Available schemas:\n"
            for schema in schemas:
                response += f"  - {schema}\n"
            response += (
                "\nUse describe_schema(schema_name='...') to see tables and columns.\n"
                + FRESHNESS_HINT
            )
            
            return [TextContent(type="text", text=response)]
        
        elif name == "describe_schema":
            schema_name = arguments["schema_name"]
            description = await db.describe_schema(schema_name)
            return [TextContent(type="text", text=description)]
        
        elif name == "list_columns":
            schema_name = arguments["schema_name"]
            table_name = arguments["table_name"]
            offset = arguments.get("offset", 0)
            limit = arguments.get("limit", 100)
            
            columns, total = await db.list_columns(schema_name, table_name, offset, limit)
            row_count = len(columns)
            
            if not columns:
                return [TextContent(
                    type="text",
                    text=f"No columns found for {schema_name}.{table_name}"
                )]
            
            # Format as a table
            lines = [
                f"Columns for {schema_name}.{table_name} ({offset + 1}-{offset + len(columns)} of {total}):",
                "",
                "| Column | Type | Nullable | Default |",
                "|--------|------|----------|---------|"
            ]
            
            for col in columns:
                col_name = col['column_name']
                dtype = col['data_type']
                if col.get('character_maximum_length'):
                    dtype += f"({col['character_maximum_length']})"
                nullable = "YES" if col['is_nullable'] == 'YES' else "NO"
                default = str(col['column_default'])[:30] if col['column_default'] else ""
                lines.append(f"| {col_name} | {dtype} | {nullable} | {default} |")
            
            if offset + len(columns) < total:
                lines.append("")
                lines.append(f"More columns available. Use offset={offset + limit} to continue.")
            
            return [TextContent(type="text", text="\n".join(lines))]
        
        else:
            outcome = "unknown_tool"
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    except SQLValidationError as e:
        outcome = "validation_error"
        error_out = str(e)
        # Safe to expose - these are intentional validation messages
        return [TextContent(type="text", text=f"SQL validation error: {e}")]
    except ValueError as e:
        outcome = "value_error"
        error_out = str(e)
        # Safe to expose - argument validation errors
        return [TextContent(type="text", text=f"Invalid argument: {e}")]
    except psycopg.Error as e:
        outcome = "db_error"
        # Safe to expose - PostgreSQL errors help users fix their queries
        error_msg = e.diag.message_primary if e.diag and e.diag.message_primary else str(e)
        error_out = error_msg
        return [TextContent(type="text", text=f"Database error: {error_msg}")]
    except Exception as e:
        outcome = "internal_error"
        error_out = type(e).__name__
        # Log full exception for debugging, but don't expose details to client
        logger.exception("Tool execution error")
        # Return generic message to avoid leaking internal information
        return [TextContent(type="text", text="An internal error occurred. Please try again or contact support if the issue persists.")]
    finally:
        session_error = None
        if auth and session_hash and outcome != "cancelled":
            try:
                if not await auth.store.session_user_by_hash(session_hash, "mcp", auth.origin):
                    outcome = "access_expired_or_revoked"
                    session_error = "Warehouse authorization expired or was revoked"
            except (psycopg.Error, PoolTimeout):
                outcome = "authorization_unavailable"
                session_error = "Warehouse authorization is temporarily unavailable"
        await _emit_audit_log(
            tool=name,
            arguments=arguments,
            user_info=user_info,
            token_fp=token_fp,
            outcome=outcome,
            duration_ms=int((time.monotonic() - started) * 1000),
            row_count=row_count,
            query_id=query_id_out,
            error=error_out,
            request_id=request_id,
        )
        if session_error:
            raise RuntimeError(session_error)



async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    request = Request(scope, receive, send)
    path, method = scope["path"], scope["method"]
    if path == "/health" and method == "GET":
        await JSONResponse({"status": "ok", "service": "warehouse-mcp"})(scope, receive, send)
        return

    auth = request.state.auth
    if request.headers.get("host") != auth.host:
        await JSONResponse({"error": "Invalid host"}, 421)(scope, receive, send)
        return
    try:
        response = await auth.route(request)
        if response is not None:
            await response(scope, receive, send)
            return
        if path != "/" or method not in {"GET", "POST", "DELETE"}:
            await JSONResponse({"error": "Not found"}, 404)(scope, receive, send)
            return
        if request.headers.get("origin", auth.origin) != auth.origin:
            await JSONResponse({"error": "Invalid origin"}, 403)(scope, receive, send)
            return
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        identity = None
        if scheme.lower() == "bearer" and len(token) == 43:
            identity = await auth.store.session_user(token, "mcp", auth.origin)
    except (psycopg.Error, PoolTimeout):
        logger.error("MCP authentication database unavailable")
        await JSONResponse({"error": "Service unavailable"}, 503)(scope, receive, send)
        return

    if not identity:
        response = JSONResponse({"error": "Unauthorized"}, 401, headers={
            "WWW-Authenticate": f'Bearer resource_metadata="{auth.origin}/.well-known/oauth-protected-resource"',
        })
        await response(scope, receive, send)
        return

    user_ctx = _current_user.set((identity["first_name"], identity["last_name"]))
    fp_ctx = _current_token_fp.set(_token_fingerprint(token))
    identity_ctx = _current_identity.set(identity)
    auth_ctx = _current_auth.set(auth)
    session_ctx = _current_session_hash.set(digest(token))
    try:
        await session_manager.handle_request(scope, receive, send)
    finally:
        _current_session_hash.reset(session_ctx)
        _current_auth.reset(auth_ctx)
        _current_identity.reset(identity_ctx)
        _current_token_fp.reset(fp_ctx)
        _current_user.reset(user_ctx)


@contextlib.asynccontextmanager
async def lifespan(app):
    auth = OAuth()
    async with auth.http, contextlib.AsyncExitStack() as stack:
        stack.push_async_callback(auth.store.pool.close)
        await auth.store.open()
        stack.push_async_callback(close_database)
        warehouse = await init_database()
        await warehouse.check_auth_database_isolation()
        await stack.enter_async_context(session_manager.run())
        yield {"auth": auth}


def create_app():
    starlette_app = Starlette(lifespan=lifespan)

    async def wrapped_app(scope, receive, send):
        if scope["type"] == "lifespan":
            await starlette_app(scope, receive, send)
        else:
            await app(scope, receive, send)

    return wrapped_app


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 3000))
    host = os.environ.get("HOST", "0.0.0.0")

    logger.info("Starting warehouse-mcp server on %s:%s", host, port)
    # Request URLs can contain OAuth callback codes and state.
    uvicorn.run(create_app(), host=host, port=port, access_log=False)
