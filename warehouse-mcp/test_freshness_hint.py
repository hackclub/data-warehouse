"""Agents find freshness through SQL: the server points them to the views, with no extra tool."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("mcp")
pytest.importorskip("psycopg_pool")

sys.path.insert(0, str(Path(__file__).resolve().parent))

import server as mcp_server  # noqa: E402


def test_server_instructions_point_to_the_freshness_views():
    options = mcp_server.mcp.create_initialization_options()
    assert "warehouse_metadata.mirror_freshness" in options.instructions
    assert "deleted at the source" in options.instructions


def test_no_dedicated_freshness_tool_and_query_mentions_the_view():
    tools = {tool.name: tool for tool in asyncio.run(mcp_server.list_tools())}
    assert "check_freshness" not in tools
    assert "warehouse_metadata.mirror_freshness" in tools["query"].description
    assert "warehouse_metadata" in tools["list_schemas"].description


def test_list_schemas_response_points_to_the_freshness_view(monkeypatch):
    fake_db = MagicMock()
    fake_db.list_schemas = AsyncMock(return_value=["public", "warehouse_metadata"])
    monkeypatch.setattr(mcp_server, "get_database", lambda: fake_db)
    out = asyncio.run(mcp_server.call_tool("list_schemas", {}))
    assert "warehouse_metadata.mirror_freshness" in out[0].text
