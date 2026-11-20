"""Tests for structured caller-identity and SQL audit logging in warehouse-mcp."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("mcp")
pytest.importorskip("psycopg_pool")

sys.path.insert(0, str(Path(__file__).resolve().parent))

import server as mcp_server  # noqa: E402
from db import SQLValidationError  # noqa: E402


def _audit_records(caplog) -> list[dict]:
    records = []
    for rec in caplog.records:
        msg = rec.getMessage()
        if msg.startswith("mcp_audit "):
            records.append(json.loads(msg[len("mcp_audit "):]))
    return records


def test_query_emits_structured_audit_log_without_leaking_token(monkeypatch, caplog):
    fake_db = MagicMock()
    fake_db.execute_query = AsyncMock(return_value=([{"n": 42}], ["n"]))
    monkeypatch.setattr(mcp_server, "get_database", lambda: fake_db)

    raw_token = base64.b64encode(b"secret-auth-token:Ada:Lovelace").decode()
    fp = mcp_server._token_fingerprint(raw_token)
    u_tok = mcp_server._current_user.set(("Ada", "Lovelace"))
    f_tok = mcp_server._current_token_fp.set(fp)
    try:
        with caplog.at_level(logging.INFO, logger=mcp_server.logger.name):
            asyncio.run(mcp_server.call_tool("query", {"sql": "SELECT 42 AS n"}))
    finally:
        mcp_server._current_user.reset(u_tok)
        mcp_server._current_token_fp.reset(f_tok)

    records = _audit_records(caplog)
    assert len(records) == 1
    entry = records[0]
    assert entry["event"] == "mcp_tool_call"
    assert entry["tool"] == "query"
    assert entry["caller"] == "Ada Lovelace"
    assert entry["token_fingerprint"] == fp
    assert entry["outcome"] == "ok"
    assert entry["sql"] == "SELECT 42 AS n"
    assert entry["row_count"] == 1
    assert isinstance(entry["duration_ms"], int)
    assert entry["timestamp"].endswith("+00:00")
    assert "secret-auth-token" not in caplog.text
    assert raw_token not in caplog.text


def test_validation_error_is_audited(monkeypatch, caplog):
    fake_db = MagicMock()
    fake_db.execute_query = AsyncMock(side_effect=SQLValidationError("forbidden DROP"))
    monkeypatch.setattr(mcp_server, "get_database", lambda: fake_db)

    u_tok = mcp_server._current_user.set(("Grace", "Hopper"))
    f_tok = mcp_server._current_token_fp.set("abcdef123456")
    try:
        with caplog.at_level(logging.INFO, logger=mcp_server.logger.name):
            out = asyncio.run(mcp_server.call_tool("query", {"sql": "DROP TABLE t"}))
    finally:
        mcp_server._current_user.reset(u_tok)
        mcp_server._current_token_fp.reset(f_tok)

    assert "SQL validation error" in out[0].text
    records = _audit_records(caplog)
    assert len(records) == 1
    entry = records[0]
    assert entry["caller"] == "Grace Hopper"
    assert entry["outcome"] == "validation_error"
    assert "forbidden DROP" in entry["error"]
    assert entry["sql"] == "DROP TABLE t"


def test_schema_and_column_tools_are_audited(monkeypatch, caplog):
    fake_db = MagicMock()
    fake_db.describe_schema = AsyncMock(return_value="schema docs")
    fake_db.list_columns = AsyncMock(
        return_value=([{"column_name": "id", "data_type": "int", "is_nullable": "NO"}], 1)
    )
    monkeypatch.setattr(mcp_server, "get_database", lambda: fake_db)

    u_tok = mcp_server._current_user.set(("Alan", "Turing"))
    try:
        with caplog.at_level(logging.INFO, logger=mcp_server.logger.name):
            asyncio.run(mcp_server.call_tool("describe_schema", {"schema_name": "sample"}))
            asyncio.run(
                mcp_server.call_tool(
                    "list_columns", {"schema_name": "sample", "table_name": "items"}
                )
            )
    finally:
        mcp_server._current_user.reset(u_tok)

    records = _audit_records(caplog)
    assert len(records) == 2
    assert records[0]["tool"] == "describe_schema"
    assert records[0]["schema"] == "sample"
    assert records[0]["caller"] == "Alan Turing"
    assert records[1]["tool"] == "list_columns"
    assert records[1]["schema"] == "sample"
    assert records[1]["table"] == "items"
    assert records[1]["row_count"] == 1
