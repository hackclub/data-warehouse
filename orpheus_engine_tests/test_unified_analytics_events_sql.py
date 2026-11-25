"""Real-Postgres regression test for loops_unpivoted timestamp casts in unified_analytics/events.sql."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pytest

from orpheus_engine_tests.test_warehouse_agent_login import server  # noqa: F401

EVENTS_SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "orpheus_engine_dbt"
    / "models"
    / "unified_analytics"
    / "events.sql"
)


def _extract_loops_unpivoted_sql() -> str:
    """Extract the real loops_events + loops_unpivoted CTEs from events.sql."""
    text = EVENTS_SQL_PATH.read_text()
    match = re.search(
        r"(WITH\s+loops_events\s+AS\s*\(.*?loops_unpivoted\s+AS\s*\(.*?\n\)),",
        text,
        flags=re.DOTALL,
    )
    assert match is not None, "Could not locate loops_events/loops_unpivoted CTEs in events.sql"
    cte_sql = match.group(1)
    cte_sql = cte_sql.replace("{{ source('loops', 'audience') }}", "test_loops.audience")
    return cte_sql + "\nSELECT column_name, event_date FROM loops_unpivoted ORDER BY column_name"


def test_events_sql_skips_year_zero_timestamps_and_keeps_valid_values(server):
    """Guarded CASE in events.sql must not crash on year-0000 Loops timestamps."""
    query = _extract_loops_unpivoted_sql()

    with server.admin() as conn, conn.cursor() as cur:
        # Confirm Postgres itself rejects unguarded year-0000 timestamptz cast
        # (proves this test would fail without the year-range guard in events.sql).
        with pytest.raises(psycopg2.errors.DatetimeFieldOverflow):
            cur.execute("SELECT '0000-01-01T00:00:00.000Z'::timestamptz")
        conn.rollback()

        cur.execute("CREATE SCHEMA IF NOT EXISTS test_loops")
        cur.execute("DROP TABLE IF EXISTS test_loops.audience")
        cur.execute(
            """
            CREATE TABLE test_loops.audience (
                email TEXT,
                subscribed BOOLEAN,
                user_group TEXT,
                birthday TEXT,
                birthday__v_text TEXT,
                bad_date_only TEXT,
                valid_iso_ts TEXT,
                valid_date_only TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO test_loops.audience VALUES (
                'test@example.invalid',
                TRUE,
                'Hack Clubber',
                '0000-01-01',
                '0000-01-01T00:00:00.000Z',
                '0000-01-01',
                '2026-11-20T12:34:56.789Z',
                '2026-11-20'
            )
            """
        )
        conn.commit()

        cur.execute(query)
        results = dict(cur.fetchall())

    assert results["birthday__v_text"] is None
    assert results["bad_date_only"] is None
    assert results["valid_iso_ts"] == datetime(2026, 11, 20, 12, 34, 56, 789000, tzinfo=timezone.utc)
    assert results["valid_date_only"] == datetime(2026, 11, 20, 0, 0, 0, tzinfo=timezone.utc)
