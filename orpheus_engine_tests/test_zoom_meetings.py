from datetime import datetime, timezone
from unittest.mock import MagicMock

import dagster as dg

from orpheus_engine.defs.zoom import meetings
from orpheus_engine.defs.zoom.meetings import (
    DETAIL_COLUMNS,
    DETAIL_VALUE_TEMPLATE,
    ZoomMeetingSyncConfig,
    _detail_row,
    zoom_meeting_participant_details,
)


def test_zoom_detail_value_template_matches_columns_and_row():
    row = _detail_row(
        {"participant_uuid": "p1", "join_time": "2026-11-22T00:00:00Z"},
        "mtg-1",
        datetime(2026, 11, 22, tzinfo=timezone.utc),
    )
    assert len(row) == len(DETAIL_COLUMNS)
    assert DETAIL_VALUE_TEMPLATE.count("%s") == len(DETAIL_COLUMNS)


def test_zoom_participant_details_tail_flush_template_matches_row_arity(monkeypatch):
    """Drive the tail-flush path (len(batch) < BATCH_SIZE) to catch placeholder-count drift."""
    captured = []

    class FakeZoom:
        def fetch_meeting_participant_details(self, meeting_id, log):
            return [{"participant_uuid": "p1", "join_time": "2026-11-22T00:00:00Z"}]

    def fake_upsert(conn, table, columns, rows, conflict_col, update_cols, template):
        captured.append((table, columns, rows, template))
        # Verify %s count matches every row's tuple length (reproducing psycopg2's check).
        for row in rows:
            assert template.count("%s") == len(row) == len(columns)

    monkeypatch.setattr(meetings, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(meetings, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(meetings, "_get_meeting_uuids_since", lambda start: ["mtg-1"])
    monkeypatch.setattr(meetings, "upsert_rows", fake_upsert)

    ctx = dg.build_asset_context()
    res = zoom_meeting_participant_details(
        context=ctx,
        config=ZoomMeetingSyncConfig(full_refresh=False, lookback_days=1),
        zoom=FakeZoom(),
    )
    assert len(captured) == 1
    assert captured[0][0] == "meeting_participant_details"
    assert res.metadata["details_synced"].value == 1
