from datetime import datetime, timezone
from unittest.mock import MagicMock

import dagster as dg
import pytest
import requests

from orpheus_engine.defs.shared import zoom as zoom_mod
from orpheus_engine.defs.shared.zoom import (
    ZoomDailyQuotaError,
    ZoomResource,
    _parse_retry_after_seconds,
)
from orpheus_engine.defs.zoom import daily_usage, meetings, recordings


def test_parse_retry_after_seconds_integer_and_iso():
    now = datetime(2026, 11, 24, 18, 0, 0, tzinfo=timezone.utc)
    assert _parse_retry_after_seconds("45", now=now) == 45.0
    assert _parse_retry_after_seconds("2026-11-25T00:00:00Z", now=now) == 6 * 3600.0
    assert _parse_retry_after_seconds("2026-11-24T17:00:00Z", now=now) == 0.0
    assert _parse_retry_after_seconds("", now=now) is None
    assert _parse_retry_after_seconds("not-a-date", now=now) is None


def test_zoom_request_fails_fast_on_daily_quota_iso_retry_after(monkeypatch):
    resp = MagicMock()
    resp.status_code = 429
    resp.headers = {"Retry-After": "2099-01-01T00:00:00Z"}
    resp.ok = False

    session = MagicMock()
    session.request.return_value = resp

    res = ZoomResource(account_id="a", client_id="c", client_secret="s")
    res._session = session
    res._token = "tok"
    res._token_expires_at = datetime(2099, 1, 1, tzinfo=timezone.utc)

    slept = []
    monkeypatch.setattr(zoom_mod.time, "sleep", lambda sec: slept.append(sec))

    with pytest.raises(ZoomDailyQuotaError):
        res._request("GET", "/report/daily", params={"year": 2026, "month": 11})

    assert session.request.call_count == 1
    assert slept == []


def test_zoom_request_retries_short_integer_retry_after(monkeypatch):
    r429 = MagicMock(status_code=429, headers={"Retry-After": "10"}, ok=False)
    r200 = MagicMock(status_code=200, headers={}, ok=True)
    r200.json.return_value = {"dates": [{"date": "2026-11-24"}]}

    session = MagicMock()
    session.request.side_effect = [r429, r200]

    res = ZoomResource(account_id="a", client_id="c", client_secret="s")
    res._session = session
    res._token = "tok"
    res._token_expires_at = datetime(2099, 1, 1, tzinfo=timezone.utc)

    slept = []
    monkeypatch.setattr(zoom_mod.time, "sleep", lambda sec: slept.append(sec))
    monkeypatch.setattr(zoom_mod.random, "uniform", lambda a, b: 0.0)

    data = res._request("GET", "/report/daily")
    assert data == {"dates": [{"date": "2026-11-24"}]}
    assert session.request.call_count == 2
    assert slept == [10.0]


def test_zoom_daily_usage_aborts_immediately_on_daily_quota_error(monkeypatch):
    calls = []

    class FakeZoom:
        def fetch_daily_usage(self, year, month, log=None):
            calls.append((year, month))
            raise ZoomDailyQuotaError("daily quota exhausted")

    monkeypatch.setattr(daily_usage, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(daily_usage, "ensure_table", lambda *a, **kw: None)

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        daily_usage.zoom_daily_usage(context=ctx, zoom=FakeZoom())

    assert len(calls) == 1, "must abort on first month instead of burning 6 months of retries"


def test_zoom_daily_usage_aborts_on_http_error(monkeypatch):
    calls = []

    class FakeZoom:
        def fetch_daily_usage(self, year, month, log=None):
            calls.append((year, month))
            raise requests.exceptions.HTTPError("Exhausted 15 retries")

    monkeypatch.setattr(daily_usage, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(daily_usage, "ensure_table", lambda *a, **kw: None)

    ctx = dg.build_asset_context()
    with pytest.raises(requests.exceptions.HTTPError):
        daily_usage.zoom_daily_usage(context=ctx, zoom=FakeZoom())

    assert len(calls) == 1


def test_zoom_daily_usage_persists_already_fetched_months_before_raising(monkeypatch):
    calls = []
    upserted = []

    class FakeZoom:
        def fetch_daily_usage(self, year, month, log=None):
            calls.append((year, month))
            if len(calls) == 1:
                return [{"date": "2026-11-24", "new_users": 1, "meetings": 2, "participants": 3, "meeting_minutes": 40}]
            raise ZoomDailyQuotaError("daily quota exhausted on month 2")

    monkeypatch.setattr(daily_usage, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(daily_usage, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(daily_usage, "upsert_rows", lambda conn, tbl, cols, rows, *a: upserted.extend(rows))

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        daily_usage.zoom_daily_usage(context=ctx, zoom=FakeZoom())

    assert len(calls) == 2
    assert len(upserted) == 1
    assert upserted[0][0] == "2026-11-24"


def test_zoom_meetings_persists_partial_batch_before_raising_quota_error(monkeypatch):
    upserted = []

    class FakeZoom:
        def fetch_users(self, log=None):
            return [{"id": "u1"}, {"id": "u2"}]

        def fetch_user_meetings(self, user_id, start, end, log=None):
            if user_id == "u1":
                yield {"uuid": "m1", "id": 1, "host_id": "u1", "end_time": "2026-11-24T05:00:00Z"}
            else:
                raise ZoomDailyQuotaError("daily quota exhausted on u2")

    monkeypatch.setattr(meetings, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(meetings, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(meetings, "upsert_rows", lambda conn, tbl, cols, rows, *a: upserted.extend(rows))

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        meetings.zoom_meetings(context=ctx, config=meetings.ZoomMeetingSyncConfig(), zoom=FakeZoom())

    assert len(upserted) == 1
    assert upserted[0][0] == "m1"


def test_zoom_cloud_recording_usage_persists_partial_entries_before_raising(monkeypatch):
    upserted = []

    class FakeZoom:
        def fetch_cloud_recording_usage(self, start, end, log=None):
            yield {"user_id": "u1", "email": "a@example.com", "usage": "10 MB"}
            raise ZoomDailyQuotaError("daily quota exhausted on chunk 2")

    monkeypatch.setattr(recordings, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(recordings, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(recordings, "upsert_rows", lambda conn, tbl, cols, rows, *a: upserted.extend(rows))

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        recordings.zoom_cloud_recording_usage(
            context=ctx, config=recordings.ZoomRecordingSyncConfig(), zoom=FakeZoom()
        )

    assert len(upserted) == 1
    assert upserted[0][0] == "u1"
