from datetime import datetime, timezone
from unittest.mock import MagicMock

import dagster as dg
import pytest
import requests

from orpheus_engine.defs.shared import zoom as zoom_mod
from orpheus_engine.defs.shared.zoom import (
    ZoomAuthError,
    ZoomDailyQuotaError,
    ZoomResource,
    _parse_retry_after_seconds,
)
from orpheus_engine.defs.sling.programs.thirdspace import thirdspace_replication_config
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


def test_zoom_request_raises_auth_error_on_repeated_401(monkeypatch):
    resp = MagicMock(status_code=401, headers={}, ok=False)
    session = MagicMock()
    session.request.return_value = resp

    res = ZoomResource(account_id="a", client_id="c", client_secret="s")
    res._session = session
    res._token = "tok"
    res._token_expires_at = datetime(2099, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(res, "_ensure_token", lambda: None)

    with pytest.raises(ZoomAuthError):
        res._request("GET", "/report/daily")

    # 1 initial attempt + 1 retry after clearing token, then fail with ZoomAuthError
    assert session.request.call_count == 2


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

    assert len(calls) == 1


def test_zoom_daily_usage_persists_already_fetched_months_and_preserves_error_if_flush_fails(monkeypatch):
    calls = []

    class FakeZoom:
        def fetch_daily_usage(self, year, month, log=None):
            calls.append((year, month))
            if len(calls) == 1:
                return [{"date": "2026-11-24", "new_users": 1, "meetings": 2, "participants": 3, "meeting_minutes": 40}]
            raise ZoomDailyQuotaError("daily quota exhausted on month 2")

    monkeypatch.setattr(daily_usage, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(daily_usage, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(daily_usage, "upsert_rows", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db down")))

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        daily_usage.zoom_daily_usage(context=ctx, zoom=FakeZoom())

    assert len(calls) == 2


def test_zoom_meetings_preserves_original_quota_error_when_finally_flush_fails(monkeypatch):
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
    monkeypatch.setattr(meetings, "upsert_rows", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db down")))

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        meetings.zoom_meetings(context=ctx, config=meetings.ZoomMeetingSyncConfig(), zoom=FakeZoom())


def test_zoom_cloud_recording_usage_preserves_original_quota_error_when_finally_flush_fails(monkeypatch):
    class FakeZoom:
        def fetch_cloud_recording_usage(self, start, end, log=None):
            yield {"user_id": "u1", "email": "a@example.com", "usage": "10 MB"}
            raise ZoomDailyQuotaError("daily quota exhausted on chunk 2")

    monkeypatch.setattr(recordings, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(recordings, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(recordings, "upsert_rows", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db down")))

    ctx = dg.build_asset_context()
    with pytest.raises(ZoomDailyQuotaError):
        recordings.zoom_cloud_recording_usage(
            context=ctx, config=recordings.ZoomRecordingSyncConfig(), zoom=FakeZoom()
        )


@pytest.mark.parametrize("exc_cls", [ZoomDailyQuotaError, ZoomAuthError])
@pytest.mark.parametrize(
    "asset_fn,zoom_method",
    [
        (meetings.zoom_meeting_participants, "fetch_meeting_participants"),
        (meetings.zoom_meeting_participant_details, "fetch_meeting_participant_details"),
        (meetings.zoom_meeting_participant_qos, "fetch_meeting_participant_qos"),
        (meetings.zoom_meeting_sharing, "fetch_meeting_sharing"),
        (meetings.zoom_meeting_polls, "fetch_meeting_polls"),
        (meetings.zoom_meeting_qa, "fetch_meeting_qa"),
    ],
)
def test_per_meeting_zoom_assets_never_swallow_quota_or_auth_errors(monkeypatch, exc_cls, asset_fn, zoom_method):
    class FakeZoom:
        pass

    setattr(FakeZoom, zoom_method, lambda self, **kw: (_ for _ in ()).throw(exc_cls("fatal")))

    monkeypatch.setattr(meetings, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(meetings, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(meetings, "_get_meeting_uuids_since", lambda start: ["mtg-1"])

    ctx = dg.build_asset_context()
    with pytest.raises(exc_cls):
        asset_fn(context=ctx, config=meetings.ZoomMeetingSyncConfig(), zoom=FakeZoom())


@pytest.mark.parametrize("exc_cls", [ZoomDailyQuotaError, ZoomAuthError])
def test_zoom_recordings_and_analytics_never_swallow_quota_or_auth_errors(monkeypatch, exc_cls):
    class FakeZoom:
        def fetch_users(self, log=None):
            return [{"id": "u1"}]

        def fetch_user_recordings(self, **kw):
            raise exc_cls("fatal")

        def fetch_recording_analytics(self, **kw):
            raise exc_cls("fatal")

    monkeypatch.setattr(recordings, "get_db_connection", lambda: MagicMock())
    monkeypatch.setattr(recordings, "ensure_table", lambda *a, **kw: None)
    monkeypatch.setattr(recordings, "_get_recording_uuids_since", lambda start: ["rec-1"])

    ctx = dg.build_asset_context()
    with pytest.raises(exc_cls):
        recordings.zoom_recordings(context=ctx, config=recordings.ZoomRecordingSyncConfig(), zoom=FakeZoom())
    with pytest.raises(exc_cls):
        recordings.zoom_recording_analytics(context=ctx, config=recordings.ZoomRecordingSyncConfig(), zoom=FakeZoom())


def test_thirdspace_uses_full_refresh_to_propagate_deletes():
    assert thirdspace_replication_config["defaults"]["mode"] == "full-refresh"
    for stream_name, cfg in thirdspace_replication_config["streams"].items():
        if isinstance(cfg, dict):
            assert cfg.get("mode", "full-refresh") == "full-refresh", f"{stream_name} must use full-refresh"
