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
from orpheus_engine.defs.zoom import daily_usage


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
