"""
Tests for the gh-proxy rate-limit / retry / data-correctness fixes:

- shared GitHub URL parser (.git, http, www., deep paths)
- gh-proxy pacing, retry classification and circuit breaker
- transient failures don't overwrite Airtable or stamp the timestamp
- "Repo 404s" rechecked weekly instead of daily
- geocoder retries gateway errors; archive keeps partial successes

No network access; HTTP is faked.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import polars as pl
import pytest
from dagster import build_asset_context

import orpheus_engine.defs.geocoder.resources as geocoder_mod
import orpheus_engine.defs.unified_ysws_db.definitions as ysws
from orpheus_engine.defs.highway_github.definitions import parse_repo_key
from orpheus_engine.defs.shared.github_utils import parse_github_repo

P = ysws.UnifiedYSWS.approved_projects


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url,expected", [
    ("https://github.com/owner/tank.git", ("owner", "tank")),
    ("http://github.com/owner/tank", ("owner", "tank")),
    ("https://www.github.com/owner/tank/", ("owner", "tank")),
    ("  https://github.com/Owner/Tank?tab=readme#top ", ("Owner", "Tank")),
    ("https://github.com/owner/tank/tree/main/sub", ("owner", "tank")),
    ("https://github.com/owner/tank/blob/main/a.py", ("owner", "tank")),
    ("github.com/owner/tank", ("owner", "tank")),
    ("https://github.com/owner", None),
    ("https://github.com/orgs/hackclub/repositories", None),
    ("https://gitlab.com/owner/tank", None),
    ("https://gist.github.com/owner/abc123", None),
    ("https://github.com/owner/.git", None),
    ("", None),
    (None, None),
])
def test_parse_github_repo(url, expected):
    assert parse_github_repo(url) == expected


def test_highway_parse_repo_key_lowercases():
    assert parse_repo_key("https://github.com/Owner/Tank.git") == ("owner", "tank")


@pytest.mark.parametrize("url,expected", [
    ("https://github.com/owner/tank.git", "https://github.com/owner/tank"),
    ("http://www.github.com/owner/tank/", "https://github.com/owner/tank"),
    # Monorepo/file links still aren't the project's own repo, so no stars
    ("https://github.com/hackclub/browserbuddy/tree/main/submissions/x", ""),
    ("https://github.com/owner/tank/blob/main/a.py", ""),
    ("https://github.com/owner/tank/pull/1", ""),
])
def test_extract_github_repo_url(url, expected):
    assert ysws._extract_github_repo_url(url) == expected


# ---------------------------------------------------------------------------
# Fake aiohttp session
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, status, json=None, headers=None, text=""):
        self.status = status
        self._json = json or {}
        self.headers = headers or {}
        self._text = text

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Returns queued responses in order; the last one repeats."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, **kwargs):
        self.calls += 1
        if len(self.responses) > 1:
            return self.responses.pop(0)
        return self.responses[0]


@pytest.fixture
def no_wait(monkeypatch):
    monkeypatch.setattr(ysws, "_gh_proxy_retry_wait", lambda headers, attempt: 0)


NOW = datetime(2026, 9, 28, tzinfo=timezone.utc)


def _fetch_one(session):
    async def run():
        throttle = ysws._GhProxyThrottle(max_rps=1000)
        return await ysws._fetch_github_stars_single_async(
            session, "rec1", "owner", "tank", "key", NOW, throttle, asyncio.Semaphore(4)
        )
    return asyncio.run(run())


# ---------------------------------------------------------------------------
# gh-proxy fetch classification
# ---------------------------------------------------------------------------

def test_200_is_authoritative():
    result = _fetch_one(_FakeSession(_FakeResponse(200, {"stargazers_count": 7, "language": "Go"})))
    assert result["repo_exists"] == "Repo Exists"
    assert result["stars"] == 7
    assert result["updated_at"] == NOW


def test_404_is_authoritative():
    result = _fetch_one(_FakeSession(_FakeResponse(404)))
    assert result["repo_exists"] == "Repo 404s"
    assert result["updated_at"] == NOW
    assert not result.get("transient")


def test_429_is_retried_then_succeeds(no_wait):
    session = _FakeSession(_FakeResponse(429), _FakeResponse(503), _FakeResponse(200, {"stargazers_count": 1}))
    result = _fetch_one(session)
    assert session.calls == 3
    assert result["repo_exists"] == "Repo Exists"


def test_persistent_5xx_is_transient_not_dead(no_wait):
    session = _FakeSession(_FakeResponse(503))
    result = _fetch_one(session)
    assert session.calls == ysws.GH_PROXY_MAX_ATTEMPTS
    assert result["transient"] is True
    assert "repo_exists" not in result
    assert "updated_at" not in result


def test_plain_403_is_transient_without_retry(no_wait):
    session = _FakeSession(_FakeResponse(403, text="Repository access blocked"))
    result = _fetch_one(session)
    assert session.calls == 1
    assert result["transient"] is True


def test_rate_limit_403_is_retried(no_wait):
    session = _FakeSession(
        _FakeResponse(403, headers={"x-ratelimit-remaining": "0"}),
        _FakeResponse(403, text="You have exceeded a secondary rate limit"),
        _FakeResponse(200, {"stargazers_count": 2}),
    )
    result = _fetch_one(session)
    assert session.calls == 3
    assert result["stars"] == 2


def test_retry_wait_honours_retry_after():
    assert 30 <= ysws._gh_proxy_retry_wait({"Retry-After": "30"}, attempt=0) < 31
    assert ysws._gh_proxy_retry_wait({"Retry-After": "9999"}, attempt=0) < ysws.GH_PROXY_MAX_RETRY_WAIT + 1
    assert 2 <= ysws._gh_proxy_retry_wait({}, attempt=0) < 3
    assert 8 <= ysws._gh_proxy_retry_wait({}, attempt=2) < 9


# ---------------------------------------------------------------------------
# Throttle + circuit breaker
# ---------------------------------------------------------------------------

def test_throttle_paces_requests():
    async def run():
        throttle = ysws._GhProxyThrottle(max_rps=20)
        start = time.monotonic()
        for _ in range(5):
            await throttle.acquire()
        return time.monotonic() - start
    # Five acquires at 50ms spacing take at least four intervals
    assert asyncio.run(run()) >= 0.2 - 0.02


def test_circuit_breaker_stops_new_requests(no_wait):
    session = _FakeSession(_FakeResponse(429))

    async def run():
        throttle = ysws._GhProxyThrottle(max_rps=1000)
        sem = asyncio.Semaphore(ysws.GH_PROXY_MAX_CONCURRENCY)
        results = await asyncio.gather(*[
            ysws._fetch_github_stars_single_async(session, f"rec{i}", "o", f"r{i}", "key", NOW, throttle, sem)
            for i in range(200)
        ])
        return results, throttle.tripped

    results, tripped = asyncio.run(run())
    assert tripped
    # Without the breaker this would be 200 * 4 = 800 requests
    assert session.calls <= ysws.GH_PROXY_CIRCUIT_BREAKER_THRESHOLD + ysws.GH_PROXY_MAX_CONCURRENCY
    assert all(r["transient"] for r in results)


def test_success_resets_consecutive_count():
    throttle = ysws._GhProxyThrottle(max_rps=1000)
    for _ in range(ysws.GH_PROXY_CIRCUIT_BREAKER_THRESHOLD - 1):
        throttle.record(rate_limited=True)
    throttle.record(rate_limited=False)
    throttle.record(rate_limited=True)
    assert not throttle.tripped


# ---------------------------------------------------------------------------
# Asset-level behaviour
# ---------------------------------------------------------------------------

def test_repo_stats_asset_drops_transient_results(monkeypatch):
    monkeypatch.setenv("GH_PROXY_API_KEY", "test")
    seen = {}

    def fake_wrapper(projects, key, now, log, max_rps):
        seen["projects"] = projects
        return [
            {"project_id": "recLive", "success": True, "repo_exists": "Repo Exists", "stars": 3, "language": "Go", "updated_at": now},
            {"project_id": "recDead", "success": False, "repo_exists": "Repo 404s", "updated_at": now},
            {"project_id": "recLimited", "success": False, "transient": True},
        ], True

    monkeypatch.setattr(ysws, "_fetch_github_stars_parallel_wrapper", fake_wrapper)
    candidates = pl.DataFrame({
        "id": ["recLive", "recDead", "recLimited", "recOther"],
        "code_url": ["https://github.com/a/live.git", "https://github.com/a/dead", "https://github.com/a/lim", "https://gitlab.com/a/b"],
    })
    out = ysws.approved_projects_repo_stats(build_asset_context(), candidates)
    df = out.value

    assert seen["projects"][0] == {"project_id": "recLive", "owner": "a", "repo": "live"}
    assert set(df["id"]) == {"recLive", "recDead", "recOther"}
    assert out.metadata["num_transient_failures"].value == 1
    assert out.metadata["circuit_breaker_tripped"].value is True


class _FakeAirtable:
    def __init__(self, df):
        self.df = df

    def get_all_records_as_polars(self, **kwargs):
        return self.df


def test_repo_404s_are_rechecked_weekly():
    now = datetime.now(timezone.utc)
    two_days = (now - timedelta(days=2)).isoformat()
    eight_days = (now - timedelta(days=8)).isoformat()
    df = pl.DataFrame({
        "id": ["live2d", "dead2d", "dead8d", "new"],
        P.code_url: ["https://github.com/a/b"] * 4,
        P.repo_star_count: [1, None, None, None],
        P.repo_stats_last_updated_at: [two_days, two_days, eight_days, None],
        P.repo_language: [None] * 4,
        P.repo_exists: ["Repo Exists", "Repo 404s", "Repo 404s", None],
    })
    context = build_asset_context(resources={"airtable": _FakeAirtable(df)})
    out = ysws.approved_projects_repo_stats_candidates(context)
    assert set(out.value["id"]) == {"live2d", "dead8d", "new"}


# ---------------------------------------------------------------------------
# Geocoder
# ---------------------------------------------------------------------------

def _resp(status, payload=None):
    import requests
    r = requests.Response()
    r.status_code = status
    r._content = b"{}" if payload is None else payload
    return r


def test_geocoder_retries_gateway_errors(monkeypatch):
    responses = [_resp(503), _resp(502), _resp(200, b'{"lat": 1, "lng": 2}')]
    monkeypatch.setattr(geocoder_mod.requests, "get", lambda *a, **k: responses.pop(0))
    monkeypatch.setattr(geocoder_mod.time, "sleep", lambda s: None)
    geocoder = geocoder_mod.GeocoderResource(api_key="test")
    assert geocoder.geocode("1 Example St") == {"lat": 1, "lng": 2}


def test_geocoder_gives_up_after_two_retries(monkeypatch):
    calls = []
    monkeypatch.setattr(geocoder_mod.requests, "get", lambda *a, **k: calls.append(1) or _resp(504))
    monkeypatch.setattr(geocoder_mod.time, "sleep", lambda s: None)
    geocoder = geocoder_mod.GeocoderResource(api_key="test")
    with pytest.raises(geocoder_mod.GeocodingError):
        geocoder.geocode("1 Example St")
    assert len(calls) == 3


def test_geocoder_does_not_retry_client_errors(monkeypatch):
    calls = []
    monkeypatch.setattr(geocoder_mod.requests, "get", lambda *a, **k: calls.append(1) or _resp(400))
    geocoder = geocoder_mod.GeocoderResource(api_key="test")
    with pytest.raises(geocoder_mod.GeocodingError):
        geocoder.geocode("nowhere")
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

def _archive_candidates():
    return pl.DataFrame({
        "id": ["rec1"],
        P.code_url: ["https://github.com/a/b"],
        P.playable_url: ["https://a.example"],
        "calculated_archive_hash": ["x"],
    })


def test_archive_keeps_partial_success_without_hash(monkeypatch):
    monkeypatch.setenv("ARCHIVE_HACKCLUB_COM_API_KEY", "test")
    monkeypatch.setattr(ysws.time, "sleep", lambda s: None)

    def fake_post(url, json, **kwargs):
        if json["url"].startswith("https://github.com"):
            return _resp(200, b'{"url": "https://archive.hackclub.com/code"}')
        return _resp(500)

    monkeypatch.setattr(ysws.requests, "post", fake_post)
    out = ysws.approved_projects_archived(build_asset_context(), _archive_candidates())
    row = out.value.row(0, named=True)
    assert row[P.archive_code_url] == "https://archive.hackclub.com/code"
    assert row[P.archive_live_url] is None
    # Hash left unset so the failed live URL is retried next run
    assert row[P.archive_hash] is None
    assert out.metadata["num_partial"].value == 1


def test_archive_retries_5xx_once(monkeypatch):
    monkeypatch.setenv("ARCHIVE_HACKCLUB_COM_API_KEY", "test")
    monkeypatch.setattr(ysws.time, "sleep", lambda s: None)
    calls = []

    def fake_post(url, json, **kwargs):
        calls.append(json["url"])
        if calls.count(json["url"]) == 1:
            return _resp(502)
        return _resp(200, b'{"url": "https://archive.hackclub.com/' + json["url"][-1].encode() + b'"}')

    monkeypatch.setattr(ysws.requests, "post", fake_post)
    out = ysws.approved_projects_archived(build_asset_context(), _archive_candidates())
    row = out.value.row(0, named=True)
    assert len(calls) == 4
    assert row[P.archive_hash] is not None
    assert out.metadata["num_partial"].value == 0
