"""
Unit tests verifying Loops session cookie scoping, redirect blocking, and
expired-session handling across all Loops HTTP call sites.

All HTTP calls are stubbed/mocked; no network access is performed.
"""

from types import SimpleNamespace

import dagster as dg
import polars as pl
import pytest
import requests
from requests.adapters import BaseAdapter
from requests.cookies import RequestsCookieJar

import orpheus_engine.defs.loops.definitions as loops_audience_defs
import orpheus_engine.defs.loops_campaign_and_metrics_export.definitions as loops_campaign_defs
from orpheus_engine.defs.shared.loops_session import (
    LOOPS_COOKIE_DOMAIN,
    LOOPS_SESSION_COOKIE_NAME,
    LoopsAuthError,
    build_loops_cookie_jar,
    loops_session_request,
)


def _assert_scoped_loops_jar(jar, expected_token: str) -> None:
    assert isinstance(jar, RequestsCookieJar)
    cookies = list(jar)
    assert len(cookies) == 1
    cookie = cookies[0]
    assert cookie.name == LOOPS_SESSION_COOKIE_NAME
    assert cookie.value == expected_token
    assert cookie.domain == LOOPS_COOKIE_DOMAIN
    assert cookie.domain_specified is True
    assert cookie.secure is True
    assert cookie.path == "/"


def _make_response(status_code: int = 200, json_data=None, text: str = "", content: bytes = b"", headers=None):
    resp = requests.Response()
    resp.status_code = status_code
    if headers:
        resp.headers.update(headers)
    if json_data is not None:
        import json
        resp._content = json.dumps(json_data).encode("utf-8")
    elif content:
        resp._content = content
    else:
        resp._content = text.encode("utf-8")
    return resp


class _RecordingRedirectAdapter(BaseAdapter):
    """Stub HTTPAdapter that records PreparedRequests and returns a 302 to another host."""

    def __init__(self, redirect_location: str):
        super().__init__()
        self.redirect_location = redirect_location
        self.sent_requests = []

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        self.sent_requests.append(request)
        if request.url.startswith("https://app.loops.so/"):
            resp = _make_response(
                status_code=302,
                text="Found",
                headers={"Location": self.redirect_location},
            )
        else:
            resp = _make_response(status_code=200, text="redirect target")
        resp.request = request
        resp.url = request.url
        return resp

    def close(self):
        pass


def test_cookie_jar_is_scoped_to_app_loops_so_and_secure():
    token = "fake-session-token-123"
    jar = build_loops_cookie_jar(token)
    _assert_scoped_loops_jar(jar, token)

    # HTTPS app.loops.so receives the cookie
    ok_req = requests.Request("GET", "https://app.loops.so/api/trpc/test", cookies=jar).prepare()
    assert f"{LOOPS_SESSION_COOKIE_NAME}={token}" in ok_req.headers.get("Cookie", "")

    # Different host does NOT receive the cookie
    other_host_req = requests.Request("GET", "https://other.example.com/steal", cookies=jar).prepare()
    assert "Cookie" not in other_host_req.headers

    # Non-HTTPS app.loops.so does NOT receive the cookie (secure=True)
    http_req = requests.Request("GET", "http://app.loops.so/api/trpc/test", cookies=jar).prepare()
    assert "Cookie" not in http_req.headers

    # Parent domain and suffix-lookalike host do NOT receive the cookie
    parent_req = requests.Request("GET", "https://loops.so/test", cookies=jar).prepare()
    assert "Cookie" not in parent_req.headers
    lookalike_req = requests.Request("GET", "https://evil-app.loops.so/test", cookies=jar).prepare()
    assert "Cookie" not in lookalike_req.headers


def test_cross_host_redirect_never_sends_cookie_even_if_followed():
    """Defense-in-depth: even if redirects were followed, RequestsCookieJar blocks cross-host leak."""
    token = "fake-session-token-secret"
    jar = build_loops_cookie_jar(token)
    adapter = _RecordingRedirectAdapter("https://other.example.com/capture?code=secret-query-param")

    session = requests.Session()
    session.mount("https://", adapter)
    session.get("https://app.loops.so/api/trpc/test", cookies=jar, allow_redirects=True)

    assert len(adapter.sent_requests) == 2
    first_req, second_req = adapter.sent_requests
    assert first_req.url == "https://app.loops.so/api/trpc/test"
    assert token in first_req.headers.get("Cookie", "")
    assert second_req.url.startswith("https://other.example.com/capture")
    assert "Cookie" not in second_req.headers


@pytest.mark.parametrize("status_code", [301, 302, 303, 307, 308])
def test_loops_session_request_blocks_redirect_and_redacts_secrets(monkeypatch, status_code):
    token = "super-secret-session-token"
    location_secret = "super-secret-location-query-token"
    adapter = _RecordingRedirectAdapter(f"https://other.example.com/login?token={location_secret}")

    def fake_get(url, **kwargs):
        assert kwargs["allow_redirects"] is False
        _assert_scoped_loops_jar(kwargs["cookies"], token)
        session = requests.Session()
        session.mount("https://", adapter)
        resp = session.get(url, **kwargs)
        resp.status_code = status_code
        return resp

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(LoopsAuthError) as exc_info:
        loops_session_request(
            "GET",
            "https://app.loops.so/api/trpc/test",
            token,
            context_label="test endpoint",
        )

    assert len(adapter.sent_requests) == 1
    assert adapter.sent_requests[0].url == "https://app.loops.so/api/trpc/test"
    err_msg = str(exc_info.value)
    assert str(status_code) in err_msg
    assert "LOOPS_SESSION_TOKEN" in err_msg
    assert token not in err_msg
    assert location_secret not in err_msg
    assert "other.example.com" not in err_msg


def test_loops_raw_audience_scopes_cookies_disables_redirects_and_keeps_s3_clean(monkeypatch):
    token = "fake-audience-token"
    presigned_s3_url = "https://s3.example.com/bucket/export.csv?X-Amz-Signature=fakesig"
    recorded_calls = []

    monkeypatch.setenv("DAGSTER_ENV", "production")
    monkeypatch.setattr(loops_audience_defs, "write_daily_parquet_backup", lambda **kwargs: None)

    def fake_post(url, **kwargs):
        recorded_calls.append(("POST", url, kwargs))
        if url.endswith("/lists.exportContacts"):
            return _make_response(200, json_data={"result": {"data": {"json": {"id": "job-123"}}}})
        if url.endswith("/audienceDownload.signs3Url"):
            return _make_response(
                200,
                json_data={
                    "result": {
                        "data": {
                            "json": {
                                "presignedUrl": presigned_s3_url,
                                "filename": "export.csv",
                            }
                        }
                    }
                },
            )
        raise AssertionError(f"Unexpected POST URL: {url}")

    def fake_get(url, **kwargs):
        recorded_calls.append(("GET", url, kwargs))
        if url.endswith("/audienceDownload.getAudienceDownload"):
            return _make_response(
                200,
                json_data={"result": {"data": {"json": {"status": "Complete"}}}},
            )
        if url == presigned_s3_url:
            return _make_response(200, content=b"email,firstName\ntest@example.com,Test\n")
        raise AssertionError(f"Unexpected GET URL: {url}")

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    context = dg.build_asset_context(
        resources={"loops_session_token": SimpleNamespace(get_value=lambda: token)}
    )
    df = loops_audience_defs.loops_raw_audience(context)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    assert len(recorded_calls) == 4

    # First 3 calls hit app.loops.so with scoped cookie jar and allow_redirects=False
    for method, url, kwargs in recorded_calls[:3]:
        assert url.startswith("https://app.loops.so/")
        assert kwargs["allow_redirects"] is False
        _assert_scoped_loops_jar(kwargs["cookies"], token)

    # 4th call is the presigned S3 CSV download: clean, no cookies attached
    s3_method, s3_url, s3_kwargs = recorded_calls[3]
    assert s3_method == "GET"
    assert s3_url == presigned_s3_url
    assert "cookies" not in s3_kwargs


def test_loops_raw_audience_fails_immediately_on_poll_redirect(monkeypatch):
    token = "fake-audience-token"
    poll_calls = 0

    monkeypatch.setenv("DAGSTER_ENV", "production")

    def fake_post(url, **kwargs):
        return _make_response(200, json_data={"result": {"data": {"json": {"id": "job-123"}}}})

    def fake_get(url, **kwargs):
        nonlocal poll_calls
        poll_calls += 1
        return _make_response(
            302,
            headers={"Location": "https://other.example.com/login?next=secret"},
        )

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    context = dg.build_asset_context(
        resources={"loops_session_token": SimpleNamespace(get_value=lambda: token)}
    )
    with pytest.raises(LoopsAuthError) as exc_info:
        loops_audience_defs.loops_raw_audience(context)

    assert poll_calls == 1
    assert token not in str(exc_info.value)
    assert "secret" not in str(exc_info.value)


@pytest.mark.parametrize(
    "call_site",
    [
        "loops_campaign_names_and_ids",
        "loops_mailing_lists",
        "_http_get_html",
        "_fetch_email_message_by_id",
        "_fetch_email_html",
        "_fetch_campaign_metrics_page",
    ],
)
def test_campaign_and_metrics_call_sites_scope_cookie_and_block_redirects(monkeypatch, call_site):
    token = "fake-campaign-token"
    captured = []

    def fake_get(url, **kwargs):
        captured.append((url, kwargs))
        return _make_response(
            307,
            text="Redirecting",
            headers={"Location": "https://other.example.com/auth?code=leak-me-not"},
        )

    monkeypatch.setattr(requests, "get", fake_get)
    context = dg.build_asset_context(
        resources={"loops_session_token": SimpleNamespace(get_value=lambda: token)}
    )

    with pytest.raises(LoopsAuthError) as exc_info:
        if call_site == "loops_campaign_names_and_ids":
            loops_campaign_defs.loops_campaign_names_and_ids(context)
        elif call_site == "loops_mailing_lists":
            loops_campaign_defs.loops_mailing_lists(context)
        elif call_site == "_http_get_html":
            loops_campaign_defs._http_get_html("/campaigns/c1/compose", token)
        elif call_site == "_fetch_email_message_by_id":
            loops_campaign_defs._fetch_email_message_by_id(token, "msg-1")
        elif call_site == "_fetch_email_html":
            loops_campaign_defs._fetch_email_html(token, "msg-1")
        elif call_site == "_fetch_campaign_metrics_page":
            loops_campaign_defs._fetch_campaign_metrics_page(token, "msg-1")

    assert len(captured) == 1
    url, kwargs = captured[0]
    assert url.startswith("https://app.loops.so/")
    assert kwargs["allow_redirects"] is False
    _assert_scoped_loops_jar(kwargs["cookies"], token)
    err_msg = str(exc_info.value)
    assert token not in err_msg
    assert "leak-me-not" not in err_msg
    assert "other.example.com" not in err_msg


@pytest.mark.parametrize("status_code", [302, 307, 401, 403])
def test_http_get_html_raises_auth_error_on_expired_cookie(monkeypatch, status_code):
    monkeypatch.setattr(
        requests,
        "get",
        lambda *a, **k: _make_response(status_code=status_code, text="Unauthorized"),
    )

    with pytest.raises(loops_campaign_defs.LoopsAuthError, match="LOOPS_SESSION_TOKEN"):
        loops_campaign_defs._http_get_html("/campaigns/c1/compose", "fake-token")
