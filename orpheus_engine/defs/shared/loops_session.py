"""Helpers for authenticated requests to app.loops.so using LOOPS_SESSION_TOKEN."""

from typing import Any

import requests
from requests.cookies import RequestsCookieJar

LOOPS_SESSION_COOKIE_NAME = "__Secure-next-auth.session-token"
LOOPS_COOKIE_DOMAIN = "app.loops.so"


class LoopsAuthError(RuntimeError):
    """LOOPS_SESSION_TOKEN was rejected or redirected. Every later request will fail too."""


def build_loops_cookie_jar(session_token: str) -> RequestsCookieJar:
    """Build a cookie jar scoping the Loops session cookie strictly to https://app.loops.so/."""
    jar = RequestsCookieJar()
    jar.set(
        LOOPS_SESSION_COOKIE_NAME,
        session_token,
        domain=LOOPS_COOKIE_DOMAIN,
        secure=True,
        path="/",
    )
    return jar


def raise_for_loops_redirect(response: requests.Response, context_label: str) -> None:
    """Raise LoopsAuthError on 3xx responses without following or logging Location/cookie values."""
    if 300 <= response.status_code < 400:
        raise LoopsAuthError(
            f"HTTP {response.status_code} redirect for {context_label}; "
            "LOOPS_SESSION_TOKEN has likely expired (refusing to follow redirect)"
        )


def loops_session_request(
    method: str,
    url: str,
    session_token: str,
    *,
    context_label: str,
    timeout: float = 30,
    **kwargs: Any,
) -> requests.Response:
    """Send an authenticated request to app.loops.so with domain-scoped cookies and no redirects."""
    method_lower = method.lower()
    if method_lower not in ("get", "post"):
        raise ValueError(f"Unsupported HTTP method for Loops session request: {method}")
    if "cookies" in kwargs:
        raise ValueError("Do not pass cookies directly; loops_session_request builds a scoped jar.")
    if "allow_redirects" in kwargs:
        raise ValueError("Do not override allow_redirects on Loops session requests.")

    request_fn = getattr(requests, method_lower)
    response = request_fn(
        url,
        cookies=build_loops_cookie_jar(session_token),
        allow_redirects=False,
        timeout=timeout,
        **kwargs,
    )
    raise_for_loops_redirect(response, context_label)
    return response
