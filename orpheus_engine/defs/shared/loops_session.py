"""Helpers for authenticated requests to app.loops.so using LOOPS_SESSION_TOKEN."""

from typing import Any, Mapping
from urllib.parse import urlsplit

import requests
from requests.cookies import RequestsCookieJar

LOOPS_SESSION_COOKIE_NAME = "__Secure-next-auth.session-token"
LOOPS_COOKIE_DOMAIN = "app.loops.so"


class LoopsAuthError(RuntimeError):
    """LOOPS_SESSION_TOKEN was rejected or redirected. Every later request will fail too."""


class LoopsRedirectError(RuntimeError):
    """Allow-listed same-origin redirect on a campaign /compose page."""

    def __init__(self, message: str, *, path_class: str):
        super().__init__(message)
        self.path_class = path_class


def _match_allowed_loops_redirect(
    location: str | None,
    allowed_redirect_paths: Mapping[str, str] | None,
) -> str | None:
    """Return the redacted path class iff Location strictly matches an allow-listed
    normalised path on https://app.loops.so (or relative /path) with no query or fragment.
    Rejects traversal, percent-encoding, backslash, userinfo, non-HTTPS, or off-origin host."""
    if not location or not allowed_redirect_paths:
        return None
    raw = location.strip()
    if not raw or raw.startswith("//"):
        return None
    if any(ch in raw for ch in ("\\", "%", ";", "@", "\r", "\n")):
        return None
    parsed = urlsplit(raw)
    if parsed.query or parsed.fragment:
        return None
    if parsed.scheme or parsed.netloc:
        if parsed.scheme.lower() != "https" or parsed.netloc.lower() != LOOPS_COOKIE_DOMAIN:
            return None
    elif not parsed.path.startswith("/"):
        return None
    path = parsed.path or "/"
    if not path.startswith("/"):
        return None
    segments = [seg for seg in path.split("/") if seg != ""]
    if any(seg in (".", "..") for seg in segments):
        return None
    normalised = "/" + "/".join(segments) if segments else "/"
    return allowed_redirect_paths.get(normalised)


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


def raise_for_loops_redirect(
    response: requests.Response,
    context_label: str,
    *,
    allowed_redirect_paths: Mapping[str, str] | None = None,
) -> None:
    """Raise LoopsRedirectError (carrying only the redacted path class) for explicitly
    allow-listed same-origin paths; raise LoopsAuthError for every other 3xx. Never
    follows or logs Location/cookie values; the caller logs the path class."""
    if 300 <= response.status_code < 400:
        path_class = _match_allowed_loops_redirect(
            response.headers.get("Location"),
            allowed_redirect_paths,
        )
        if path_class is not None:
            raise LoopsRedirectError(
                f"HTTP {response.status_code} allow-listed redirect (class={path_class}) "
                f"for {context_label} (refusing to follow redirect)",
                path_class=path_class,
            )
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
    allowed_redirect_paths: Mapping[str, str] | None = None,
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
    raise_for_loops_redirect(
        response,
        context_label,
        allowed_redirect_paths=allowed_redirect_paths,
    )
    return response
