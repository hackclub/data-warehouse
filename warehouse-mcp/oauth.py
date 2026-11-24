"""Bridge MCP public-client OAuth to Hack Club Auth OpenID Connect."""

import base64
import hashlib
from html import escape
import json
import logging
import os
import re
import secrets
from urllib.parse import urlencode, urlsplit, parse_qsl

import httpx
import jwt
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse

from auth_store import AuthStore, digest

logger = logging.getLogger(__name__)
ISSUER = "https://auth.hackclub.com"
FLOW_COOKIE = "warehouse_oauth"
ADMIN_COOKIE = "warehouse_admin"
PUBLIC_CLIENT_ID = re.compile(r"[A-Za-z0-9_-]{32}")
VERIFIER = re.compile(r"[A-Za-z0-9._~-]{43,128}")
CHALLENGE = re.compile(r"[A-Za-z0-9_-]{43}")
NO_STORE = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}


def pkce(verifier: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")


def redirect_parts(uri):
    """Permit browser HTTPS redirects and local development loopback redirects."""
    if not isinstance(uri, str) or not uri or len(uri) > 2048:
        return None
    if not uri.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in uri) or "\\" in uri or "#" in uri:
        return None
    try:
        parts = urlsplit(uri)
        if parts.port == 0:
            return None
    except ValueError:
        return None
    if not parts.hostname or "%" in parts.netloc or parts.username is not None:
        return None
    if parts.scheme == "https" or (
        parts.scheme == "http" and parts.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        return parts
    return None


def valid_redirect(uri):
    parts = redirect_parts(uri)
    reserved = {"code", "state", "error", "error_description", "error_uri"}
    return parts is not None and reserved.isdisjoint(dict(parse_qsl(parts.query)))


def page(title, content, status=200):
    return HTMLResponse(
        f'<!doctype html><html lang="en"><meta charset="utf-8"><title>{title}</title>'
        f'<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<main><h1>{title}</h1>{content}</main></html>',
        status_code=status,
        headers={
            **NO_STORE,
            "Referrer-Policy": "same-origin",
            "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'; form-action 'self'",
        },
    )


def error(code, status=400):
    return JSONResponse({"error": code}, status, headers=NO_STORE)


async def body(request):
    data = bytearray()
    async for chunk in request.stream():
        data.extend(chunk)
        if len(data) > 16384:
            raise InvalidRequest
    return data


async def form(request):
    if request.headers.get("content-type", "").split(";")[0] != "application/x-www-form-urlencoded":
        raise InvalidRequest
    try:
        fields = parse_qsl((await body(request)).decode(), max_num_fields=32)
    except ValueError as exc:
        raise InvalidRequest from exc
    if len(dict(fields)) != len(fields):
        raise InvalidRequest
    if any("\x00" in field for pair in fields for field in pair):
        raise InvalidRequest
    return dict(fields)


class InvalidRequest(Exception):
    pass


class OAuth:
    def __init__(self):
        self.origin = os.environ.get("MCP_PUBLIC_URL", "http://localhost:3000").rstrip("/")
        parsed = redirect_parts(self.origin)
        if not parsed or self.origin != f"{parsed.scheme}://{parsed.netloc}":
            raise RuntimeError("MCP_PUBLIC_URL must be an HTTPS or loopback HTTP origin")
        self.host = parsed.netloc
        self.secure = parsed.scheme == "https"
        self.client_id = os.environ["HACKCLUB_CLIENT_ID"]
        self.client_secret = os.environ["HACKCLUB_CLIENT_SECRET"]
        self.superadmin_email = os.environ["MCP_SUPERADMIN_EMAIL"].strip().lower()
        dsn = os.environ["MCP_AUTH_DATABASE_URL"]
        if not all((self.client_id, self.client_secret, self.superadmin_email, dsn)):
            raise RuntimeError("Warehouse MCP auth configuration cannot be empty")
        self.store = AuthStore(dsn, self.superadmin_email)
        self.http = httpx.AsyncClient(timeout=10, follow_redirects=False)

    @property
    def callback_url(self):
        return f"{self.origin}/oauth/callback"

    def valid_resource(self, params):
        # Allow older clients that omit resource. All grants are still bound to this origin.
        return params.get("resource", self.origin) in {self.origin, self.origin + "/"}

    async def route(self, request: Request):
        path, method = request.scope["path"], request.method
        try:
            if path == "/.well-known/oauth-authorization-server" and method == "GET":
                return JSONResponse({
                    "issuer": self.origin,
                    "authorization_endpoint": f"{self.origin}/authorize",
                    "token_endpoint": f"{self.origin}/token",
                    "registration_endpoint": f"{self.origin}/register",
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                })
            if path == "/.well-known/oauth-protected-resource" and method == "GET":
                return JSONResponse({"resource": self.origin, "authorization_servers": [self.origin]})
            if path == "/register" and method == "POST":
                return await self.register(request)
            if path == "/authorize" and method in {"GET", "POST"}:
                return await self.authorize(request)
            if path == "/oauth/callback" and method == "GET":
                return await self.callback(request)
            if path == "/token" and method == "POST":
                return await self.token(request)
            if path == "/admin" and method in {"GET", "POST"}:
                return await self.admin(request)
        except InvalidRequest:
            return error("invalid_request")
        return None

    async def register(self, request):
        try:
            data = json.loads(await body(request))
        except ValueError as exc:
            raise InvalidRequest from exc
        if not isinstance(data, dict):
            raise InvalidRequest
        redirects = data.get("redirect_uris")
        if (not isinstance(redirects, list) or not 1 <= len(redirects) <= 10
                or not all(map(valid_redirect, redirects))):
            return error("invalid_redirect_uri")
        grants = data.get("grant_types", ["authorization_code"])
        if (data.get("token_endpoint_auth_method", "none") != "none"
                or data.get("response_types", ["code"]) != ["code"]
                or not isinstance(grants, list) or "authorization_code" not in grants):
            return error("invalid_client_metadata")
        client_id = secrets.token_urlsafe(24)
        await self.store.register(client_id, redirects)
        return JSONResponse({
            "client_id": client_id, "redirect_uris": redirects,
            "response_types": ["code"], "grant_types": ["authorization_code", "refresh_token"],
            "token_endpoint_auth_method": "none",
        }, 201, headers=NO_STORE)

    async def authorize(self, request):
        if request.method == "POST":
            if request.headers.get("origin") != self.origin:
                return error("invalid_origin", 403)
            params = await form(request)
        else:
            params = request.query_params
            if len(dict(params)) != len(params.multi_items()):
                raise InvalidRequest
        client_id, redirect_uri = params.get("client_id", ""), params.get("redirect_uri", "")
        if not PUBLIC_CLIENT_ID.fullmatch(client_id):
            return error("invalid_client_or_redirect_uri")
        registered = await self.store.fetch(
            "SELECT redirect_uris FROM mcp_auth.clients WHERE id = %s", (client_id,)
        )
        if not registered or redirect_uri not in registered["redirect_uris"]:
            return error("invalid_client_or_redirect_uri")
        if not self.valid_resource(params):
            return error("invalid_target")
        if (not CHALLENGE.fullmatch(params.get("code_challenge", ""))
                or params.get("code_challenge_method") != "S256"
                or params.get("response_type") != "code"
                or len(params.get("state", "")) > 2048):
            raise InvalidRequest
        flow = {
            "purpose": "mcp", "client_id": client_id, "redirect_uri": redirect_uri,
            "challenge": params["code_challenge"], "client_state": params.get("state"),
        }
        if request.method == "POST":
            return await self.start_login(flow)

        fields = "".join(
            f'<input type="hidden" name="{key}" value="{escape(params[key])}">'
            for key in ("client_id", "redirect_uri", "code_challenge", "code_challenge_method",
                        "response_type", "state", "resource") if key in params
        )
        return page("Connect to the Hack Club Data Warehouse",
                    f"<p>Allow the client at <strong>{escape(redirect_uri)}</strong> to run "
                    "read-only queries under your Hack Club account? Tool requests are logged.</p>"
                    '<form method="post" action="/authorize">'
                    f"{fields}<button>Continue with Hack Club</button></form>")

    async def start_login(self, flow):
        state, browser, nonce, verifier = (secrets.token_urlsafe(32) for _ in range(4))
        await self.store.save_flow(state, browser, {**flow, "nonce": nonce, "verifier": verifier})
        query = urlencode({
            "client_id": self.client_id, "redirect_uri": self.callback_url,
            "response_type": "code", "scope": "openid profile email", "state": state,
            "nonce": nonce, "code_challenge": pkce(verifier), "code_challenge_method": "S256",
        })
        target = f"{ISSUER}/oauth/authorize?{query}"
        # Finish the same-origin form submission before navigating to the issuer.
        # Otherwise some browsers apply form-action to the entire redirect chain,
        # including the OAuth client's callback when the user is already signed in.
        response = page(
            "Continue to Hack Club Auth",
            f'<p><a href="{escape(target)}">Continue to Hack Club Auth</a></p>',
        )
        response.headers["Refresh"] = f"0; url={target}"
        response.set_cookie(FLOW_COOKIE, browser, max_age=600, path="/oauth/callback",
                            secure=self.secure, httponly=True, samesite="lax")
        return response

    async def identity(self, code, flow):
        response = await self.http.post(f"{ISSUER}/oauth/token", data={
            "grant_type": "authorization_code", "client_id": self.client_id,
            "client_secret": self.client_secret, "redirect_uri": self.callback_url,
            "code": code, "code_verifier": flow["verifier"],
        })
        response.raise_for_status()
        tokens = response.json()
        id_token = tokens["id_token"]
        key_id = jwt.get_unverified_header(id_token)["kid"]
        keys = await self.http.get(f"{ISSUER}/oauth/discovery/keys")
        keys.raise_for_status()
        key = next((key for key in keys.json()["keys"] if key["kid"] == key_id), None)
        if key is None:
            raise InvalidRequest
        claims = jwt.decode(
            id_token, jwt.PyJWK.from_dict(key, algorithm="RS256").key,
            algorithms=["RS256"], audience=self.client_id, issuer=ISSUER,
            options={"require": ["exp", "iat", "sub", "nonce"]}, leeway=15,
        )
        subject = claims["sub"]
        if (claims["nonce"] != flow["nonce"] or not subject or "\x00" in subject
                or claims.get("azp", self.client_id) != self.client_id
                or claims["aud"] not in (self.client_id, [self.client_id])):
            raise InvalidRequest
        if not claims.get("email"):
            userinfo = await self.http.get(
                f"{ISSUER}/oauth/userinfo",
                headers={"Authorization": f'Bearer {tokens["access_token"]}'},
            )
            userinfo.raise_for_status()
            profile = userinfo.json()
            if profile.get("sub") != subject:
                raise InvalidRequest
            claims.update(profile)
        email = claims.get("email")
        if (not isinstance(email, str) or not email.strip() or "\x00" in email
                or claims.get("email_verified") is not True):
            raise InvalidRequest
        return {
            "subject": subject, "email": email.strip().lower(),
            "first_name": self.profile_name(claims.get("given_name")),
            "last_name": self.profile_name(claims.get("family_name")),
        }

    @staticmethod
    def profile_name(value):
        # Names also appear in SQL line comments, so strip line breaks and controls.
        return " ".join(value.replace("\x00", "").split())[:200] if isinstance(value, str) else ""

    async def callback(self, request):
        params = request.query_params
        state, browser = params.get("state"), request.cookies.get(FLOW_COOKIE)
        if not state or not browser or len(dict(params)) != len(params.multi_items()):
            raise InvalidRequest
        pending = await self.store.consume_flow(state, browser)
        if pending is None:
            return page("Sign-in expired", "<p>Start sign-in again from your client.</p>", 400)
        flow = pending["details"]
        credential = secrets.token_urlsafe(32)
        try:
            if "error" in params or not params.get("code"):
                raise InvalidRequest
            identity = await self.identity(params["code"], flow)
        except (httpx.HTTPError, jwt.PyJWTError, InvalidRequest, KeyError, ValueError, TypeError) as exc:
            logger.warning("Hack Club Auth sign-in failed (%s)", type(exc).__name__)
            response = page("Sign-in failed", "<p>Start sign-in again and use a verified Hack Club account.</p>", 400)
        else:
            allowed = await self.store.complete_login(identity, flow, credential, self.origin)
            if not allowed:
                guidance = (
                    "Sign in using the configured superadmin account."
                    if flow["purpose"] == "admin"
                    else "Ask the warehouse superadmin to approve your account, then sign in again."
                )
                response = page("Access required", f"<p>{guidance}</p>", 403)
            elif flow["purpose"] == "admin":
                response = RedirectResponse("/admin", 303, headers=NO_STORE)
                response.set_cookie(ADMIN_COOKIE, credential, max_age=28800, path="/admin",
                                    secure=self.secure, httponly=True, samesite="lax")
            else:
                redirect_uri = flow["redirect_uri"]
                query = {"code": credential}
                if flow["client_state"] is not None:
                    query["state"] = flow["client_state"]
                sep = "&" if "?" in redirect_uri else "?"
                response = RedirectResponse(redirect_uri + sep + urlencode(query), 303, headers=NO_STORE)
        response.delete_cookie(FLOW_COOKIE, path="/oauth/callback")
        return response

    async def token(self, request):
        params = await form(request)
        grant_type = params.get("grant_type")
        if grant_type not in {"authorization_code", "refresh_token"}:
            return error("unsupported_grant_type")
        if not self.valid_resource(params):
            return error("invalid_target")
        access_token = secrets.token_urlsafe(32)
        refresh_token = secrets.token_urlsafe(32)
        if grant_type == "authorization_code":
            verifier = params.get("code_verifier", "")
            if not VERIFIER.fullmatch(verifier):
                return error("invalid_grant")
            issued = await self.store.exchange_code(
                params.get("code", ""), params.get("client_id", ""),
                params.get("redirect_uri", ""), pkce(verifier), self.origin,
                access_token, refresh_token,
            )
            if not issued:
                return error("invalid_grant")
        else:
            client_id = params.get("client_id", "")
            presented = params.get("refresh_token", "")
            if not PUBLIC_CLIENT_ID.fullmatch(client_id) or not presented:
                return error("invalid_grant")
            result = await self.store.exchange_refresh_token(
                presented, client_id, self.origin, access_token, refresh_token,
                requested_scope=params.get("scope"),
            )
            if result != "ok":
                return error(result)
        return JSONResponse({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 28800,
            "refresh_token": refresh_token,
        }, headers={**NO_STORE, "Pragma": "no-cache"})

    async def admin(self, request):
        token = request.cookies.get(ADMIN_COOKIE)
        admin = await self.store.session_user(token, "admin", self.origin) if token else None
        if request.method == "POST":
            if request.headers.get("origin") != self.origin:
                return error("invalid_origin", 403)
            params = await form(request)
            action = params.get("action")
            if action == "login" and not admin:
                return await self.start_login({"purpose": "admin"})
            if not admin:
                return error("access_denied", 403)
            if action == "logout":
                await self.store.fetch("""
                    DELETE FROM mcp_auth.sessions WHERE token_hash = %s AND purpose = 'admin'
                    RETURNING token_hash
                """, (digest(token),))
                response = RedirectResponse("/admin", 303, headers=NO_STORE)
                response.delete_cookie(ADMIN_COOKIE, path="/admin")
                return response
            if action in {"invite_email", "remove_invite"}:
                email = params.get("email", "").strip().lower()
                if len(email) > 254 or not re.fullmatch(r"[^\s@]+@[^\s@]+", email):
                    return page("Invalid email", '<p>Enter an email address. <a href="/admin">Back to access</a>.</p>', 400)
                allowed = await self.store.set_email_invite(
                    token, self.origin, email, action == "invite_email",
                )
            elif action in {"approve", "revoke"} and params.get("subject"):
                allowed = await self.store.set_access(token, self.origin, params["subject"], action == "approve")
            else:
                raise InvalidRequest
            if not allowed:
                return error("access_denied", 403)
            return RedirectResponse("/admin", 303, headers=NO_STORE)
        if not admin:
            return page("Warehouse administration", '<form method="post"><button name="action" '
                        'value="login">Sign in with Hack Club</button></form>')
        rows = []
        for user in await self.store.users():
            action = "revoke" if user["approved"] else "approve"
            status = "Approved" if user["approved"] else "No access"
            rows.append(f'<li>{escape(user["email"])} · {escape(user["subject"])} · {status}'
                        '<form method="post">'
                        f'<input type="hidden" name="subject" value="{escape(user["subject"])}">'
                        f'<button name="action" value="{action}">{action.title()}</button>'
                        "</form></li>")
        invitations = []
        for invite in await self.store.email_invites():
            email = escape(invite["email"])
            invitations.append(
                f'<li><form method="post">{email}'
                f'<input type="hidden" name="email" value="{email}">'
                '<button name="action" value="remove_invite">Remove invitation</button></form></li>'
            )
        return page(
            "Warehouse access",
            '<form method="post"><button name="action" value="logout">Sign out</button></form>'
            '<h2>Invite by email</h2><p>Access is granted when the person signs in through their MCP '
            'client with this verified Hack Club Auth email.</p>'
            '<form method="post"><label>Email <input type="email" name="email" required maxlength="254"></label>'
            '<button name="action" value="invite_email">Invite</button></form>'
            '<h2>Pending invitations</h2>'
            + ("<ul>" + "".join(invitations) + "</ul>" if invitations else "<p>No pending invitations.</p>")
            + '<h2>Accounts</h2>'
            + ("<ul>" + "".join(rows) + "</ul>" if rows else "<p>No other accounts have signed in yet.</p>"),
        )
