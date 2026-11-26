"""Exercise the authorization bridge through the MCP HTTP endpoint."""

import asyncio
from html import escape
import json
import os
import time
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest

pytest.importorskip("mcp")
pytest.importorskip("psycopg_pool")

import httpx
import jwt
import psycopg
from cryptography.hazmat.primitives.asymmetric import rsa
from starlette.testclient import TestClient

import oauth
import server
from db import Database
from migrate import MIGRATIONS, apply_auth_migrations
from oauth import pkce


@pytest.fixture
def auth_database():
    dsn = os.environ.get("TEST_MCP_AUTH_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires a disposable test Postgres database")
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute("SELECT current_database()").fetchone() == ("warehouse_mcp_test",)
        assert conn.execute("SELECT to_regnamespace('mcp_auth')").fetchone() == (None,)
        try:
            yield dsn
        finally:
            conn.execute("DROP SCHEMA IF EXISTS mcp_auth CASCADE")


def test_oauth_approval_token_exchange_and_audit(auth_database, monkeypatch):
    origin = "http://localhost:3000"
    headers = {"Origin": origin}
    callback = "http://127.0.0.1:6543/callback?source=test"
    admin_email = "admin@example.test"
    user_email = "member@example.test"
    monkeypatch.setenv("MCP_PUBLIC_URL", origin)
    monkeypatch.setenv("HACKCLUB_CLIENT_ID", "test-client")
    monkeypatch.setenv("HACKCLUB_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("MCP_SUPERADMIN_EMAIL", admin_email)
    monkeypatch.setenv("MCP_AUTH_DATABASE_URL", auth_database)

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk["kid"] = "oidc-test-key"
    upstream_grants = {}

    def hca(request):
        if request.url.path == "/oauth/discovery/keys":
            return httpx.Response(200, json={"keys": [jwk]})
        assert request.url.path == "/oauth/token"
        fields = parse_qs(request.content.decode())
        assert fields["redirect_uri"] == [origin + "/oauth/callback"]
        assert fields["client_secret"] == ["test-secret"]
        challenge, nonce, subject, email, verified = upstream_grants.pop(fields["code"][0])
        assert pkce(fields["code_verifier"][0]) == challenge
        now = int(time.time())
        id_token = jwt.encode({
            "iss": oauth.ISSUER, "aud": "test-client", "iat": now, "exp": now + 60,
            "nonce": nonce, "sub": subject, "email": email, "email_verified": verified,
            "given_name": "Example", "family_name": "Member",
        }, private_key, algorithm="RS256", headers={"kid": jwk["kid"]})
        return httpx.Response(200, json={"id_token": id_token})

    async_client = httpx.AsyncClient
    monkeypatch.setattr(oauth.httpx, "AsyncClient", lambda **kw: async_client(
        **kw, transport=httpx.MockTransport(hca),
    ))
    warehouse = MagicMock()
    warehouse.check_auth_database_isolation = AsyncMock()
    monkeypatch.setattr(server, "init_database", AsyncMock(return_value=warehouse))
    monkeypatch.setattr(server, "close_database", AsyncMock())
    monkeypatch.setattr(server, "session_manager", server.StreamableHTTPSessionManager(
        app=server.mcp, stateless=True, json_response=True,
    ))
    db = MagicMock()
    db.execute_query = AsyncMock(return_value=([{"n": 42}], ["n"]))
    monkeypatch.setattr(server, "get_database", lambda: db)

    with TestClient(server.create_app(), base_url=origin, follow_redirects=False) as client:
        warehouse.check_auth_database_isolation.assert_awaited_once()
        assert client.get("/health").status_code == 200
        metadata = client.get("/.well-known/oauth-authorization-server")
        assert metadata.status_code == 200
        assert metadata.json()["grant_types_supported"] == ["authorization_code", "refresh_token"]
        unauthorized = client.get("/")
        assert unauthorized.status_code == 401
        assert origin in unauthorized.headers["www-authenticate"]
        assert client.post("/register", json={"redirect_uris": ["http://example.test/"]}).status_code == 400
        registered = client.post("/register", json={"redirect_uris": [callback]})
        assert registered.status_code == 201
        assert registered.json()["grant_types"] == ["authorization_code", "refresh_token"]
        client_id = registered.json()["client_id"]
        verifier = "a" * 43
        params = {
            "client_id": client_id, "redirect_uri": callback,
            "code_challenge": pkce(verifier), "code_challenge_method": "S256",
            "response_type": "code", "resource": origin, "state": "client-state",
        }
        assert client.get("/authorize", params={**params, "redirect_uri": origin}).status_code == 400
        assert client.post("/authorize", data=params).status_code == 403

        def start():
            consent = client.get("/authorize", params=params)
            assert consent.status_code == 200
            assert callback in consent.text
            assert consent.headers["Content-Security-Policy"].endswith("form-action 'self'")
            return client.post("/authorize", data=params, headers=headers)

        def finish(login, subject, email, verified=True):
            # The POST must finish on the MCP origin. The browser then starts a
            # separate navigation (or the user follows the fallback link).
            assert login.status_code == 200
            assert "location" not in login.headers
            assert login.headers["Cache-Control"] == "no-store"
            assert login.headers["Content-Security-Policy"].endswith("form-action 'self'")
            prefix, target_url = login.headers["Refresh"].split("url=", 1)
            assert prefix == "0; "
            assert f'href="{escape(target_url)}"' in login.text
            target = urlsplit(target_url)
            assert f"{target.scheme}://{target.netloc}" == oauth.ISSUER
            assert target.path == "/oauth/authorize"
            fields = parse_qs(target.query)
            code = uuid4().hex
            upstream_grants[code] = (
                fields["code_challenge"][0], fields["nonce"][0], subject, email, verified,
            )
            return client.get("/oauth/callback", params={"state": fields["state"][0], "code": code})

        assert finish(start(), "member", user_email).status_code == 403
        def admin_login():
            login_page = client.get("/admin")
            assert login_page.headers["Content-Security-Policy"].endswith("form-action 'self'")
            return client.post("/admin", data={"action": "login"}, headers=headers)

        assert finish(admin_login(), "fake-admin", admin_email, verified=False).status_code == 400
        assert finish(admin_login(), "member", user_email).status_code == 403
        assert finish(admin_login(), "admin", admin_email).status_code == 303
        admin_page = client.get("/admin")
        assert user_email in admin_page.text
        assert admin_page.headers["Content-Security-Policy"].endswith("form-action 'self'")
        invited_email = "invited@example.test"
        invite = {"action": "invite_email", "email": " Invited@Example.Test "}
        assert client.post("/admin", data=invite).status_code == 403
        assert client.post("/admin", data={"action": "invite_email", "email": "invalid"}, headers=headers).status_code == 400
        assert client.post("/admin", data=invite, headers=headers).status_code == 303
        assert client.post("/admin", data=invite, headers=headers).status_code == 303
        assert invited_email in client.get("/admin").text
        assert finish(start(), "unverified-invite", invited_email, verified=False).status_code == 400
        assert finish(start(), "outside", "outside@example.test").status_code == 403
        assert finish(start(), "invited", invited_email).status_code == 303

        remove = {"action": "remove_invite", "email": "cancelled@example.test"}
        assert client.post("/admin", data={**remove, "action": "invite_email"}, headers=headers).status_code == 303
        assert client.post("/admin", data=remove, headers=headers).status_code == 303
        assert finish(start(), "cancelled", remove["email"]).status_code == 403
        with psycopg.connect(auth_database) as conn:
            assert conn.execute("SELECT count(*) FROM mcp_auth.email_invites").fetchone() == (0,)
            assert conn.execute("SELECT approved FROM mcp_auth.users WHERE subject = 'invited'").fetchone() == (True,)
            assert conn.execute("""
                SELECT count(*) FROM mcp_auth.audit
                WHERE record->>'event' = 'mcp_email_invite_changed' AND record->>'target_email' = %s
            """, (invited_email,)).fetchone() == (1,)

        assert client.post("/admin", data={"action": "approve", "subject": "member"}).status_code == 403
        assert client.post("/admin", data={"action": "approve", "subject": "member"}, headers=headers).status_code == 303

        completed = finish(start(), "member", user_email)
        assert completed.status_code == 303
        result = parse_qs(urlsplit(completed.headers["location"]).query)
        assert result["state"] == ["client-state"]
        assert result["source"] == ["test"]
        exchange = {
            "grant_type": "authorization_code", "code": result["code"][0],
            "code_verifier": verifier, "client_id": client_id, "redirect_uri": callback,
            "resource": origin,
        }
        assert client.post("/token", data={**exchange, "resource": "https://elsewhere.test"}).status_code == 400
        assert client.post("/token", data={**exchange, "code_verifier": "b" * 43}).status_code == 400
        assert client.post("/token", data={**exchange, "redirect_uri": origin}).status_code == 400
        issued = client.post("/token", data=exchange)
        assert issued.status_code == 200
        token = issued.json()["access_token"]
        refresh_token = issued.json()["refresh_token"]
        assert len(refresh_token) == 43
        assert client.post("/token", data=exchange).status_code == 400

        auth = {"Authorization": f"Bearer {token}", "Accept": "application/json, text/event-stream"}
        resp = client.post("/", headers=auth, json={
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "query", "arguments": {"sql": "SELECT 42 AS n"}},
        })
        assert resp.status_code == 200
        assert "Query ID:" in resp.json()["result"]["content"][0]["text"]
        db.execute_query.assert_awaited_once_with("SELECT 42 AS n", user_info=("Example", "Member"))

        with psycopg.connect(auth_database) as conn:
            started = conn.execute("""
                SELECT record FROM mcp_auth.audit WHERE record->>'event' = 'mcp_tool_started'
            """).fetchone()[0]
            assert started["caller_subject"] == "member"
            assert started["sql"] == "SELECT 42 AS n"
            event = conn.execute("""
                SELECT record FROM mcp_auth.audit WHERE record->>'event' = 'mcp_tool_call'
            """).fetchone()[0]
            assert event["caller_subject"] == "member"
            assert event["caller_email"] == user_email
            assert event["request_id"] == started["request_id"]
            assert event["sql"] == "SELECT 42 AS n"
            assert event["outcome"] == "ok"
            assert token not in json.dumps(event)
            assert conn.execute(
                "SELECT token_hash FROM mcp_auth.sessions WHERE purpose = 'mcp'"
            ).fetchone() == (oauth.digest(token),)

        with monkeypatch.context() as audit_failure:
            audit_failure.setattr(oauth.AuthStore, "audit", AsyncMock(side_effect=psycopg.OperationalError))
            refused = client.post("/", headers=auth, json={
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "query", "arguments": {"sql": "SELECT 10"}},
            })
            assert refused.json()["result"]["isError"]
            assert db.execute_query.await_count == 1

        async def query_during_revocation(*args, **kwargs):
            revoked = await asyncio.to_thread(
                client.post, "/admin", data={"action": "revoke", "subject": "member"}, headers=headers,
            )
            assert revoked.status_code == 303
            return [{"n": 42}], ["n"]

        db.execute_query.side_effect = query_during_revocation
        in_flight = client.post("/", headers=auth, json={
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "query", "arguments": {"sql": "SELECT 43"}},
        })
        assert in_flight.json()["result"]["isError"]
        assert "authorization expired or was revoked" in in_flight.text
        with psycopg.connect(auth_database) as conn:
            assert conn.execute("""
                SELECT count(*) FROM mcp_auth.audit
                WHERE record->>'outcome' = 'access_expired_or_revoked'
            """).fetchone() == (1,)
        assert client.get("/", headers=auth).status_code == 401
        revoked_refresh = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": client_id,
        })
        assert revoked_refresh.status_code == 400
        assert revoked_refresh.json() == {"error": "invalid_grant"}

        original_admin_token = client.cookies.get(oauth.ADMIN_COOKIE)
        assert finish(start(), "admin", "updated@example.test").status_code == 403
        assert finish(admin_login(), "admin", admin_email).status_code == 303
        assert client.cookies.get(oauth.ADMIN_COOKIE) != original_admin_token
        client.cookies.set(oauth.ADMIN_COOKIE, original_admin_token, domain="localhost.local", path="/admin")
        assert client.post("/admin", data={"action": "approve", "subject": "member"}, headers=headers).status_code == 403


def test_warehouse_database_rejects_auth_schema(auth_database, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_COOLIFY_URL", auth_database)

    async def check():
        store = oauth.AuthStore(auth_database, "admin@example.test")
        warehouse = Database()
        try:
            await store.open()
            await warehouse.open()
            with pytest.raises(RuntimeError, match="require separate Postgres databases"):
                await warehouse.check_auth_database_isolation()
        finally:
            await warehouse.close()
            await store.pool.close()

    asyncio.run(check())


def test_sessions_are_bound_to_purpose_resource_and_expiration(auth_database):
    async def check():
        email = "admin@example.test"
        store = oauth.AuthStore(auth_database, email)
        resource = "https://warehouse.example.test"
        identity = {"subject": "admin", "email": email, "first_name": "", "last_name": ""}
        admin_credential = uuid4().hex
        mcp_credential = uuid4().hex
        authorization_code = uuid4().hex
        client_id = "test-client"
        redirect_uri = "https://client.example.test/callback"
        challenge = pkce("a" * 43)
        try:
            await store.open()
            await store.register(client_id, [redirect_uri])
            assert await store.complete_login(identity, {"purpose": "admin"}, admin_credential, resource)
            assert await store.complete_login(identity, {
                "purpose": "mcp", "client_id": client_id,
                "redirect_uri": redirect_uri, "challenge": challenge,
            }, authorization_code, resource)
            assert await store.exchange_code(
                authorization_code, client_id, redirect_uri, challenge, resource, mcp_credential,
                uuid4().hex,
            )
            assert await store.session_user(admin_credential, "admin", resource)
            assert await store.session_user(mcp_credential, "mcp", resource)
            assert await store.session_user(admin_credential, "mcp", resource) is None
            assert await store.session_user(mcp_credential, "admin", resource) is None
            assert await store.session_user(mcp_credential, "mcp", "https://other.example.test") is None

            async with store.pool.connection() as conn:
                await conn.execute("UPDATE mcp_auth.sessions SET expires_at = now()")
            assert await store.session_user(admin_credential, "admin", resource) is None
            assert await store.session_user(mcp_credential, "mcp", resource) is None
            assert not await store.set_access(admin_credential, resource, "member", True)
        finally:
            await store.pool.close()

    asyncio.run(check())


def test_auth_migrations_adopt_existing_schema_and_are_idempotent(auth_database):
    async def check():
        async with await psycopg.AsyncConnection.connect(auth_database) as conn:
            migration = MIGRATIONS / "0001_initial.sql"
            await conn.execute(migration.read_text())
            await conn.execute("""
                INSERT INTO mcp_auth.audit (record) VALUES ('{"event": "existing_audit"}')
            """)
            await conn.commit()

            expected = [
                migration.name,
                "0002_email_invites.sql",
                "0003_refresh_tokens.sql",
                "0004_refresh_token_ttl_90d.sql",
            ]
            assert await apply_auth_migrations(conn) == expected
            assert await apply_auth_migrations(conn) == []
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT version FROM mcp_auth.schema_migrations ORDER BY version")
                assert await cursor.fetchall() == [(name,) for name in expected]
                await cursor.execute("SELECT record->>'event' FROM mcp_auth.audit")
                assert await cursor.fetchall() == [("existing_audit",)]

    asyncio.run(check())


def test_cache_results_are_scoped_to_identity():
    cache = server.get_cache()
    cached = cache.store("SELECT 1", [{"value": 1}], ["value"], subject="member")
    assert cache.get_rows(cached.query_id, subject="member") == ([{"value": 1}], 1)
    assert cache.get_rows(cached.query_id, subject="another-member") is None
    assert cache.grep_rows(cached.query_id, "1", subject="another-member") is None


def test_refresh_token_rotation_binding_scope_expiry_and_reuse_detection(auth_database, monkeypatch):
    origin = "http://localhost:3000"
    headers = {"Origin": origin}
    callback = "http://127.0.0.1:6543/callback"
    admin_email = "admin@example.test"
    monkeypatch.setenv("MCP_PUBLIC_URL", origin)
    monkeypatch.setenv("HACKCLUB_CLIENT_ID", "test-client")
    monkeypatch.setenv("HACKCLUB_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("MCP_SUPERADMIN_EMAIL", admin_email)
    monkeypatch.setenv("MCP_AUTH_DATABASE_URL", auth_database)

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk["kid"] = "oidc-refresh-key"
    upstream_grants = {}

    def hca(request):
        if request.url.path == "/oauth/discovery/keys":
            return httpx.Response(200, json={"keys": [jwk]})
        fields = parse_qs(request.content.decode())
        challenge, nonce, subject, email = upstream_grants.pop(fields["code"][0])
        assert pkce(fields["code_verifier"][0]) == challenge
        now = int(time.time())
        id_token = jwt.encode({
            "iss": oauth.ISSUER, "aud": "test-client", "iat": now, "exp": now + 60,
            "nonce": nonce, "sub": subject, "email": email, "email_verified": True,
            "given_name": "Admin", "family_name": "User",
        }, private_key, algorithm="RS256", headers={"kid": jwk["kid"]})
        return httpx.Response(200, json={"id_token": id_token})

    async_client = httpx.AsyncClient
    monkeypatch.setattr(oauth.httpx, "AsyncClient", lambda **kw: async_client(
        **kw, transport=httpx.MockTransport(hca),
    ))
    warehouse = MagicMock()
    warehouse.check_auth_database_isolation = AsyncMock()
    monkeypatch.setattr(server, "init_database", AsyncMock(return_value=warehouse))
    monkeypatch.setattr(server, "close_database", AsyncMock())
    monkeypatch.setattr(server, "session_manager", server.StreamableHTTPSessionManager(
        app=server.mcp, stateless=True, json_response=True,
    ))
    db = MagicMock()
    db.execute_query = AsyncMock(return_value=([{"n": 1}], ["n"]))
    monkeypatch.setattr(server, "get_database", lambda: db)

    with TestClient(server.create_app(), base_url=origin, follow_redirects=False) as client:
        client_id = client.post("/register", json={"redirect_uris": [callback]}).json()["client_id"]
        other_client_id = client.post("/register", json={"redirect_uris": [callback]}).json()["client_id"]
        verifier = "v" * 43

        def issue_tokens(subject="admin", email=admin_email):
            params = {
                "client_id": client_id, "redirect_uri": callback,
                "code_challenge": pkce(verifier), "code_challenge_method": "S256",
                "response_type": "code", "resource": origin,
            }
            login = client.post("/authorize", data=params, headers=headers)
            _, target_url = login.headers["Refresh"].split("url=", 1)
            fields = parse_qs(urlsplit(target_url).query)
            code = uuid4().hex
            upstream_grants[code] = (fields["code_challenge"][0], fields["nonce"][0], subject, email)
            cb = client.get("/oauth/callback", params={"state": fields["state"][0], "code": code})
            assert cb.status_code == 303
            auth_code = parse_qs(urlsplit(cb.headers["location"]).query)["code"][0]
            resp = client.post("/token", data={
                "grant_type": "authorization_code", "code": auth_code,
                "code_verifier": verifier, "client_id": client_id, "redirect_uri": callback,
            })
            assert resp.status_code == 200
            return resp.json()

        first = issue_tokens()
        rt1 = first["refresh_token"]
        at1 = first["access_token"]
        assert first["expires_in"] == 28800

        # Refresh token is stored hashed, never plaintext, with a 90-day TTL.
        with psycopg.connect(auth_database) as conn:
            token_hash, ttl_seconds = conn.execute(
                "SELECT token_hash, extract(epoch FROM expires_at - now()) "
                "FROM mcp_auth.refresh_tokens WHERE subject = 'admin'"
            ).fetchone()
            assert token_hash == oauth.digest(rt1)
            assert 90 * 86400 - 60 <= ttl_seconds <= 90 * 86400 + 60

        # Missing client_id, wrong client_id, wrong resource, and scope escalation
        # must all fail WITHOUT consuming or revoking rt1.
        assert client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1,
        }).json() == {"error": "invalid_grant"}
        assert client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1, "client_id": other_client_id,
        }).json() == {"error": "invalid_grant"}
        assert client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1,
            "client_id": client_id, "resource": "https://elsewhere.test",
        }).json() == {"error": "invalid_target"}
        bad_scope = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1,
            "client_id": client_id, "scope": "admin",
        })
        assert bad_scope.status_code == 400
        assert bad_scope.json() == {"error": "invalid_scope"}

        # Valid refresh (without resource, matching standard OAuth2 clients) rotates tokens.
        refreshed = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1, "client_id": client_id,
        })
        assert refreshed.status_code == 200
        second = refreshed.json()
        at2 = second["access_token"]
        rt2 = second["refresh_token"]
        assert at2 != at1
        assert rt2 != rt1
        assert second["token_type"] == "Bearer"
        assert second["expires_in"] == 28800

        # New access token works on the MCP endpoint.
        auth2 = {"Authorization": f"Bearer {at2}", "Accept": "application/json, text/event-stream"}
        tool_resp = client.post("/", headers=auth2, json={
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "query", "arguments": {"sql": "SELECT 1 AS n"}},
        })
        assert tool_resp.status_code == 200
        assert "Query ID:" in tool_resp.json()["result"]["content"][0]["text"]

        # Refresh audit event is recorded without raw refresh tokens, and rotated token gets 90-day TTL.
        with psycopg.connect(auth_database) as conn:
            rt2_ttl_seconds = conn.execute(
                "SELECT extract(epoch FROM expires_at - now()) "
                "FROM mcp_auth.refresh_tokens WHERE token_hash = %s",
                (oauth.digest(rt2),),
            ).fetchone()[0]
            assert 90 * 86400 - 60 <= rt2_ttl_seconds <= 90 * 86400 + 60
            audit_row = conn.execute("""
                SELECT record FROM mcp_auth.audit WHERE record->>'event' = 'mcp_token_refreshed'
            """).fetchone()[0]
            assert audit_row["caller_subject"] == "admin"
            assert audit_row["client_id"] == client_id
            assert rt1 not in json.dumps(audit_row)
            assert rt2 not in json.dumps(audit_row)
            assert at2 not in json.dumps(audit_row)

        # Retry after a lost response within the grace window succeeds without revoking the family.
        lost_retry = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1, "client_id": client_id,
        })
        assert lost_retry.status_code == 200
        rt_retry = lost_retry.json()["refresh_token"]
        at_retry = lost_retry.json()["access_token"]
        assert rt_retry not in {rt1, rt2}
        # Both the original winner (at2/rt2) and the retry (at_retry/rt_retry) still work.
        assert client.post("/", headers=auth2, json={
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "query", "arguments": {"sql": "SELECT 1 AS n"}},
        }).status_code == 200
        auth_retry = {"Authorization": f"Bearer {at_retry}", "Accept": "application/json, text/event-stream"}
        assert client.post("/", headers=auth_retry, json={
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "query", "arguments": {"sql": "SELECT 1 AS n"}},
        }).status_code == 200
        # Successor token rt_retry can continue refreshing.
        after_retry = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt_retry, "client_id": client_id,
        })
        assert after_retry.status_code == 200
        rt_latest = after_retry.json()["refresh_token"]
        at_latest = after_retry.json()["access_token"]
        auth_latest = {"Authorization": f"Bearer {at_latest}", "Accept": "application/json, text/event-stream"}

        # Two concurrent refreshes with the same token both succeed and keep the family alive.
        concurrent_grant = issue_tokens()
        rt_shared = concurrent_grant["refresh_token"]
        race_a = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt_shared, "client_id": client_id,
        })
        race_b = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt_shared, "client_id": client_id,
        })
        assert race_a.status_code == 200
        assert race_b.status_code == 200
        for idx, winner in enumerate((race_a.json(), race_b.json()), start=10):
            winner_auth = {
                "Authorization": f'Bearer {winner["access_token"]}',
                "Accept": "application/json, text/event-stream",
            }
            assert client.post("/", headers=winner_auth, json={
                "jsonrpc": "2.0", "id": idx, "method": "tools/call",
                "params": {"name": "query", "arguments": {"sql": "SELECT 1 AS n"}},
            }).status_code == 200
            next_refresh = client.post("/token", data={
                "grant_type": "refresh_token", "refresh_token": winner["refresh_token"],
                "client_id": client_id,
            })
            assert next_refresh.status_code == 200

        # Replaying rt1 AFTER the grace window triggers reuse detection and revokes the whole family.
        with psycopg.connect(auth_database, autocommit=True) as conn:
            conn.execute(
                "UPDATE mcp_auth.refresh_tokens SET consumed_at = now() - interval '91 seconds' "
                "WHERE token_hash = %s",
                (oauth.digest(rt1),),
            )
        replayed = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt1, "client_id": client_id,
        })
        assert replayed.status_code == 400
        assert replayed.json() == {"error": "invalid_grant"}

        # Both the newest refresh token (rt_latest) and the family's access tokens (at2, at_latest) are revoked.
        assert client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt_latest, "client_id": client_id,
        }).status_code == 400
        assert client.get("/", headers=auth2).status_code == 401
        assert client.get("/", headers=auth_latest).status_code == 401
        with psycopg.connect(auth_database) as conn:
            assert conn.execute("""
                SELECT count(*) FROM mcp_auth.audit
                WHERE record->>'event' = 'mcp_refresh_token_reuse_detected'
            """).fetchone() == (1,)

        # Expired refresh tokens are rejected.
        third = issue_tokens()
        rt3 = third["refresh_token"]
        with psycopg.connect(auth_database, autocommit=True) as conn:
            conn.execute(
                "UPDATE mcp_auth.refresh_tokens SET expires_at = now() - interval '1 second' WHERE token_hash = %s",
                (oauth.digest(rt3),),
            )
        expired_resp = client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt3, "client_id": client_id,
        })
        assert expired_resp.status_code == 400
        assert expired_resp.json() == {"error": "invalid_grant"}

        # Email change revokes outstanding refresh tokens for that subject.
        fourth = issue_tokens()
        rt4 = fourth["refresh_token"]
        params = {
            "client_id": client_id, "redirect_uri": callback,
            "code_challenge": pkce(verifier), "code_challenge_method": "S256",
            "response_type": "code", "resource": origin,
        }
        login = client.post("/authorize", data=params, headers=headers)
        _, target_url = login.headers["Refresh"].split("url=", 1)
        fields = parse_qs(urlsplit(target_url).query)
        code = uuid4().hex
        upstream_grants[code] = (fields["code_challenge"][0], fields["nonce"][0], "admin", "changed@example.test")
        assert client.get("/oauth/callback", params={"state": fields["state"][0], "code": code}).status_code == 403
        assert client.post("/token", data={
            "grant_type": "refresh_token", "refresh_token": rt4, "client_id": client_id,
        }).status_code == 400


@pytest.mark.parametrize(
    "public_url, sends_hsts",
    [("https://mcp.example.test", True), ("http://localhost:3000", False)],
)
def test_security_headers_follow_the_public_origin(monkeypatch, public_url, sends_hsts):
    # The proxy ends TLS and reaches the app over plain HTTP, so HSTS must
    # follow MCP_PUBLIC_URL and not the request scheme.
    monkeypatch.setenv("MCP_PUBLIC_URL", public_url)

    async def inner(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    monkeypatch.setattr(server, "app", inner)
    response = TestClient(server.create_app(), base_url="http://app.internal:3000").get("/health")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    if sends_hsts:
        assert response.headers["strict-transport-security"] == "max-age=31536000; includeSubDomains"
    else:
        assert "strict-transport-security" not in response.headers
