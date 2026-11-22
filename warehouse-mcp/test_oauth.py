"""Exercise the authorization bridge through the MCP HTTP endpoint."""

import asyncio
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
        unauthorized = client.get("/")
        assert unauthorized.status_code == 401
        assert origin in unauthorized.headers["www-authenticate"]
        assert client.post("/register", json={"redirect_uris": ["http://example.test/"]}).status_code == 400
        registered = client.post("/register", json={"redirect_uris": [callback]})
        assert registered.status_code == 201
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
            return client.post("/authorize", data=params, headers=headers)

        def finish(login, subject, email, verified=True):
            assert login.status_code == 303
            target = urlsplit(login.headers["location"])
            assert target.hostname == "auth.hackclub.com"
            fields = parse_qs(target.query)
            code = uuid4().hex
            upstream_grants[code] = (
                fields["code_challenge"][0], fields["nonce"][0], subject, email, verified,
            )
            return client.get("/oauth/callback", params={"state": fields["state"][0], "code": code})

        assert finish(start(), "member", user_email).status_code == 403
        def admin_login():
            return client.post("/admin", data={"action": "login"}, headers=headers)

        assert finish(admin_login(), "fake-admin", admin_email, verified=False).status_code == 400
        assert finish(admin_login(), "member", user_email).status_code == 403
        assert finish(admin_login(), "admin", admin_email).status_code == 303
        assert user_email in client.get("/admin").text
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


def test_cache_results_are_scoped_to_identity():
    cache = server.get_cache()
    cached = cache.store("SELECT 1", [{"value": 1}], ["value"], subject="member")
    assert cache.get_rows(cached.query_id, subject="member") == ([{"value": 1}], 1)
    assert cache.get_rows(cached.query_id, subject="another-member") is None
    assert cache.grep_rows(cached.query_id, "1", subject="another-member") is None
