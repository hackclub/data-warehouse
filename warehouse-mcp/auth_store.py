"""Postgres storage for MCP access control, OAuth grants, and audit records."""

import hashlib
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from migrate import apply_auth_migrations

REFRESH_REUSE_GRACE_SECONDS = 90


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class AuthStore:
    def __init__(self, dsn: str, superadmin_email: str):
        self.superadmin_email = superadmin_email
        self.pool = AsyncConnectionPool(
            dsn, open=False, min_size=1, max_size=5, timeout=5,
            kwargs={"row_factory": dict_row, "options": "-c statement_timeout=5000"},
        )

    async def open(self):
        await self.pool.open(wait=True, timeout=10)
        async with self.pool.connection() as conn:
            await apply_auth_migrations(conn)

    async def fetch(self, sql, params=()):
        async with self.pool.connection() as conn:
            return await (await conn.execute(sql, params)).fetchone()

    async def register(self, client_id, redirect_uris):
        async with self.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO mcp_auth.clients VALUES (%s, %s)", (client_id, redirect_uris)
            )

    async def save_flow(self, state, browser, details):
        async with self.pool.connection() as conn:
            await conn.execute("""
                DELETE FROM mcp_auth.flows WHERE expires_at <= now();
                DELETE FROM mcp_auth.codes WHERE expires_at <= now();
                DELETE FROM mcp_auth.sessions WHERE expires_at <= now();
                DELETE FROM mcp_auth.refresh_tokens WHERE expires_at <= now()
            """)
            await conn.execute(
                "INSERT INTO mcp_auth.flows (state_hash, browser_hash, details) VALUES (%s, %s, %s)",
                (digest(state), digest(browser), Jsonb(details)),
            )

    async def consume_flow(self, state, browser):
        return await self.fetch("""
            DELETE FROM mcp_auth.flows
            WHERE state_hash = %s AND browser_hash = %s AND expires_at > now()
            RETURNING details
        """, (digest(state), digest(browser)))

    async def complete_login(self, identity, flow, credential, resource):
        async with self.pool.connection() as conn:
            await conn.execute("""
                INSERT INTO mcp_auth.users (subject, email, first_name, last_name)
                VALUES (%(subject)s, %(email)s, %(first_name)s, %(last_name)s)
                ON CONFLICT (subject) DO NOTHING
            """, identity)
            user = await (await conn.execute("""
                SELECT * FROM mcp_auth.users WHERE subject = %s FOR UPDATE
            """, (identity["subject"],))).fetchone()
            if user["email"] != identity["email"]:
                # Credentials issued to a previous identity snapshot cannot gain
                # privileges from a later login or become valid again.
                await self.revoke_credentials(conn, user["subject"])
            await conn.execute("""
                UPDATE mcp_auth.users SET email = %(email)s,
                    first_name = %(first_name)s, last_name = %(last_name)s
                WHERE subject = %(subject)s
            """, identity)
            admin = identity["email"] == self.superadmin_email
            if flow["purpose"] == "admin":
                if not admin:
                    return False
                await conn.execute("""
                    INSERT INTO mcp_auth.sessions (token_hash, subject, purpose, resource)
                    VALUES (%s, %s, 'admin', %s)
                """, (digest(credential), user["subject"], resource))
            else:
                if not (admin or user["approved"]):
                    invited = await (await conn.execute("""
                        DELETE FROM mcp_auth.email_invites WHERE email = %s RETURNING email
                    """, (identity["email"],))).fetchone()
                    if not invited:
                        return False
                    await conn.execute("""
                        UPDATE mcp_auth.users SET approved = true WHERE subject = %s
                    """, (user["subject"],))
                    await self.write_audit(conn, {
                        "event": "mcp_email_invite_redeemed",
                        "caller_subject": user["subject"], "caller_email": identity["email"],
                    })
                await conn.execute("""
                    INSERT INTO mcp_auth.codes
                        (code_hash, subject, client_id, redirect_uri, challenge, resource)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (digest(credential), user["subject"], flow["client_id"],
                      flow["redirect_uri"], flow["challenge"], resource))
        return True

    async def exchange_code(self, code, client_id, redirect_uri, challenge, resource, token, refresh_token):
        async with self.pool.connection() as conn:
            # Acquire the user lock first so approval changes and token issuance serialize.
            user = await (await conn.execute("""
                SELECT u.* FROM mcp_auth.users u JOIN mcp_auth.codes c USING (subject)
                WHERE c.code_hash = %s FOR UPDATE OF u
            """, (digest(code),))).fetchone()
            if not user or not (user["approved"] or user["email"] == self.superadmin_email):
                return False
            consumed = await (await conn.execute("""
                DELETE FROM mcp_auth.codes
                WHERE code_hash = %s AND client_id = %s AND redirect_uri = %s
                    AND challenge = %s AND resource = %s AND expires_at > now()
                RETURNING subject
            """, (digest(code), client_id, redirect_uri, challenge, resource))).fetchone()
            if not consumed:
                return False
            await self.issue_mcp_tokens(
                conn, uuid4(), user["subject"], client_id, resource, token, refresh_token,
            )
        return True

    @staticmethod
    async def issue_mcp_tokens(conn, family_id, subject, client_id, resource, token, refresh_token):
        await conn.execute("""
            INSERT INTO mcp_auth.sessions (token_hash, subject, purpose, resource, family_id)
            VALUES (%s, %s, 'mcp', %s, %s)
        """, (digest(token), subject, resource, family_id))
        await conn.execute("""
            INSERT INTO mcp_auth.refresh_tokens (token_hash, family_id, subject, client_id, resource)
            VALUES (%s, %s, %s, %s, %s)
        """, (digest(refresh_token), family_id, subject, client_id, resource))

    async def exchange_refresh_token(self, refresh_token, client_id, resource, token, new_refresh_token):
        token_hash = digest(refresh_token)
        async with self.pool.connection() as conn:
            # Acquire the user lock first so approval changes and refresh rotation serialize.
            row = await (await conn.execute("""
                SELECT rt.family_id, rt.subject, rt.client_id, rt.resource, rt.consumed_at,
                       (rt.consumed_at > now() - make_interval(secs => %s)) AS within_grace,
                       (rt.expires_at > now()) AS active,
                       u.email, u.approved
                FROM mcp_auth.refresh_tokens rt
                JOIN mcp_auth.users u USING (subject)
                WHERE rt.token_hash = %s
                FOR UPDATE OF u, rt
            """, (REFRESH_REUSE_GRACE_SECONDS, token_hash))).fetchone()
            if not row or row["client_id"] != client_id or row["resource"] != resource:
                return False
            if row["consumed_at"] is not None and not row["within_grace"]:
                # Reuse of a rotated refresh token past the grace window revokes the token family.
                await conn.execute(
                    "DELETE FROM mcp_auth.refresh_tokens WHERE family_id = %s",
                    (row["family_id"],),
                )
                await conn.execute(
                    "DELETE FROM mcp_auth.sessions WHERE family_id = %s",
                    (row["family_id"],),
                )
                await self.write_audit(conn, {
                    "event": "mcp_refresh_token_reuse_detected",
                    "caller_subject": row["subject"],
                    "caller_email": row["email"],
                    "client_id": client_id,
                })
                return False
            if not row["active"] or not (row["approved"] or row["email"] == self.superadmin_email):
                return False
            if row["consumed_at"] is None:
                await conn.execute(
                    "UPDATE mcp_auth.refresh_tokens SET consumed_at = now() WHERE token_hash = %s",
                    (token_hash,),
                )
            await self.issue_mcp_tokens(
                conn, row["family_id"], row["subject"], client_id, resource, token, new_refresh_token,
            )
            await self.write_audit(conn, {
                "event": "mcp_token_refreshed",
                "caller_subject": row["subject"],
                "caller_email": row["email"],
                "client_id": client_id,
                "token_fingerprint": digest(token)[:12],
            })
        return True

    async def session_user(self, token, purpose, resource):
        return await self.session_user_by_hash(digest(token), purpose, resource)

    async def session_user_by_hash(self, token_hash, purpose, resource):
        return await self.fetch("""
            SELECT u.* FROM mcp_auth.sessions s JOIN mcp_auth.users u USING (subject)
            WHERE s.token_hash = %s AND s.purpose = %s AND s.resource = %s
                AND s.expires_at > now()
                AND (u.email = %s OR (s.purpose = 'mcp' AND u.approved))
        """, (token_hash, purpose, resource, self.superadmin_email))

    async def users(self):
        async with self.pool.connection() as conn:
            return await (await conn.execute("""
                SELECT * FROM mcp_auth.users WHERE email <> %s ORDER BY email, subject
            """, (self.superadmin_email,))).fetchall()

    async def email_invites(self):
        async with self.pool.connection() as conn:
            return await (await conn.execute("""
                SELECT email FROM mcp_auth.email_invites ORDER BY email
            """)).fetchall()

    @staticmethod
    async def revoke_credentials(conn, subject):
        await conn.execute("DELETE FROM mcp_auth.codes WHERE subject = %s", (subject,))
        await conn.execute("DELETE FROM mcp_auth.sessions WHERE subject = %s", (subject,))
        await conn.execute("DELETE FROM mcp_auth.refresh_tokens WHERE subject = %s", (subject,))

    async def lock_admin(self, conn, token, resource):
        actor = await (await conn.execute("""
            SELECT u.subject, u.email FROM mcp_auth.users u
            JOIN mcp_auth.sessions s USING (subject)
            WHERE s.token_hash = %s AND s.purpose = 'admin' AND s.resource = %s
                AND s.expires_at > now() AND u.email = %s
            FOR UPDATE OF u
        """, (digest(token), resource, self.superadmin_email))).fetchone()
        if not actor:
            return None
        # Check again while holding the identity lock. A concurrent logout
        # either removes the session first or waits for this change to commit.
        session = await (await conn.execute("""
            SELECT 1 FROM mcp_auth.sessions
            WHERE token_hash = %s AND expires_at > clock_timestamp() FOR UPDATE
        """, (digest(token),))).fetchone()
        return actor if session else None

    async def set_access(self, token, resource, subject, approved):
        async with self.pool.connection() as conn:
            actor = await self.lock_admin(conn, token, resource)
            if not actor:
                return False
            user = await (await conn.execute("""
                UPDATE mcp_auth.users SET approved = %s WHERE subject = %s AND email <> %s
                RETURNING email
            """, (approved, subject, self.superadmin_email))).fetchone()
            if not user:
                return True
            if not approved:
                await self.revoke_credentials(conn, subject)
            await conn.execute("DELETE FROM mcp_auth.email_invites WHERE email = %s", (user["email"],))
            record = {
                "event": "mcp_access_changed", "caller_subject": actor["subject"],
                "caller_email": actor["email"], "target_subject": subject,
                "target_email": user["email"], "approved": approved,
            }
            await self.write_audit(conn, record)
        return True

    async def set_email_invite(self, token, resource, email, invited):
        async with self.pool.connection() as conn:
            actor = await self.lock_admin(conn, token, resource)
            if not actor:
                return False
            if invited:
                changed = await conn.execute("""
                    INSERT INTO mcp_auth.email_invites (email)
                    SELECT %s WHERE %s <> %s
                        AND NOT EXISTS (SELECT 1 FROM mcp_auth.users WHERE email = %s)
                    ON CONFLICT DO NOTHING
                """, (email, email, self.superadmin_email, email))
            else:
                changed = await conn.execute("DELETE FROM mcp_auth.email_invites WHERE email = %s", (email,))
            if changed.rowcount:
                await self.write_audit(conn, {
                    "event": "mcp_email_invite_changed", "caller_subject": actor["subject"],
                    "caller_email": actor["email"], "target_email": email, "invited": invited,
                })
        return True

    @staticmethod
    async def write_audit(conn, record):
        await conn.execute("INSERT INTO mcp_auth.audit (record) VALUES (%s)", (Jsonb(record),))

    async def audit(self, record):
        async with self.pool.connection() as conn:
            await self.write_audit(conn, record)
