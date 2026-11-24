CREATE TABLE mcp_auth.refresh_tokens (
    token_hash text PRIMARY KEY,
    family_id uuid NOT NULL,
    subject text NOT NULL REFERENCES mcp_auth.users,
    client_id text NOT NULL REFERENCES mcp_auth.clients,
    resource text NOT NULL,
    consumed_at timestamptz,
    expires_at timestamptz NOT NULL DEFAULT now() + interval '30 days'
);
CREATE INDEX mcp_refresh_tokens_subject ON mcp_auth.refresh_tokens (subject);
CREATE INDEX mcp_refresh_tokens_family ON mcp_auth.refresh_tokens (family_id);
ALTER TABLE mcp_auth.sessions ADD COLUMN family_id uuid;
CREATE INDEX mcp_sessions_family ON mcp_auth.sessions (family_id) WHERE family_id IS NOT NULL;
