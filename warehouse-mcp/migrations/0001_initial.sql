-- IF NOT EXISTS also adopts databases initialized by the original MCP startup.
CREATE SCHEMA IF NOT EXISTS mcp_auth;
CREATE TABLE IF NOT EXISTS mcp_auth.users (
    subject text PRIMARY KEY,
    email text NOT NULL,
    first_name text NOT NULL,
    last_name text NOT NULL,
    approved boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS mcp_auth.clients (
    id text PRIMARY KEY,
    redirect_uris text[] NOT NULL
);
CREATE TABLE IF NOT EXISTS mcp_auth.flows (
    state_hash text PRIMARY KEY,
    browser_hash text NOT NULL,
    details jsonb NOT NULL,
    expires_at timestamptz NOT NULL DEFAULT now() + interval '10 minutes'
);
CREATE TABLE IF NOT EXISTS mcp_auth.codes (
    code_hash text PRIMARY KEY,
    subject text NOT NULL REFERENCES mcp_auth.users,
    client_id text NOT NULL REFERENCES mcp_auth.clients,
    redirect_uri text NOT NULL,
    challenge text NOT NULL,
    resource text NOT NULL,
    expires_at timestamptz NOT NULL DEFAULT now() + interval '5 minutes'
);
CREATE TABLE IF NOT EXISTS mcp_auth.sessions (
    token_hash text PRIMARY KEY,
    subject text NOT NULL REFERENCES mcp_auth.users,
    purpose text NOT NULL CHECK (purpose IN ('admin', 'mcp')),
    resource text NOT NULL,
    expires_at timestamptz NOT NULL DEFAULT now() + interval '8 hours'
);
CREATE TABLE IF NOT EXISTS mcp_auth.audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    record jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS mcp_audit_recorded_at ON mcp_auth.audit (recorded_at);
CREATE INDEX IF NOT EXISTS mcp_codes_subject ON mcp_auth.codes (subject);
CREATE INDEX IF NOT EXISTS mcp_sessions_subject ON mcp_auth.sessions (subject);
