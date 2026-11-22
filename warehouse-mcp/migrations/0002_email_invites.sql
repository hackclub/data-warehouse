CREATE TABLE mcp_auth.email_invites (
    email text PRIMARY KEY
);
CREATE INDEX mcp_users_email ON mcp_auth.users (email);
