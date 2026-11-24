ALTER TABLE mcp_auth.refresh_tokens
    ALTER COLUMN expires_at SET DEFAULT now() + interval '90 days';
