"""The recorded inventory of credential columns in the warehouse.

Every logged-in warehouse-mcp user reads the warehouse as hc_warehouse_mcp, which
has pg_read_all_data (runbooks/prod-health-monitoring.md section 10). So does the
agents' login. A credential copied into the warehouse is therefore readable by
all of them. Each column below gets one treatment:

- EXCLUDE_AT_SOURCE: a live sync copies the table. Its config leaves the column
  out (a Sling `-col`, a positive select list without it, a disabled stream, or
  the dlt `drop_columns` list), and the purge job drops any copy already in the
  warehouse. A full refresh cannot bring it back, because the sync no longer reads
  it. orpheus_engine_tests/test_secret_columns.py checks every config.
- DROP_FROZEN_COPY: nothing syncs the table any more (an ended program's mirror,
  a paused connector's snapshot). The purge job drops the column.
- HIDE_ROWS: the sync itself needs the value (the QuickBooks refresh token). The
  table gets row-level security with no policy. Its owner, the sync login,
  still reads and writes it. pg_read_all_data does not bypass row-level security,
  so every other login, hc_warehouse_mcp included, sees no rows.

Columns that only look like secrets (hashes, blind indexes, ciphertext, token
counts, analytics visit ids) are listed by class in
runbooks/secret-columns.md, with the open questions.

Keep this file free of values, counts and people's names: the repo is public.
"""

from __future__ import annotations

from dataclasses import dataclass

EXCLUDE_AT_SOURCE = "exclude_at_source"
DROP_FROZEN_COPY = "drop_frozen_copy"
HIDE_ROWS = "hide_rows"
TREATMENTS = (EXCLUDE_AT_SOURCE, DROP_FROZEN_COPY, HIDE_ROWS)

# How a live sync excludes the column.
SLING = "sling"
DLT_AIRTABLE = "dlt_airtable"


@dataclass(frozen=True)
class SecretColumn:
    schema: str
    table: str
    column: str
    treatment: str
    kind: str
    sync: str | None = None  # for EXCLUDE_AT_SOURCE: SLING or DLT_AIRTABLE

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}.{self.column}"


def _sling(schema, table, column, kind):
    return SecretColumn(schema, table, column, EXCLUDE_AT_SOURCE, kind, SLING)


def _dlt(schema, table, column, kind):
    return SecretColumn(schema, table, column, EXCLUDE_AT_SOURCE, kind, DLT_AIRTABLE)


def _frozen(schema, table, column, kind):
    return SecretColumn(schema, table, column, DROP_FROZEN_COPY, kind)


_ARCADE = "airtable_arcade__summer_24__app4kcwulfb02bv8q"
_HIGH_SEAS = "airtable_high_seas___ysws_apptenfycuiyfgcr6"
_JUICE = "airtable_juice_appfnfjafslg23jlt"
_SCRAPYARD = "airtable_hack_club_scrapyard_appigkif7gbvisalg"

SECRET_COLUMNS: tuple[SecretColumn, ...] = (
    # --- Hackatime (hackatime_warehouse_mirror) ---
    _sling("hackatime", "admin_api_keys", "token", "admin API key"),
    _sling("hackatime", "api_keys", "token", "API key"),
    _sling("hackatime", "email_verification_requests", "token", "email verification token"),
    _sling("hackatime", "oauth_access_grants", "token", "OAuth authorization code"),
    _sling("hackatime", "oauth_access_tokens", "token", "OAuth access token"),
    _sling("hackatime", "oauth_access_tokens", "refresh_token", "OAuth refresh token"),
    _sling("hackatime", "oauth_access_tokens", "previous_refresh_token", "OAuth refresh token"),
    _sling("hackatime", "oauth_applications", "secret", "OAuth client secret"),
    _sling("hackatime", "sign_in_tokens", "token", "sign-in link token"),
    _sling("hackatime", "users", "github_access_token", "GitHub OAuth token"),
    _sling("hackatime", "users", "hca_access_token", "Hack Club Auth token"),
    _sling("hackatime", "users", "slack_access_token", "Slack OAuth token"),
    # --- HCER public GitHub data ---
    _sling("hcer_public_github_data", "access_tokens", "access_token", "GitHub OAuth token"),
    # --- HCB ---
    _sling("hcb", "donations", "stripe_client_secret", "Stripe payment client secret"),
    # --- Program mirrors ---
    _sling("beest", "lookout_sessions", "token", "Lookout session token"),
    _sling("blueprint", "one_time_passwords", "secret", "one-time password"),
    _sling("blueprint", "users", "hcb_access_token", "HCB OAuth token"),
    _sling("blueprint", "users", "hcb_refresh_token", "HCB OAuth token"),
    _sling("blueprint", "users", "identity_vault_access_token", "identity vault token"),
    _sling("fabricate", "users", "session_token", "login session token"),
    _sling("fabricate", "users", "slack_user_token", "Slack OAuth token"),
    _sling("flavortown", "report_review_tokens", "token", "review link token"),
    _sling("flavortown", "users", "api_key", "API key"),
    _sling("flavortown", "users", "magic_link_token", "sign-in link token"),
    _sling("flavortown", "users", "session_token", "login session token"),
    _sling("hackatime_legacy", "users", "api_key", "API key"),
    _sling("hackatime_legacy", "users", "password", "password hash"),
    _sling("hackatime_legacy", "users", "reset_token", "password reset token"),
    _sling("hackatime_legacy", "users", "wakatime_api_key", "WakaTime API key"),
    _sling("haggle", "rsvp_rsvp", "hackatime_token", "Hackatime API key"),
    _sling("journey", "users", "identity_vault_access_token", "identity vault token"),
    _sling("phantom", "oauth_states", "code_verifier", "OAuth PKCE verifier"),
    _sling("phantom", "oauth_states", "state", "OAuth state"),
    _sling("save_point", "users", "hca_access_token", "Hack Club Auth token"),
    _sling("save_point", "users", "hca_refresh_token", "Hack Club Auth token"),
    _sling("shipwrecked_the_bay", "Account", "access_token", "OAuth access token"),
    _sling("shipwrecked_the_bay", "Account", "id_token", "OIDC ID token"),
    _sling("shipwrecked_the_bay", "Account", "refresh_token", "OAuth refresh token"),
    _sling("shipwrecked_the_bay", "Session", "sessionToken", "login session token"),
    _sling("shipwrecked_the_bay", "User", "identityToken", "identity vault token"),
    _sling("shipwrecked_the_bay", "VerificationToken", "token", "sign-in link token"),
    _sling("stardance", "lookout_sessions", "token", "Lookout session token"),
    _sling("stasis", "session", "token", "login session token"),
    _sling("stasis", "verification", "value", "verification code"),
    _sling("summer_of_making_2025", "magic_links", "token", "sign-in link token"),
    _sling("summer_of_making_2025", "users", "identity_vault_access_token", "identity vault token"),
    _sling("summer_of_making_2025", "users", "wrapped_share_token", "share link token"),
    # --- Airtable bases synced by dlt ---
    _dlt("airtable_campfire", "participants", "checkin_token", "event check-in token"),
    _dlt("airtable_milkyway", "users__otp", "value", "one-time password"),
    _dlt("airtable_neighborhood", "neighbors", "token", "login token"),
    # --- Ended program, mirror disabled ---
    _frozen("fallout", "collapse_timelapses", "session_token", "timelapse session token"),
    _frozen("fallout", "hcb_connections", "access_token", "HCB OAuth token"),
    _frozen("fallout", "hcb_connections", "refresh_token", "HCB OAuth token"),
    _frozen("fallout", "hcb_donation_requests", "token", "donation link token"),
    _frozen("fallout", "lookout_timelapses", "session_token", "timelapse session token"),
    _frozen("fallout", "pending_collaboration_invites", "token", "invite token"),
    _frozen("fallout", "project_time_audits", "token", "audit link token"),
    _frozen("fallout", "soup_campaign_recipients", "unsubscribe_token", "unsubscribe token"),
    _frozen("fallout", "soup_campaigns", "unsubscribe_token", "unsubscribe token"),
    _frozen("fallout", "users", "device_token", "push notification token"),
    _frozen("fallout", "users", "hca_token", "Hack Club Auth token"),
    _frozen("fallout", "users", "lapse_token", "Lapse token"),
    # --- Paused Fivetran snapshots of ended programs' Airtable bases ---
    _frozen(_ARCADE, "projects", "user_auth_token", "login token"),
    _frozen(_ARCADE, "showcase", "creator_auth_token", "login token"),
    _frozen(_ARCADE, "users", "auth_token", "login token"),
    _frozen(_ARCADE, "users", "login_token", "login token"),
    _frozen(_ARCADE, "vote", "voter_key", "voting key"),
    _frozen(_SCRAPYARD, "events", "api_password", "API password"),
    _frozen(_HIGH_SEAS, "people", "magic_auth_token", "sign-in link token"),
    _frozen(_HIGH_SEAS, "people", "shop_otp", "one-time password"),
    _frozen(_HIGH_SEAS, "shop_addresses", "person_shop_otp", "one-time password"),
    _frozen(_JUICE, "signups", "token", "login token"),
    _frozen(_JUICE, "ysws_project_submission", "token", "login token"),
    # --- Needed by its own sync ---
    SecretColumn("quickbooks", "_qbo_oauth", "refresh_token", HIDE_ROWS, "QuickBooks OAuth refresh token"),
)

DROPPED_COLUMNS: tuple[SecretColumn, ...] = tuple(
    c for c in SECRET_COLUMNS if c.treatment in (EXCLUDE_AT_SOURCE, DROP_FROZEN_COPY)
)
HIDDEN_TABLES: tuple[tuple[str, str], ...] = tuple(
    dict.fromkeys((c.schema, c.table) for c in SECRET_COLUMNS if c.treatment == HIDE_ROWS)
)


def excluded_columns(schema: str, table: str) -> list[str]:
    """Inventory columns a live sync must leave out of schema.table."""
    return [
        c.column
        for c in SECRET_COLUMNS
        if c.treatment == EXCLUDE_AT_SOURCE and c.schema == schema and c.table == table
    ]
