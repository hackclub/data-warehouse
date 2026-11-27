# Secret columns in the warehouse

Every approved warehouse-mcp user, and every agent, reads the warehouse through a
login that has `pg_read_all_data` (`runbooks/prod-health-monitoring.md` section 10).
That grant covers every table, so a credential copied into the warehouse is
readable by all of them. Grants cannot fix that one column at a time: a column
`REVOKE` does not override `pg_read_all_data`. So credentials are kept out of the
copy instead.

The recorded inventory is code:
`orpheus_engine/defs/secret_columns/inventory.py`. This page explains it and
lists it. `orpheus_engine_tests/test_secret_columns.py` fails if a live sync can
copy an inventory column, and if this list and the code differ.

## Treatments

| Treatment | When | How |
| --- | --- | --- |
| Excluded at source, copy dropped | A live sync copies the table | The sync's config leaves the column out: a Sling `-col` or a positive `select` list without it, or the dlt `AIRTABLE_SECRET_DROP_COLUMNS` list for Airtable bases. The purge job drops the copy already in the warehouse. A full refresh cannot bring it back, because the sync no longer reads it. |
| Frozen copy dropped | Nothing syncs the table any more: an ended program's disabled mirror, or a paused connector's snapshot | The purge job drops the column. |
| Hidden by row security | The sync itself needs the value | Row-level security with no policy. The table's owner, the sync login, still reads and writes it. `pg_read_all_data` does not bypass row-level security, so every other login sees no rows. Only `quickbooks._qbo_oauth` (the QuickBooks refresh token) is treated this way. The QuickBooks sync turns it on when it creates the table. |

## The purge job

`warehouse_secret_column_purge_job` (schedule `warehouse_secret_column_purge_schedule`,
hourly at :40 New York time, on by default) runs as the sync login, which owns the
synced tables:

- For each dropped inventory column that is still in the warehouse, it runs
  `ALTER TABLE ... DROP COLUMN`, with a 10 s lock timeout. Indexes on the column
  go with it.
- It never uses `CASCADE`. If a view depends on the column, it leaves the column
  in place and fails, naming the view. Change the view first.
- It turns on row-level security on each hidden table if it is off. It fails if
  such a table has `FORCE ROW LEVEL SECURITY` (that would lock out its own sync)
  or any policy (a policy can show the rows to another login).
- A lock timeout fails the run. The next run tries again.

After the first clean run, each run is a few catalog reads.

Dropping a column changes every row's `_row_hash`, so the next DuckLake sync
rewrites those tables once. DuckLake drops the column on its own, but its older
snapshots keep the old values until they expire.

## Check it

As the login to check (for the contract, the query tool's `hc_warehouse_mcp`; its URL
is in its own Vaultwarden item, in a mode 0600 file):

```sh
PYTHONPATH=. .venv/bin/python scripts/verify_secret_columns.py --env-file <file>
```

It prints each inventory column as `absent` or with the number of non-null values that
login can read, never a value, and exits 1 if any is readable.

## Adding a column

1. Add it to `SECRET_COLUMNS` in `inventory.py` with its treatment.
2. For a live Sling mirror, exclude it in the stream's config (name the table
   explicitly; a `public.*` wildcard copies every column). For a dlt Airtable base,
   add the renamed field to `AIRTABLE_SECRET_DROP_COLUMNS`.
3. Add the row to the table below and run the tests.
4. After the deploy, the next purge run drops the copy.

A reader login with column grants (`hc_warehouse_reader`, section 9 of the
health runbook) still has a grant on a newly excluded column. The daily drift
check reports it as "excluded column is readable by the reader". Refresh that
program's grants with `scripts/provision_warehouse_reader.py --refresh-grants-sql-file`.

## Reviewed and kept

The scan that built the inventory matched column names such as `token`,
`secret`, `password`, `key`, `session`, `hash` and `otp`. These classes were
reviewed and are not live credentials:

- Hashes and blind indexes (`*_hash`, `*_digest`, `*_bidx`, `token_hash`,
  `hashed_token`): one-way values. The contract allows them.
- Ciphertext (`*_ciphertext`, `encrypted*`, `*Enc`, `*_encrypted`): encrypted at
  the source, and the key is not in the warehouse.
- Token and API key metadata: `token_preview`, `*_last4`, `*_expires_at`,
  `token_type`, `scopes`, `token_version`.
- AI usage counts (`*_tokens`, `prompt_tokens`) and game currencies (`cookies`).
- Analytics and work-session ids: `ahoy_visits.visit_token` / `visitor_token`,
  `workSessionId`, and fingerprint-style `session_id` columns.
- Sync, idempotency and lookup keys (`*_key`, `key`).
- HCB grant `hashid` columns: public URL ids.
- Sling's `_row_hash` / `_row_hash_at` change-tracking columns.

Columns still under review are tracked in the private ops notes.

## Inventory

| Column | What it holds | Treatment |
| --- | --- | --- |
| `hackatime.admin_api_keys.token` | admin API key | excluded at source, copy dropped (Sling) |
| `hackatime.api_keys.token` | API key | excluded at source, copy dropped (Sling) |
| `hackatime.email_verification_requests.token` | email verification token | excluded at source, copy dropped (Sling) |
| `hackatime.oauth_access_grants.token` | OAuth authorization code | excluded at source, copy dropped (Sling) |
| `hackatime.oauth_access_tokens.token` | OAuth access token | excluded at source, copy dropped (Sling) |
| `hackatime.oauth_access_tokens.refresh_token` | OAuth refresh token | excluded at source, copy dropped (Sling) |
| `hackatime.oauth_access_tokens.previous_refresh_token` | OAuth refresh token | excluded at source, copy dropped (Sling) |
| `hackatime.oauth_applications.secret` | OAuth client secret | excluded at source, copy dropped (Sling) |
| `hackatime.sign_in_tokens.token` | sign-in link token | excluded at source, copy dropped (Sling) |
| `hackatime.users.github_access_token` | GitHub OAuth token | excluded at source, copy dropped (Sling) |
| `hackatime.users.hca_access_token` | Hack Club Auth token | excluded at source, copy dropped (Sling) |
| `hackatime.users.slack_access_token` | Slack OAuth token | excluded at source, copy dropped (Sling) |
| `hcer_public_github_data.access_tokens.access_token` | GitHub OAuth token | excluded at source, copy dropped (Sling) |
| `hcb.donations.stripe_client_secret` | Stripe payment client secret | excluded at source, copy dropped (Sling) |
| `beest.lookout_sessions.token` | Lookout session token | excluded at source, copy dropped (Sling) |
| `blueprint.one_time_passwords.secret` | one-time password | excluded at source, copy dropped (Sling) |
| `blueprint.users.hcb_access_token` | HCB OAuth token | excluded at source, copy dropped (Sling) |
| `blueprint.users.hcb_refresh_token` | HCB OAuth token | excluded at source, copy dropped (Sling) |
| `blueprint.users.identity_vault_access_token` | identity vault token | excluded at source, copy dropped (Sling) |
| `fabricate.users.session_token` | login session token | excluded at source, copy dropped (Sling) |
| `fabricate.users.slack_user_token` | Slack OAuth token | excluded at source, copy dropped (Sling) |
| `flavortown.report_review_tokens.token` | review link token | excluded at source, copy dropped (Sling) |
| `flavortown.users.api_key` | API key | excluded at source, copy dropped (Sling) |
| `flavortown.users.magic_link_token` | sign-in link token | excluded at source, copy dropped (Sling) |
| `flavortown.users.session_token` | login session token | excluded at source, copy dropped (Sling) |
| `hackatime_legacy.users.api_key` | API key | excluded at source, copy dropped (Sling) |
| `hackatime_legacy.users.password` | password hash | excluded at source, copy dropped (Sling) |
| `hackatime_legacy.users.reset_token` | password reset token | excluded at source, copy dropped (Sling) |
| `hackatime_legacy.users.wakatime_api_key` | WakaTime API key | excluded at source, copy dropped (Sling) |
| `haggle.rsvp_rsvp.hackatime_token` | Hackatime API key | excluded at source, copy dropped (Sling) |
| `journey.users.identity_vault_access_token` | identity vault token | excluded at source, copy dropped (Sling) |
| `phantom.oauth_states.code_verifier` | OAuth PKCE verifier | excluded at source, copy dropped (Sling) |
| `phantom.oauth_states.state` | OAuth state | excluded at source, copy dropped (Sling) |
| `save_point.users.hca_access_token` | Hack Club Auth token | excluded at source, copy dropped (Sling) |
| `save_point.users.hca_refresh_token` | Hack Club Auth token | excluded at source, copy dropped (Sling) |
| `shipwrecked_the_bay.Account.access_token` | OAuth access token | excluded at source, copy dropped (Sling) |
| `shipwrecked_the_bay.Account.id_token` | OIDC ID token | excluded at source, copy dropped (Sling) |
| `shipwrecked_the_bay.Account.refresh_token` | OAuth refresh token | excluded at source, copy dropped (Sling) |
| `shipwrecked_the_bay.Session.sessionToken` | login session token | excluded at source, copy dropped (Sling) |
| `shipwrecked_the_bay.User.identityToken` | identity vault token | excluded at source, copy dropped (Sling) |
| `shipwrecked_the_bay.VerificationToken.token` | sign-in link token | excluded at source, copy dropped (Sling) |
| `stardance.lookout_sessions.token` | Lookout session token | excluded at source, copy dropped (Sling) |
| `stasis.session.token` | login session token | excluded at source, copy dropped (Sling) |
| `stasis.verification.value` | verification code | excluded at source, copy dropped (Sling) |
| `summer_of_making_2025.magic_links.token` | sign-in link token | excluded at source, copy dropped (Sling) |
| `summer_of_making_2025.users.identity_vault_access_token` | identity vault token | excluded at source, copy dropped (Sling) |
| `summer_of_making_2025.users.wrapped_share_token` | share link token | excluded at source, copy dropped (Sling) |
| `airtable_campfire.participants.checkin_token` | event check-in token | excluded at source, copy dropped (dlt) |
| `airtable_milkyway.users__otp.value` | one-time password | excluded at source, copy dropped (dlt) |
| `airtable_neighborhood.neighbors.token` | login token | excluded at source, copy dropped (dlt) |
| `fallout.collapse_timelapses.session_token` | timelapse session token | frozen copy dropped |
| `fallout.hcb_connections.access_token` | HCB OAuth token | frozen copy dropped |
| `fallout.hcb_connections.refresh_token` | HCB OAuth token | frozen copy dropped |
| `fallout.hcb_donation_requests.token` | donation link token | frozen copy dropped |
| `fallout.lookout_timelapses.session_token` | timelapse session token | frozen copy dropped |
| `fallout.pending_collaboration_invites.token` | invite token | frozen copy dropped |
| `fallout.project_time_audits.token` | audit link token | frozen copy dropped |
| `fallout.soup_campaign_recipients.unsubscribe_token` | unsubscribe token | frozen copy dropped |
| `fallout.soup_campaigns.unsubscribe_token` | unsubscribe token | frozen copy dropped |
| `fallout.users.device_token` | push notification token | frozen copy dropped |
| `fallout.users.hca_token` | Hack Club Auth token | frozen copy dropped |
| `fallout.users.lapse_token` | Lapse token | frozen copy dropped |
| `airtable_arcade__summer_24__app4kcwulfb02bv8q.projects.user_auth_token` | login token | frozen copy dropped |
| `airtable_arcade__summer_24__app4kcwulfb02bv8q.showcase.creator_auth_token` | login token | frozen copy dropped |
| `airtable_arcade__summer_24__app4kcwulfb02bv8q.users.auth_token` | login token | frozen copy dropped |
| `airtable_arcade__summer_24__app4kcwulfb02bv8q.users.login_token` | login token | frozen copy dropped |
| `airtable_arcade__summer_24__app4kcwulfb02bv8q.vote.voter_key` | voting key | frozen copy dropped |
| `airtable_hack_club_scrapyard_appigkif7gbvisalg.events.api_password` | API password | frozen copy dropped |
| `airtable_high_seas___ysws_apptenfycuiyfgcr6.people.magic_auth_token` | sign-in link token | frozen copy dropped |
| `airtable_high_seas___ysws_apptenfycuiyfgcr6.people.shop_otp` | one-time password | frozen copy dropped |
| `airtable_high_seas___ysws_apptenfycuiyfgcr6.shop_addresses.person_shop_otp` | one-time password | frozen copy dropped |
| `airtable_juice_appfnfjafslg23jlt.signups.token` | login token | frozen copy dropped |
| `airtable_juice_appfnfjafslg23jlt.ysws_project_submission.token` | login token | frozen copy dropped |
| `quickbooks._qbo_oauth.refresh_token` | QuickBooks OAuth refresh token | hidden by row security |
