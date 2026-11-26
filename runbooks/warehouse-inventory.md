# Warehouse Data Inventory

Authoritative map of shared Hack Club infrastructure and active YSWS program
data in the warehouse Postgres database. Keep this document free of credentials,
internal hostnames/IPs, and personal data.

## Shared Infrastructure Systems

| System | Warehouse Schema(s) | Dagster Asset(s) | Sync Cadence | Notes |
| --- | --- | --- | --- | --- |
| **Attend** | `attend` | `attend_warehouse_mirror` | Every 6h (`materialize_all_assets_job`) | Event check-in / attendance app; dedicated `hc_warehouse_reader`. |
| **Auth (Hack Club Auth)** | `auth` | `auth_warehouse_mirror` | Every 6h (`materialize_all_assets_job`) | Identity & verification activities; incremental mirror. |
| **Theseus** | `theseus` | `theseus_warehouse_mirror` | Every 6h (`materialize_all_assets_job`) | Mail / fulfillment (`letters`, etc.) synced from Orchard Postgres. |
| **Fallen** | `fallen` | `fallen_warehouse_mirror` | Every 6h (`materialize_all_assets_job`) | Dedicated `hc_warehouse_reader`. |
| **Ari (Another Review Interface)** | `ari` | `ari_warehouse_mirror` | Every 6h (`materialize_all_assets_job`) | Shared YSWS review platform (31 tables). |
| **HCB** | `hcb`, `public_hcb_analytics`, `public_hcb_ysws_true_spend_analytics` | `hcb_warehouse_mirror`, `my_dbt_assets`, `ysws_true_spend_site` | Every 6h (`materialize_all_assets_job`) | Fiscal sponsorship ledger and true-spend marts. |
| **Hackatime** | `hackatime`, `public_hackatime_analytics`, `public_summer_2026_analytics` | `hackatime_warehouse_mirror`, `materialize_hackatime_dau_job` | Every 2h (`hackatime_dau_hourly_schedule`) | Coding heartbeats and DAU analytics. |
| **Loops** | `loops`, `public_loops_analytics` | `loops_raw_audience` → `loops_audience`, `loops_campaigns_to_warehouse`, `loops_mailing_lists_to_warehouse` | Every 6h (`materialize_all_assets_job`) | Email audience, mailing lists, and campaign metrics. |
| **Unified YSWS Projects DB** | `airtable_unified_ysws_projects_db` | `materialize_unified_ysws_job` | Every 15m (`unified_ysws_15min_schedule`) | Canonical Airtable registry of YSWS programs and approved projects. |
| **All Airtable Bases (raw)** | `airtable_raw_all_bases` | `airtable_raw_all_bases_sync` | Every 6h (`materialize_all_assets_job`) | Catch-all raw mirror of accessible Airtable bases. |
| **Fillout** | `fillout` | `fillout_sync` | Every 6h (`materialize_all_assets_job`) | Form submissions across Hack Club forms. |
| **Zenventory (AGH Fulfillment)** | `agh_fulfillment_zenventory` | `materialize_frequent_job` | Every 15m (`frequent_15min_schedule`) | Warehouse inventory, orders, shipments. |

## Mirror Freshness Visibility

Agents and operators can check per-schema and per-asset sync freshness without querying the Dagster instance DB:

- **Per-schema worst-of SQL view:** `warehouse_metadata.mirror_freshness` (one row per warehouse schema, including dbt marts, Fivetran schemas, and explicit `untracked` rows with reasons; readable by `hc_warehouse_agent` and `hc_warehouse_mcp` via `pg_read_all_data`).
- **Per-asset SQL view:** `warehouse_metadata.asset_freshness` (one row per Dagster/dbt/Fivetran asset; exposes individual stale tables inside multi-asset schemas such as `airtable_campfire` and `loops`).
- **No separate tool:** read both views with plain SQL (the `warehouse-mcp` `query` tool, `bin/readonly-psql`). The `warehouse-mcp` server instructions and its `query` and `list_schemas` text point agents to `warehouse_metadata.mirror_freshness`. Each view and column has a Postgres comment that explains it. `describe_schema(schema_name='warehouse_metadata')` in `warehouse-mcp` lists both views with those comments, or read them with SQL: `SELECT obj_description('warehouse_metadata.mirror_freshness'::regclass, 'pg_class')`.
- **Backing Dagster asset:** `warehouse_mirror_freshness` (runs every 15 minutes in `materialize_frequent_job`; cadences are derived directly from the job selections in `orpheus_engine/schedules.py`).

Because `age_hours`, `is_stale`, and `status` (`fresh` | `stale` | `never_synced` | `untracked`) are computed dynamically against `NOW()` in the views, mirrors still flip to stale if Dagster stops running. Check `recorded_at` to verify that the freshness asset itself is current.

> **Limitation (incremental mirrors & hard deletes):** Freshness reflects last successful sync time only. Incremental Sling mirrors (for example `stardance.users`, `auth.oauth_access_tokens`, and several `flavortown.*` tables) do not propagate hard deletes from the source database, so a freshly synced incremental table can still retain rows deleted at the source.

## Active YSWS Programs → Warehouse Location

Every approved YSWS submission is also aggregated in
`airtable_unified_ysws_projects_db.approved_projects` (keyed by program in
`airtable_unified_ysws_projects_db.ysws_programs`). Individual program operational
data lives in one of these places:

1. **Dedicated Sling Postgres mirror** (`orpheus_engine/defs/sling/programs/<name>.py`, schema `<name>`):
   - Active programs with dedicated schemas: `club_shop`, `contd`, `coral`,
     `crescent`, `cubed`, `embassy`, `expedition`, `fabricate`, `fallen`,
     `forge`, `grandma`, `half_life`, `jame_gam`, `mipmap`, `phantom`, `pixl`,
     `scraps`, `stardance` (+ `stardance_ahoy`), `taiyaki`, `terra`,
     `thirdspace`, `three_am` (3am), `trmnt`.
   - Programs using `hc_warehouse_reader` are checked daily for column grant
     drift by `warehouse_reader_grant_drift_job` (see `runbooks/prod-health-monitoring.md` §9).
2. **Dedicated Airtable DLT mirror** (schema `airtable_<name>`):
   - `airtable_mipmap`, `airtable_shredder`, `airtable_snowglobe`,
     `airtable_sleepover`, `airtable_daydream`, `airtable_daydream_ops`,
     `airtable_campfire`, `airtable_campfire_flagship`.
3. **Shared Airtable (`airtable_raw_all_bases`) and/or Review Platform (`ari`)**:
   - Active Airtable-native programs verified in `airtable_raw_all_bases` (plus
     `airtable_unified_ysws_projects_db.approved_projects`): `8-pin-grant`,
     `ASIC`, `BakeBuild`, `Bashed`, `Boba Drops`, `CQ`, `Crossroads`,
     `Dayoneof`, `Fellows`, `Fjording it`, `Folktale`, `FuseRing`,
     `Genesis Reborn`, `Goship`, `Hackcraft Mod Edition`, `Hacklet`,
     `Hackrail` (also in `ari`), `Molten`, `Re-Dream`, `Sprig`,
     `Storyboard V3`, `Survive` (also in `ari`), `Swirl`, `TTY`, `Tickets`,
     `Toppings`, `Torque`, `Twisted` (also in `ari`), `Vibes`, `Wonders`,
     `flavorless`.
   - Programs with dedicated mirrors that also use `ari` for review
     workflows: `3am`, `Embassy`, `Grandma`, `Half Life`, `Phantom`,
     `Snowglobe` (`airtable_snowglobe`), `Terra`, `thirdspace`.
4. **Active Programs Not Yet Mirrored (Unified YSWS Registry Only)**:
   - `Haggle`: has a Hack Club Orchard Postgres database that is not yet
     mirrored (public endpoint off); currently tracked in
     `airtable_unified_ysws_projects_db` (`ysws_programs` / `approved_projects`).
   - `Harvest`: an itch.io jam with no Hack Club operational database
     (submissions live on itch.io; approved projects are tracked in
     `airtable_unified_ysws_projects_db.approved_projects`).

## Warehouse MCP access and audit logs

The MCP server issues its own bearer tokens after users verify their identity with
Hack Club Auth. The superadmin can pre-approve an email in `/admin` using
**Invite by email**. The invitation is redeemed on the first MCP sign-in with
that verified email and is then replaced by an approved account. The form
does not send email. Pending invitations can be removed in `/admin`.
Users can also sign in first and ask the superadmin to approve their account.
Revoking access invalidates that account's outstanding authorization codes,
sessions, and refresh tokens. Tool calls recheck the session before
returning results, including when revocation occurs during a query. Previously
issued shared-secret tokens require users to reconnect. MCP access tokens
expire after eight hours and can be renewed with rotating refresh tokens
(valid for 90 days of inactivity). A 90-second grace window after rotation
allows retries after lost responses or concurrent client refreshes without
forcing re-authentication; reusing a rotated refresh token after the grace
window revokes that token family and its sessions.

The `warehouse-mcp` app needs the following environment variables.

| Variable | Configuration |
| --- | --- |
| `MCP_PUBLIC_URL` | Public origin, such as `https://warehouse-mcp.example.org`. Defaults to `http://localhost:3000` for local use. |
| `HACKCLUB_CLIENT_ID`, `HACKCLUB_CLIENT_SECRET` | Credentials for the MCP app in Hack Club Auth. Register the exact callback at `<MCP_PUBLIC_URL>/oauth/callback`. |
| `MCP_SUPERADMIN_EMAIL` | Verified Hack Club Auth email for the superadmin. |
| `MCP_AUTH_DATABASE_URL` | DSN for the MCP auth and audit Postgres database. Give its app role permission to create and own the `mcp_auth` schema and its tables. |
| `WAREHOUSE_COOLIFY_URL` | Existing read-only warehouse DSN for the MCP service. |

Use a dedicated auth database and app role so the read-only warehouse connection
cannot query session or audit tables. On a new Postgres server, connect as the
Postgres administrator with `psql` and provision the role and database.
The password prompt keeps the password out of the command history.

```sql
CREATE ROLE warehouse_mcp_auth LOGIN;
\password warehouse_mcp_auth
CREATE DATABASE warehouse_mcp_auth OWNER warehouse_mcp_auth;
```

Set `MCP_AUTH_DATABASE_URL` for that role and database. Apply the versioned
migrations from the repo root before swapping traffic to the new release.

```sh
uv run --project warehouse-mcp python warehouse-mcp/migrate.py
```

In the Docker image, the equivalent command is `uv run python migrate.py`.
The MCP also applies pending migrations on startup. Successful migrations are
recorded in `mcp_auth.schema_migrations`, so repeated runs skip them and existing
sessions and audit records are preserved. Startup refuses to serve if the
warehouse connection sees the auth schema in its database.

Apply ingress rate limits to the public OAuth endpoints, especially
`/register`, which stores client registrations.
When deploying behind a proxy, preserve the public Host header and omit OAuth
query strings from proxy access logs. The Docker app listens on port 8000 and the
local Python entry point defaults to port 3000. For local development, set the
variables in the ignored `warehouse-mcp/.env` file and run
`uv run --project warehouse-mcp python warehouse-mcp/server.py` from the repo root.

Tool calls get a start record in `mcp_auth.audit` before execution. They share
a request ID with the completion record, which is also emitted as a structured
`mcp_audit` line to stdout. Records include the verified caller subject and
email, tool, outcome, duration, and relevant inputs such as SQL (up to 4,000
characters), schema, table, or cached query ID. Records include a short token
fingerprint, and stored authorization codes and session tokens are hashed.
Access changes are also recorded in Postgres. Tool results are returned only
after audit persistence succeeds. Treat the audit database and log exports as
sensitive because they contain caller emails and query text.

To view recent entries using the auth database connection, run:

```sql
SELECT recorded_at, record
FROM mcp_auth.audit
ORDER BY recorded_at DESC
LIMIT 50;
```

For direct Postgres logins to the warehouse and Dagster instance databases (outside
MCP), see `runbooks/prod-health-monitoring.md` §12.
