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
| **Zenventory (AGH Fulfillment)** | `agh_fulfillment` | `materialize_frequent_job` | Every 15m (`frequent_15min_schedule`) | Warehouse inventory, orders, shipments. |

## Active YSWS Programs → Warehouse Location

Every approved YSWS submission is also aggregated in
`airtable_unified_ysws_projects_db.approved_projects` (keyed by program in
`airtable_unified_ysws_projects_db.ysws_programs`). Individual program operational
data lives in one of three places:

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
3. **Shared Airtable / Review Platform** (`airtable_raw_all_bases` and/or `ari`):
   - Lightweight or Airtable-native programs without a standalone Postgres app
     (e.g. `8-pin-grant`, `ASIC`, `Bashed`, `CQ`, `Crossroads`, `Dayoneof`,
     `Fellows`, `Fjording it`, `Folktale`, `Genesis Reborn`, `Goship`,
     `Hackcraft Mod Edition`, `Hacklet`, `Hackrail`, `Haggle`, `Molten`,
     `Re-Dream`, `Storyboard V3`, `Survive`, `Torque`, `Twisted`, `Wonders`,
     `flavorless`) land in `airtable_raw_all_bases` and `ari` (for review
     workflows) plus `airtable_unified_ysws_projects_db.approved_projects`.

## Access & Audit Logging

- **`warehouse-mcp` (`warehouse-mcp/server.py`):** Emits a structured JSON
  `mcp_audit` log line to stdout for every tool call (`query`, `get_rows`,
  `grep_rows`, `list_schemas`, `describe_schema`, `list_columns`) containing
  `timestamp`, `tool`, `caller`, `token_fingerprint` (12-char SHA-256 prefix,
  never the token), `outcome`, `duration_ms`, `sql` (truncated at 4,000 chars),
  target `schema`/`table`/`query_id`, `row_count`, and `error` on failure.
- Logs are collected from container stdout by the host log shipper (`alloy` /
  `coolify-log-drain`).
