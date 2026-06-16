# Runbook: YSWS DAU Self-Heal — verify the pipeline, freshness, and accuracy; fix what's broken

**Audience:** an AI agent (or human) working in this repo with warehouse access.
**Goal:** the DAU/hours numbers already on the Summer 2026 dashboard are trustworthy:
the prod pipeline is running, every upstream source is current, and sampled outputs
trace correctly back to raw data. Anything broken gets diagnosed and — where possible —
fixed in the same run.
**Cadence:** run whenever asked, after incidents, or when a chart "looks wrong".
**Sibling runbook:** `runbooks/ysws-dau-coverage.md` adds programs that are missing
entirely. If the problem is "program X isn't on the chart at all", use that one.

This runbook is timeless: procedure only, no state. Which programs exist, which sources
feed them, and what was healthy last time all live in the database, the dbt models, and
git history — rediscover them every run. Never assume a prior run's diagnosis still
holds.

---

## Phase 0 — Preflight

- Secrets: `~/dev/hackclub/data-warehouse/.env` (canonical dev checkout if in a worktree).
  Load with `set -a && source ~/dev/hackclub/data-warehouse/.env && set +a`.
- Prod warehouse: `psql "$WAREHOUSE_COOLIFY_URL"` (read-only until the fix phase).
- dbt against prod: `cd orpheus_engine_dbt && DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt <cmd>`.
- Serving schema is `public_summer_2026_analytics` (no bare `summer_2026_analytics`).
- Enumerate what SHOULD be healthy by reading the models, not from memory:
  `program_windows` + daily-grained specials in `daily_active_users.sql` = the program
  set; `summer_2026_metadata.sql` = the source-freshness contract per program.

## Phase 1 — Is the prod pipeline running?

Work data-side first — it needs no extra access and is the ground truth users see:

1. **Mart staleness.** The dashboard marts are tables rebuilt by the scheduled dbt job:
   ```sql
   SELECT MAX(last_materialized_at) FROM public_summer_2026_analytics.summer_2026_metadata;
   ```
   If this is older than the expected build cadence (compare against the Dagster
   schedule defined in `orpheus_engine/defs/dbt/`), the dbt job itself isn't running —
   that's a different failure class than stale sources, triage it first.
2. **Mirror recency per source schema.** For every schema referenced in `sources.yml`,
   check the freshest row the warehouse actually holds (max `updated_at` /
   `_row_hash_at` / `_dlt_load_id` time, whichever the schema has). dlt-based pipelines
   also expose `<schema>._dlt_loads` — confirm recent successful load_ids.
3. **Dagster itself.** If data-side signals are ambiguous (e.g. everything stale at
   once), inspect the Dagster deployment: locate it from the repo's deploy configs
   (`docker_deploy/`, `bin/`) and infra notes; it runs on the prod tailnet and may be
   reachable via nested crobat SSH like the rest of prod. If you cannot reach it,
   report the data-side evidence and ask the user rather than guessing.

## Phase 2 — Source freshness sweep

1. Read the dashboard's own health table — it encodes per-program expectations:
   ```sql
   SELECT * FROM public_summer_2026_analytics.dashboard_data_health ORDER BY health_sort_key;
   ```
   Interpretation requires the program's lifecycle (from `program_windows` and the
   unified YSWS db): an ENDED program's mirror may legitimately read lagging/stale —
   what matters is that its data covers through its window end. A LIVE program's source
   must be fresh.
2. For each stale/lagging LIVE source, drill down:
   - Does the source endpoint still accept connections? Test the matching
     `*_COOLIFY_URL`/`*_K8S_URL` from `.env` with a read-only psql connect + `\dt`.
     Connection refused / auth failure / host gone → infra problem (endpoint moved,
     credentials rotated, server down). Connects fine but warehouse is behind → the
     sync asset is failing or not scheduled; check its Sling config and Dagster asset.
   - Check whether the app changed schema (new/renamed columns break Sling streams and
     dbt CTEs): compare `information_schema` of source vs what the configs expect.
3. Also sweep the shared sources every program depends on (Hackatime heartbeats, the
   unified YSWS Airtable mirror) — a stale shared source silently flattens every
   hackatime-based program at once.

## Phase 3 — Accuracy sampling (numbers, not just freshness)

Fresh data can still be wrong. Sample and verify:

1. **Recompute-and-compare.** Pick ≥5 random (program, recent complete date) pairs from
   `daily_active_users`; recompute DAU for each straight from the program's raw source
   tables using the documented methodology (read the program's CTE), and compare. Any
   mismatch beyond rounding → investigate before touching anything.
2. **Trace random users.** Pick ≥8 random rows from `summer_unified_time_log` across
   programs; for each, trace to the raw source (heartbeat hours for that email+alias,
   or the journal/devlog rows) and confirm hours, date attribution, and project linkage.
3. **Cross-model invariants**, each as a SQL check:
   - `daily_active_users_deduped.dau ≤ SUM(daily_active_users.dau)` per date, and ≥ MAX
     of any single program that date.
   - Row-level DAU weights on `summer_unified_time_log` aggregate back to the marts
     (within ~0.01): `SUM(dau_deduped)` per date == `daily_active_users_deduped.dau`, and
     `SUM(dau)` per (program, date) == `daily_active_users.dau`. The weights spread each
     person's "1" across their rows (dau within-program, dau_deduped across all programs),
     so the sums must equal the headcounts. A drift means the weighting math broke or
     identity/NULL handling drifted between the time log and the marts (this is what
     `summer_2026_dau_deduped_reconciles` tests).
   - hours per (program, user, day) ≤ 24; credited ≤ ~1.0 per (user, hour) on Hackatime
     paths.
   - `latest_complete_date` = yesterday (UTC-ish; hackatime buckets are ET-labeled —
     known, don't "fix").
   - No date gaps in a live program's series; no rows after an ended program's window.
   - Programs that exclude banned users have ZERO rows from currently-banned users
     (re-run the exclusion as a check — bans accrue over time, and full rebuilds should
     pick new bans up; if a mirror is incremental, verify ban flags actually refresh).
   - `split_factor` distribution sane: overlapping-window programs show some splits;
     non-overlapping show none.
4. **Anomaly scan.** Day-over-day DAU or hours changes >5× for any program; a live
   program flatlining at 0 while the unified YSWS db shows recent approvals for it;
   hours/DAU ratios drifting outside ~0.1–10 h/user. Each anomaly gets a one-line
   explanation (real event, e.g. a deadline spike, vs defect).

## Phase 4 — Triage and fix

Classify each finding and act by class:

| Class | Signature | Action |
|---|---|---|
| dbt-job-down | marts stale, sources fine | find why the scheduled job stopped; if it's an env/infra issue you can't reach, report precisely; you can one-off rebuild (4b) to unblock the dashboard meanwhile |
| mirror-broken | live source stale; endpoint unreachable or sync failing | endpoint/credential fixes usually need a human (Coolify access) — gather exact evidence (host, port, error) and ask; Sling config fixes (renamed column, new table) you can do. Improvising access (SSH tunnel, one-off `docker exec psql`, short-lived proxy) is fine for DIAGNOSIS, but never the fix: don't leave a temp proxy standing, don't create systemd units or other persistent server changes. Tear down anything you stood up, then HARD STOP and ask the user for a durable endpoint the sync can rely on (Coolify reaps unmanaged helper containers, so an improvised path will silently die) |
| model-defect | recompute mismatch, invariant violation, schema drift | fix the dbt CTE; follow the coverage runbook's ground rules and validation protocol |
| data-abuse | new junk pattern (duration spam, alias gaming) inflating numbers | extend quality controls (caps/exclusions) per the coverage runbook's ground rules — audit first, document numbers in comments |
| benign | anomaly explained by a real event | document in the report, change nothing |

**User checkpoint:** infra changes, credential requests, and any methodology change
(new exclusion, new cap) get user sign-off before implementation. Pure defect fixes
(the model doesn't do what its own comments say) may proceed directly.

### 4a. Validate fixes

Same protocol as the coverage runbook §4c: compile, run the wrapped compiled SQL
read-only, confirm the fix changes exactly what it should (diff per-program totals
before/after), sample again around the fixed area.

### 4b. Rebuild and test

```bash
DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt run  -s "summer_unified_time_log+" "summer_2026_metadata+"
DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt test -s "summer_unified_time_log+" "summer_2026_metadata+"
```
All `summer_2026_dashboard_*` tests must pass.

## Phase 5 — Report

One table: each check (pipeline, per-source freshness, each sample/invariant, each
anomaly) with status ✅/⚠️/❌, diagnosis, action taken or needed, and who's blocked on
what (e.g. "needs env var on prod Dagster", "needs Coolify endpoint fix"). Commit any
code changes to a feature branch (do NOT push to main unless the user says so); durable
explanations belong in model comments, not in this runbook.
