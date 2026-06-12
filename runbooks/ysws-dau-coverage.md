# Runbook: YSWS DAU Coverage — find, add, and self-heal program DAU calculations

**Audience:** an AI agent (or human) working in this repo with warehouse access.
**Goal:** every YSWS program that meaningfully matters (ranked by weighted projects over
the past 12 months) has correct daily-active-user and hours-logged series in the
`summer_2026_analytics` dbt models, and the ones already there keep working.
**Cadence:** run whenever asked, or when the dashboard health table shows stale/missing
programs. Each run adds or fixes ONE program (chosen by the user) — incremental by design.

Reference implementations (read these before changing anything):
- Commit `401e325` — added Summer of Making 2025 + Blueprint; the canonical template for
  methodology, quality controls, comments, and validation.
- `orpheus_engine_dbt/models/summer_2026_analytics/summer_unified_time_log.sql` — the
  heart of the system. Its comments document every methodology decision made so far.

---

## Ground rules (the quality bar — non-negotiable)

1. **No banned users.** If the program has a ban/fraud flag (`is_banned` etc.), audit the
   banned share of hours, exclude banned users, and record the numbers in a SQL comment.
   Precedent: SoM banned users held 28.2% of hours; Blueprint 4.1%. Mere project deletion
   is NOT a fraud signal — keep non-banned users' deleted projects.
2. **Only program-linked time.** Attribute Hackatime hours only through the program's
   explicit project↔alias linkage. Beware full-mirror tables: SoM's `hackatime_projects`
   mirrors EVERY Hackatime project of every signed-in user (no project FK) — using such a
   table credits all of a user's coding to the program. Verify the FK shape first.
3. **No double counting.**
   - Never count a banked-time field (devlog/journal `duration_seconds` that snapshots
     Hackatime time) alongside Hackatime claims for the same program.
   - Programs whose run windows overlap share the equal-split dedup in
     `summer_unified_time_log` (alias claimed by N programs, or same repo URL same day →
     hours / N). Adding an overlapping program WILL shave split hours off existing
     programs — that is correct; quantify and report the deltas, don't hide them.
4. **Closed windows for ended programs.** Hackatime aliases never unclaim. An ended
   program with an open window will wrongly split hours with live programs forever.
5. **Cap self-reported time.** Free-input duration fields attract junk (Blueprint: 999h
   entries, 81-entry spam reaching 400h/user-day). If the source has no server-side
   validation, cap at 24h/entry AND proportionally rescale each user-day to ≤24h. DAU is
   never affected by caps.
6. **Audit before deciding, document after.** Every exclusion/cap must cite measured
   numbers in the model comments, the way the existing CTE comments do.

---

## Phase 0 — Preflight

- Secrets: `~/dev/hackclub/data-warehouse/.env` (note: if running from a worktree, the
  canonical dev checkout's `.env` is the source of truth). Load with
  `set -a && source ~/dev/hackclub/data-warehouse/.env && set +a`.
- Prod warehouse, read-only validation: `psql "$WAREHOUSE_COOLIFY_URL"`.
- dbt against prod: `cd orpheus_engine_dbt && DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt <cmd>`
  (env must be loaded first — sources.yml needs env vars to parse).
- **The serving schema is `public_summer_2026_analytics`** (dbt target schema `public` +
  custom schema). There is no bare `summer_2026_analytics` schema in prod.
- `git fetch` and branch off latest `origin/main`.

## Phase 1 — Coverage gap analysis

### 1a. Rank programs by weighted projects (past 12 months)

```sql
SELECT n.value AS program_name,
       ROUND(SUM(COALESCE(p.ysws_weighted_project_contribution, 0))::numeric, 1) AS weighted_projects,
       COUNT(*) AS project_count,
       MIN(p.approved_at)::date AS first_approval,
       MAX(p.approved_at)::date AS last_approval
FROM airtable_unified_ysws_projects_db.approved_projects p
JOIN airtable_unified_ysws_projects_db.approved_projects__ysws_name n
  ON n._dlt_parent_id = p._dlt_id
WHERE p.approved_at >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY n.value ORDER BY weighted_projects DESC LIMIT 40;
```

### 1b. Current DAU coverage

Covered = the union of:
- `program_windows` entries in `summer_unified_time_log.sql`, plus
- the daily-grained specials in `daily_active_users.sql` (currently `macondo`, `fallout`).

Map codenames → public names by reading the model comments (e.g. `stardance` is the
current summer flagship; `summer_of_making` = "Summer of Making"). When unsure whether a
codename matches a unified-db name, check the program's site or ask.

### 1c. Self-heal scan (existing programs)

```sql
SELECT * FROM public_summer_2026_analytics.dashboard_data_health
ORDER BY health_sort_key;
-- and: programs whose recent DAU flatlined at 0 while still active
SELECT program_name, MAX(activity_date) FILTER (WHERE dau > 0) AS last_nonzero
FROM public_summer_2026_analytics.daily_active_users GROUP BY 1 ORDER BY 2;
```

A live program with a `stale` source or a DAU flatline is a self-heal candidate and
competes with missing programs for this run's slot. Typical causes: the upstream mirror
endpoint moved/broke (fix in Coolify/Sling config), or the app changed schema (fix the
CTE). Known chronic case: horizons' mirror (see model comments).

### Skip rules

- One-day or weekend events (all approvals on ~1 date, e.g. Daydream, Campfire
  Satellites) have no daily-activity system — mark `n/a`, don't count as missing.
- Programs already investigated and blocked are listed at the bottom of this file under
  **Program ledger** — read it first, update it after every run.

## Phase 2 — Triage the top 3 actionable gaps

For each of the top 3 missing (or broken) programs, classify the work by walking this
ladder and stopping at the first hit:

| Class | Condition | Work |
|---|---|---|
| A | Warehouse schema exists with activity tables (check `information_schema`) | dbt only (~hours) |
| B | No schema, but DB is findable on known infra | Sling sync + dbt |
| C | DB location unknown | infra discovery first, then B |
| D | App DB has no activity data (e.g. Highway = rsvps-only landing app) | needs a methodology decision from the user (Hackatime+Airtable joins, or skip) |

Infra discovery (class C), in order:
1. Warehouse schemas: `SELECT schema_name FROM information_schema.schemata`.
2. Already-declared dbt sources (`sources.yml`) and Sling configs (`orpheus_engine/defs/sling/assets.py`)
   — several programs are mirrored but never got DAU models (that's how SoM/Blueprint/HCTG/
   shipwrecked_the_bay sat unused).
3. Coolify workers over Tailscale via nested crobat SSH (plain `-J` fails; crobat flaps —
   retry): `ssh crobat 'ssh root@<IP> "docker ps -a -q | xargs docker inspect 2>/dev/null | grep -io \"[a-z0-9./_-]*<name>[a-z0-9./_-]*\" | sort -u"'`
   Workers: a=100.80.243.122, b=100.75.33.42, cooked=100.82.244.53. Grep FULL inspect
   output, not just domain labels. DB URL shape once found:
   `postgres://postgres:<pw from container env>@<worker-tailscale-ip>:<published proxy port>/postgres?sslmode=disable`.
4. nephthys k8s (Hetzner) for Cloudflare-fronted apps not on Coolify — cf. `HORIZONS_K8S_URL`.
5. The Coolify cloud API (`COOLIFY_API_KEY`) has been 401ing — don't burn time; note it.
6. Airtable: `airtable_raw_all_bases.bases` (search `schema::text ILIKE '%<name>%'`;
   `base_name` is often NULL).
7. **Never guess credentials/hosts. If not found, the program is class C-blocked: ask.**

Also verify SUBSTANCE: a found DB must contain activity tables (projects/journals/claims),
not just rsvps/users (the Highway lesson). And check the data actually spans the program's
run (row counts, max timestamps).

## Phase 3 — User checkpoint (required, do not skip)

Present a compact table: rank, program, weighted projects, status (missing/broken),
class (A–D), what exactly is needed, and your recommendation with one-line rationale.
Then ask the user to pick ONE program to implement this run (offer the top 3 + "other").
Do not start implementation before the user chooses. If a class-D decision is needed
(e.g. Highway), present the options and get the ruling as part of the same checkpoint.

## Phase 4 — Implement the user's choice

### 4a. Methodology decision tree

Inspect the real columns (`information_schema`) — never trust table names or source
descriptions. Then:

1. **Hackatime claims path** (stardance/SoM pattern) when the app has explicit
   project↔alias links. Identity join: normalized email (copy the exact CASE expression
   used throughout `summer_unified_time_log.sql`) or, if the app stores a Hackatime user
   id instead of a shared email, the beest pattern (`beest_htid_email`). Measure and
   report join coverage (SoM benchmark: 97.8% of active users matched).
2. **Custom/journal path** (stack/blueprint pattern) for self-reported durations. Audit
   the distribution; apply rule 5 caps if free-input.
3. **Program-native daily rollup** (macondo pattern) or **artifact-based** (fallout
   timelapses/journals) when the app provides them — these go directly into
   `daily_active_users.sql` / `daily_hours_logged_by_program.sql` instead of the unified log.
4. Hours come from `{{ ref('hourly_project_activity') }}` for Hackatime paths
   (prod: `public_hackatime_analytics.hourly_project_activity`). Known quirks: hour
   buckets are ET-labeled (pre-existing, applies to every program — do not "fix"); no
   Hackatime trust-level filtering exists upstream.

### 4b. Files to touch (mirror commit 401e325)

- `summer_unified_time_log.sql`: `program_windows` entry (+ window rationale comment),
  claims and/or custom CTE (+ audited quality-control comment), union wiring.
- `summer_2026_metadata.sql`: program row + freshness UNION (note mirror-freshness
  semantics for ended programs).
- `daily_active_users.sql`: header comment (sources, PROGRAM STATUS).
- `sources.yml`: source block with `meta: { dagster: { deps: ['<x>_warehouse_mirror'] } }`,
  only the tables actually used.
- Multi-word codenames: extend `program_labels` in `dashboard_program_daily_metrics.sql`
  AND the CASE in `dashboard_dau_methodology.sql` (INITCAP keeps underscores).
- New Sling sync (class B/C): connection + replication config in
  `orpheus_engine/defs/sling/assets.py` with sensitive-column `select:` allow-lists
  (exclude tokens/ciphertext/otp — follow blueprint/stasis/stardance examples), infra
  tables disabled, env var added to `.env` AND to `_SLING_CONNECTION_URL_ENV_VARS`.
  Flag explicitly in your report that prod Dagster needs the env var set — you can't.

### 4c. Validation protocol (all steps, read-only, BEFORE rebuilding prod)

1. `dbt compile` the changed models; wrap the compiled `summer_unified_time_log` SQL in a
   per-program aggregate (`WITH model AS (<compiled sql sans final ORDER BY>) SELECT ...`)
   and run via psql: new program's totals plausible vs the unified YSWS db scale; existing
   programs unchanged except quantified dedup deltas.
2. Monthly DAU curve: ramps at launch, tapers/stops at program end, no out-of-window rows.
3. Integrity: credited ≤ ~1.0 per (user,hour) on Hackatime paths; ≤ 24.0 per user-day
   everywhere; zero future-dated rows; zero exact-dupe source rows; email-collision count.
4. Sampling (≥8 random active users): credited ≤ their global Hackatime hours in-window
   (proves linkage filtering); cross-check against any program-native total (devlog sums,
   `approved_seconds`, etc.) — ballpark agreement, differences explained.
5. Banned-exclusion verification: zero rows from banned users post-build.

### 4d. Build, test, deliver

```bash
DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt run  -s "summer_unified_time_log+" "summer_2026_metadata+"
DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt test -s "summer_unified_time_log+" "summer_2026_metadata+"
```
All 5 `summer_2026_dashboard_*` tests must pass. Spot-check
`dashboard_program_daily_metrics` (label renders correctly, `in_45_day_chart_window`
sane). Rebuild takes ~40s and swaps tables atomically.

Commit to the feature branch (do NOT push to main unless the user says so). Update the
**Program ledger** below in the same commit. Report: methodology + audit numbers, dedup
deltas, validation results, surprises, and anything (env vars, Coolify fixes) that needs
a human.

---

## Program ledger

Status of every program investigated so far. Update this section every run.

| Program | Weighted (12mo @ 2026-06) | Status | Notes |
|---|---|---|---|
| summer_of_making | 4,381 | ✅ live | Hackatime claims via `projects.hackatime_project_keys`; banned excluded (28.2% of hrs); window 2025-06-16→2025-10-03 |
| flavortown | 3,595 | ✅ live | pre-existing |
| blueprint | 2,855 | ✅ live | journals; banned excluded; 24h/entry + 24h/user-day caps; durations are "since last journal" (banked) with no server validation |
| highway | 1,683 | ⛔ blocked (D) | Coolify DB on worker b (100.75.33.42:5454) is the rsvps-only landing app. Real activity = Hackatime + Airtable base `appuDQSHCdCHyOrxw`. Needs user ruling on methodology |
| siege | 1,651 | 🔍 class C | Not on Coolify workers a/b/cooked (domain-label grep only so far); Cloudflare-fronted; likely nephthys k8s. Full-inspect grep + k8s check still to do |
| shipwrecked | 1,396 | 🟡 class A | `shipwrecked_the_bay` schema mirrored (Prisma camelCase: "User"/"Project"/"HackatimeProjectLink"). Window overlaps SoM — first real dedup-split test |
| daydream | 1,200 | n/a | weekend game-jam event, no daily activity system |
| campfire satellites | 1,117 | n/a | single-day event (2026-04-01) |
| athena award | 1,026 | 🔍 class B/C | app `express.athena` on Coolify worker a; `airtable_athena_award` schema exists; DB not yet located |
| midnight | 934 | 🔍 class C | on nephthys k8s (midnight.hackclub.com → Hetzner 5.161.244.66) |
| milkyway | 931 | 🔍 class C | Cloudflare-fronted, location unknown |
| hack_club_the_game | 690 | 🟡 class A | active program; schema mirrored + sources.yml already declared (users/projects/hackatime_projects/project_reviews); verify whether its `hackatime_projects` is per-project claims or a SoM-style full mirror, and whether identity is email or hackatime_id (beest pattern) |
| stasis / fallout / macondo / beest / stack / offtrack / horizons / stardance | — | ✅ live | pre-existing. horizons: chronically stale mirror (self-heal candidate) |
