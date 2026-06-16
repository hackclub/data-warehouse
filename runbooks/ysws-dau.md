# Runbook: YSWS DAU Coverage — find and add missing program DAU calculations

**Audience:** an AI agent (or human) working in this repo with warehouse access.
**Goal:** every YSWS program that meaningfully matters (ranked by weighted projects over
the past 12 months) **and has a live leading-indicator activity system** (see ground
rule 7) has correct daily-active-user and hours-logged series in the
`summer_2026_analytics` dbt models. Programs whose only record is a submission form
(repo URL + self-reported hours) are out of scope — `n/a`, not "unbuilt".
**Cadence:** run whenever asked. Each run adds ONE program (chosen by the user) —
incremental by design.
**Sibling runbook:** `runbooks/ysws-dau-self-heal.md` checks that what already exists is
healthy and accurate. If the user's complaint is "a chart looks wrong/stale", run that
one instead.

This runbook is timeless: it holds procedure and hard-won lessons only. All state —
which programs are covered, which were investigated, what's blocked — lives in the
database, the dbt model comments, and git history. Rediscover it every run (Phase 1);
never assume a previous run's conclusions still hold.

Reference implementation (read before changing anything): commit `401e325`
("Add Summer of Making 2025 and Blueprint to Summer 2026 DAU charts") and the comments in
`orpheus_engine_dbt/models/summer_2026_analytics/summer_unified_time_log.sql` — the
model comments are the system's institutional memory and document every methodology
decision made so far, per program.

---

## Ground rules (the quality bar — non-negotiable)

1. **No banned users.** If the program has a ban/fraud flag (`is_banned` etc.), audit the
   banned share of hours, exclude banned users, and record the measured numbers in a SQL
   comment. Mere project deletion is NOT a fraud signal — keep non-banned users' deleted
   projects.
2. **Only program-linked time.** Attribute Hackatime hours only through the program's
   explicit project↔alias linkage. Beware full-mirror tables: some apps keep a table of
   EVERY Hackatime project for every signed-in user (no project FK) — using such a table
   credits all of a user's coding everywhere to the program. Verify the FK shape first.
3. **No double counting.**
   - Never count a banked-time field (a devlog/journal `duration_seconds` that snapshots
     Hackatime time) alongside Hackatime claims for the same program.
   - Programs whose run windows overlap share the equal-split dedup in
     `summer_unified_time_log` (alias claimed by N programs, or same repo URL same day →
     hours / N). Adding an overlapping program WILL shave split hours off existing
     programs — that is correct; quantify and report the deltas, don't hide them.
4. **Closed windows for ended programs.** Hackatime aliases never unclaim. An ended
   program with an open window will wrongly split hours with live programs forever.
5. **Cap self-reported time.** Free-input duration fields attract junk (caps have caught
   999h single entries and bulk-entry spam reaching 400h/user-day). If the source app has
   no server-side validation, cap at 24h/entry AND proportionally rescale each user-day
   to ≤24h. DAU is never affected by caps.
6. **Audit before deciding, document after.** Every exclusion/cap must cite measured
   numbers in the model comments, the way the existing CTE comments do.
7. **Leading indicators only — no post-hoc reconstruction.** DAU and hours must come
   from a system Hack Club operated that recorded activity *live, while users were
   building*: Hackatime heartbeats reached through a project↔alias link the program
   captured during the build, or an app's own devlogs / journals / work-sessions /
   daily rollups. A repo URL plus self-reported `hours_spent` collected on a
   submission form is NOT an activity system — we only learn the repo exists at
   submission, so anything derived from it after the fact (e.g. scraping the
   submitted repo's commit history) is lagging and survivorship-biased: it credits
   only people who finished and submitted, and assigns activity to dates no HC system
   was actually watching. Do not use it. The lone grandfathered exception is Highway,
   where the JOURNAL.md commits WERE the deliverable and the repos were the
   submission's whole point; treat it as a one-off, never as a template, and do not
   generalize commit-day reconstruction to any other program. A program with no live
   leading-indicator system is `n/a` for DAU — not a coverage gap to be filled.

---

## Phase 0 — Preflight

- Secrets: `~/dev/hackclub/data-warehouse/.env` (if running from a worktree, the
  canonical dev checkout's `.env` is the source of truth). Load with
  `set -a && source ~/dev/hackclub/data-warehouse/.env && set +a`.
- Prod warehouse, read-only validation: `psql "$WAREHOUSE_COOLIFY_URL"`.
- dbt against prod: `cd orpheus_engine_dbt && DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt <cmd>`
  (env must be loaded first — sources.yml needs env vars to parse).
- **The serving schema is `public_summer_2026_analytics`** (dbt target schema `public` +
  custom schema). There is no bare `summer_2026_analytics` schema in prod.
- `git fetch` and branch off latest `origin/main`.

## Phase 1 — Coverage gap analysis (rediscover everything)

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
- any daily-grained specials handled directly in `daily_active_users.sql` (programs with
  their own rollup or artifact-based activity — read the model to enumerate them).

Map codenames → public names by reading the model comments; when a codename doesn't
obviously match a unified-db name, check the program's site or ask the user.

### 1c. Prior investigations

Check `git log --oneline -- orpheus_engine_dbt/models/summer_2026_analytics runbooks/`
and the model comments for programs previously investigated or blocked, so you don't
redo a dead end — but treat old conclusions as hypotheses to re-verify cheaply (an app
that had no DB last quarter may have one now).

### Skip rules

- One-day or weekend events (all approvals clustered on ~1 date in the 1a query) have no
  daily-activity system — mark `n/a`, don't count as missing.
- **Submission-only programs** — those whose only trace is the unified-DB row (repo URL +
  self-reported hours) with no live leading-indicator system (no app devlogs/sessions/
  journals, no Hackatime project↔alias link captured during the build) — are `n/a` per
  ground rule 7, not a coverage gap. Most low-ceremony Airtable-form YSWS land here, and
  they can rank high by weighted projects, so check this BEFORE proposing them: a repo
  URL in `code_url` and a `hours_spent` value are NOT a leading indicator. The discriminator
  is whether the program captured a live Hackatime alias / app activity per user during
  the build — if it only collected a repo at submission, stop, mark `n/a`. (In-person
  event programs like Daydream/Campfire and certifications are `n/a` for the same reason
  unless they separately captured live per-user build activity — attendance is not DAU.)
- A program already covered but broken belongs to the self-heal runbook, not this one.

## Phase 2 — Triage the top 3 actionable gaps

For each of the top 3 missing programs, classify the work by walking this ladder and
stopping at the first hit:

| Class | Condition | Work |
|---|---|---|
| A | Warehouse schema exists with activity tables (check `information_schema`) | dbt only (~hours) |
| B | No schema, but DB is findable on known infra | Sling sync + dbt |
| C | DB location unknown | infra discovery first, then B |
| D | App DB exists but holds no activity data (e.g. only an rsvps/landing table) | needs a methodology ruling from the user (Hackatime+Airtable joins, or skip) |

Infra discovery (class C), in order:
1. Warehouse schemas: `SELECT schema_name FROM information_schema.schemata`.
2. Already-declared dbt sources (`sources.yml`) and Sling configs
   (`orpheus_engine/defs/sling/assets.py`) — programs are sometimes mirrored long before
   anyone builds DAU on top.
3. Coolify worker servers. Enumerate them live (`tailscale status | grep -i coolify`),
   then search each over nested crobat SSH (plain `-J` fails; crobat flaps — retry after
   ~30s): `ssh crobat 'ssh root@<IP> "docker ps -a -q | xargs docker inspect 2>/dev/null | grep -io \"[a-z0-9./_-]*<name>[a-z0-9./_-]*\" | sort -u"'`
   Grep FULL inspect output, not just proxy domain labels — DB references hide in env
   vars and container names. DB URL shape once found:
   `postgres://postgres:<pw from container env>@<worker-tailscale-ip>:<published proxy port>/postgres?sslmode=disable`
   (`sslmode=disable` is only allowed for Tailscale 100.64/10 IPs — enforced in assets.py).
4. The custom k8s cluster (Hetzner-hosted) for Cloudflare-fronted apps not on Coolify —
   see how existing `*_K8S_URL` env vars are shaped. Everything deploys on Coolify or
   this k8s; repo deploy configs (Kamal etc.) can be vestigial templates — never trust
   them for hosting location.
5. Airtable: `airtable_raw_all_bases.bases` (search `schema::text ILIKE '%<name>%'`;
   `base_name` is often NULL).
6. **Never guess credentials or hosts. If not found, the program is class-C-blocked: ask.**

**Creative access is fine for discovery — never for deployment.** When a DB exists but
isn't directly reachable, improvise to get a look inside: nested SSH tunnels, a one-off
`docker exec psql` on the host, a short-lived proxy container, etc. That's how you
classify the program and inspect schemas. But the prod pipeline must never depend on an
improvised path: do not leave a temp proxy standing, do not create systemd units or make
any other persistent server changes, do not treat a hand-rolled sidecar as the permanent
route. (Coolify reaps unmanaged helper containers, so an improvised proxy will silently
die anyway.) Tear down whatever you stood up, then HARD STOP and ask the user to provide
or bless a durable connection path (a real endpoint + credentials that survive restarts)
before wiring anything in 4b.

Also verify SUBSTANCE: a found DB must contain activity tables (projects/journals/claims)
— at least one program's "DB" turned out to be an rsvps-only landing app while the real
activity lived in Hackatime + Airtable. And check the data actually spans the program's
run (row counts, max timestamps) before classifying as class A/B.

Those activity tables must be a LEADING indicator (ground rule 7): live per-user
devlogs/sessions/journals, a program-native daily rollup, or a Hackatime project↔alias
link the program captured during the build. A table that only holds final submissions
(repo URL + self-reported hours, even one row per project) has no daily signal and is
NOT reconstructable from the submitted repos — classify the program `n/a`, not class A.
A useful triage shortcut for the whole form-YSWS tail: ask "did this program capture a
Hackatime alias (or app activity) per user while they were building, or only a repo at
submission?" — only the former is buildable.

## Phase 3 — User checkpoint (required, do not skip)

Present a compact table: rank, program, weighted projects, class (A–D), what exactly is
needed, and your recommendation with one-line rationale. Then ask the user to pick ONE
program to implement this run (offer the top 3 + "other"). Do not start implementation
before the user chooses. If a class-D ruling is needed, present the options and get the
decision as part of the same checkpoint.

## Phase 4 — Implement the user's choice

### 4a. Methodology decision tree

Inspect the real columns (`information_schema`) — never trust table names or source
descriptions. Then:

1. **Hackatime claims path** when the app has explicit project↔alias links. Identity
   join: normalized email (copy the exact CASE expression used throughout
   `summer_unified_time_log.sql`) or, if the app stores a Hackatime user id instead of a
   shared email, the hackatime_user_id→email mapping pattern (see the beest CTEs).
   Measure and report join coverage (good benchmark: ~98% of active users matched).
2. **Custom/journal path** for self-reported durations (see the stack/blueprint CTEs).
   Audit the duration distribution; apply ground-rule-5 caps if free-input.
3. **Program-native daily rollup** or **artifact-based activity** (timelapses, journal
   posts) when the app provides them — these go directly into `daily_active_users.sql` /
   `daily_hours_logged_by_program.sql` instead of the unified log (see the existing
   daily-grained programs there). The artifact must be a LIVE leading indicator the
   program tracked during the build (ground rule 7). Highway's GitHub-commit-day path is
   the grandfathered exception, NOT a reusable pattern: do not reconstruct DAU from a
   submitted repo's commit history for any other program — those repos were unknown until
   submission, so the signal is lagging and survivorship-biased.
4. Hours come from `{{ ref('hourly_project_activity') }}` for Hackatime paths
   (prod: `public_hackatime_analytics.hourly_project_activity`). Known quirks: hour
   buckets are ET-labeled (pre-existing, applies to every program — do not "fix" it
   for one program); no Hackatime trust-level filtering exists upstream.

### 4b. Files to touch (mirror commit 401e325)

- `summer_unified_time_log.sql`: `program_windows` entry (+ window rationale comment),
  claims and/or custom CTE (+ audited quality-control comment), union wiring. The
  row-level DAU weights (`dau`, `dau_deduped` — section 8) are computed generically over
  the final row set, so a new program gets them for free; no per-program wiring. Two
  prerequisites for them to be correct:
  - **Identity.** `dau_deduped` (and `daily_active_users_deduped`) merge a person across
    programs by `user_email`, so emit the SAME normalized email (copy the exact CASE
    expression). A program-specific fallback key (`newprog_user_<id>`) does NOT dedup
    against that person's email elsewhere — it overcounts the unique total in BOTH the
    stacked `dau_deduped` and the deduped mart, so they still reconcile and the test
    won't catch it. Bridge to a real email wherever the source can; fall back only when
    it genuinely can't.
  - **Hours.** `dau_deduped` weights each person by their share of their own day's
    `credited_hours_logged`. An activity-only / zero-hour program (no durations, like
    Highway) only contributes a person via the equal-split fallback and gets 0 of any
    person who logged real time elsewhere. Fine for historical hour-less programs; **flag
    it if a LIVE program would be hour-less**, since it will read low on the stacked chart.
  - **Daily-grain sources spread across 24h.** If the source only knows the calendar day
    (a per-day rollup, not real hourly timestamps), aggregate to (day, user) then
    `CROSS JOIN hours_of_day` and divide the hours by 24 — one row per hour, attribution
    split evenly — rather than spiking the whole day at 00:00 UTC (see macondo/horizons).
    Round hours to ~6dp so 24×(hours/24) recovers the day total. Hour-bearing sources with
    real timestamps (e.g. fallout) and zero-hour activity-only markers (highway) are left
    at their native grain.
- `summer_2026_metadata.sql`: program row + freshness UNION (note mirror-freshness
  semantics for ended programs).
- `daily_active_users.sql`: header comment (sources, PROGRAM STATUS) only — `dau`
  (distinct users per program/date) is computed generically from the time log. The
  stackable cross-program headcount lives on `summer_unified_time_log.dau_deduped`
  (see that file's bullet above), not here.
- `sources.yml`: source block with `meta: { dagster: { deps: ['<x>_warehouse_mirror'] } }`,
  only the tables actually used.
- Multi-word codenames: extend `program_labels` in `dashboard_program_daily_metrics.sql`
  AND the CASE in `dashboard_dau_methodology.sql` (INITCAP keeps underscores).
- New Sling sync (class B/C): connection + replication config in
  `orpheus_engine/defs/sling/assets.py` with sensitive-column `select:` allow-lists
  (exclude tokens/ciphertext/otp — follow the existing examples), infra tables disabled,
  env var added to `.env` AND to `_SLING_CONNECTION_URL_ENV_VARS`. Flag explicitly in
  your report that prod Dagster needs the env var set — you can't set it yourself.
  The connection URL must be the durable, user-provided path — never a tunnel or proxy
  you improvised during Phase 2 discovery.

### 4c. Validation protocol (all steps, read-only, BEFORE rebuilding prod)

1. `dbt compile` the changed models; wrap the compiled `summer_unified_time_log` SQL in a
   per-program aggregate (`WITH model AS (<compiled sql sans final ORDER BY>) SELECT ...`)
   and run via psql: new program's totals plausible vs the unified YSWS db scale; existing
   programs unchanged except quantified dedup deltas.
2. Monthly DAU curve: ramps at launch, tapers/stops at program end, no out-of-window rows.
3. Integrity: credited ≤ ~1.0 per (user,hour) on Hackatime paths; ≤ 24.0 per user-day
   everywhere; zero future-dated rows; zero exact-dupe source rows; count email collisions.
4. Sampling (≥8 random active users): credited ≤ their global Hackatime hours in-window
   (proves linkage filtering); cross-check against any program-native total (devlog sums,
   reviewer-approved seconds, etc.) — ballpark agreement, differences explained.
5. Banned-exclusion verification: zero rows from banned users post-build.

### 4d. Build, test, deliver

```bash
DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt run  -s "summer_unified_time_log+" "summer_2026_metadata+"
DBT_PROFILES_DIR=~/.dbt_stardance_tmp uv run dbt test -s "summer_unified_time_log+" "summer_2026_metadata+"
```
All `summer_2026_dashboard_*` tests must pass, plus `summer_2026_dau_deduped_reconciles`
(asserts `SUM(summer_unified_time_log.dau_deduped)` per day == `daily_active_users_deduped.dau`
— it guards the split arithmetic and that both models filter identity/NULL the same way;
it does NOT vouch for identity quality, see 4b). Spot-check
`dashboard_program_daily_metrics` (label renders correctly, `in_45_day_chart_window`
sane). Rebuild takes ~1min and swaps tables atomically.

Commit to the feature branch (do NOT push to main unless the user says so). The durable
record of what was decided and why belongs in the model comments, not in this runbook.
Report to the user: methodology + audit numbers, dedup deltas, validation results,
surprises, and anything (env vars, Coolify fixes) that needs a human.
