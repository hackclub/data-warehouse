{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_dau ON {{ this }} (program_name, activity_date);"
) }}

-- ============================================================================
-- Summer 2026 — Daily Active Users (per program)
--
-- Grain: one row per (program_name, activity_date) in UTC.
--
-- DAU = distinct users who logged qualifying time/activity that day, using the
-- program's own activity system. It is not a shipping/submission count.
--
--   dau             — distinct active users for that program/date.
--   dau_methodology — short label explaining which activity source(s) define DAU.
--
-- Sources:
--   Coding/journal/work-session programs (stardance, flavortown, stack,
--   offtrack, beest, stasis, horizons, blueprint, summer_of_making,
--   hack_club_the_game, shipwrecked, siege, athena_award, milkyway,
--   sleepover, neighborhood, construct, shiba, midnight, carnival, moonshot)
--   come from {{ ref('summer_unified_time_log') }}, inheriting its cross-program
--   equal-split dedup and run windows. Horizons switches to its app-native
--   user_daily_activity table from 2026-04-22 onward.
--   macondo uses its own per-day rollup (daily_project_activity: hackatime_seconds
--   + journal_seconds).
--   fallout (hardware) logs time via build timelapses (lookout + lapse, duration>0)
--   and devlog journals. Ships are intentionally not counted as DAU.
--   highway (hardware, Summer 2025) kept journals as JOURNAL.md files in each
--   submitted GitHub repo, so daily activity = commit days on the submitted
--   repos (highway_github scrape + the program's Airtable submissions in
--   airtable_raw_all_bases). DAU only — commits carry no durations, so highway
--   has no hours series.
--
-- PROGRAM STATUS:
--   active here : stardance, flavortown (historical baseline), fallout, macondo,
--                 stack, offtrack (0 until source rows arrive), beest, stasis,
--                 horizons, blueprint, hack_club_the_game (Hackatime-only;
--                 beta opened 2026-01-16, first public approvals 2026-02-17),
--                 sleepover (Hackatime-only; Airtable-backed app, launched
--                 2026-01-16, ported from spring_2026_analytics),
--                 construct — Construct (active; app activity began 2025-12-02),
--                 custom devlog minutes with trust filtering and duration caps.
--                 carnival — Carnival (active; window opens 2025-12-01),
--                 Hackatime-only via project_hackatime_project alias claims;
--                 is_frozen ban gate (0 today), banked devlog/project seconds
--                 not counted (see summer_unified_time_log).
--   historical  : summer_of_making — SoM 2025 (2025-06-16 to 2025-10-02),
--                 included for the year-over-year comparison. Hackatime-only
--                 methodology; see summer_unified_time_log for why devlog
--                 durations are not used.
--                 shipwrecked — Shipwrecked 2025 (2025-05-28 to 2025-09-03),
--                 included for the year-over-year comparison. Hackatime-only
--                 methodology; see summer_unified_time_log for why the banked
--                 link hours (rawHours/hoursOverride) are not used.
--                 siege — Siege (2025-08-31 to 2026-04-07), Hackatime-only via
--                 projects.hackatime_projects alias claims; banned users and
--                 fraud-adjudicated projects excluded (see
--                 summer_unified_time_log).
--                 athena_award — Athena Award (2025-05-21 to ~2026-02-13),
--                 included for the year-over-year comparison. Hackatime-only
--                 methodology via the program's Airtable base (its app's
--                 backend); see summer_unified_time_log for why the banked
--                 hackatime_duration/approved_duration are not used.
--                 highway — Highway 2025 (submissions 2025-05 to 2025-11,
--                 Undercity event 2025-07-11..14), included for the
--                 year-over-year comparison. GitHub-commit-day methodology
--                 (see highway_dau below); only ~37% of submissions were
--                 linkable to Hackatime projects, so the Hackatime path would
--                 have undercounted ~2x.
--                 milkyway — Milkyway (2025-10-07 to 2026-05-01), Hackatime
--                 claims + artlog art-time via the program's Airtable base
--                 (its app's backend); banned users excluded, devlog hours
--                 not counted (banked; see summer_unified_time_log).
--                 neighborhood — Neighborhood 2025 (2025-05-01 to 2025-07-31),
--                 Hackatime-only via its Airtable Hackatime project alias rows;
--                 see summer_unified_time_log for why Airtable total_time_hours
--                 is not used.
--                 midnight — Midnight (2025-11-05 to 2026-04-21),
--                 Hackatime-only via projects.now_hackatime_projects; fraud
--                 projects excluded.
--                 moonshot — Moonshot (2025-10-25 to 2025-12-31), Hackatime-only
--                 via HackatimeProjectLink alias claims (same Prisma schema as
--                 shipwrecked); FraudSuspect users excluded, banked
--                 rawHours/hoursOverride not counted. One-time manual backfill
--                 from a normally-stopped DB (see summer_unified_time_log).
--                 shiba — Shiba (2025-08-18 to 2026-02-12), Airtable
--                 raw-all-bases creator posts + logged-in play telemetry with
--                 duration caps.
--                 horizons — Horizons uses Hackatime claims before 2026-04-22,
--                 then app-native qualified user_daily_activity seconds.
--
-- Identity = normalized email where the source provides or can bridge to one;
-- otherwise a stable source-specific user key. A person counts once per program
-- per day.
-- ============================================================================

WITH coding_dau AS (
    SELECT
        program_name,
        (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
        COUNT(DISTINCT user_email) AS dau,
        CASE
            WHEN BOOL_OR(logging_method = 'hackatime') AND BOOL_OR(logging_method <> 'hackatime')
                THEN 'hackatime_and_custom_time'
            WHEN BOOL_OR(logging_method = 'hackatime')
                THEN 'hackatime_time'
            ELSE 'custom_time'
        END AS dau_methodology
    FROM {{ ref('summer_unified_time_log') }}
    WHERE program_name <> 'horizons'
       OR (activity_hour AT TIME ZONE 'UTC')::date < DATE '2026-04-22'
    GROUP BY program_name, (activity_hour AT TIME ZONE 'UTC')::date
),

-- Macondo's own per-day rollup: hackatime_seconds + journal_seconds
macondo_dau AS (
    SELECT
        'macondo'::text AS program_name,
        dpa.day::date AS activity_date,
        COUNT(DISTINCT dpa.user_id) FILTER (WHERE dpa.hackatime_seconds > 0
                                              OR dpa.journal_seconds > 0) AS dau,
        'daily_project_activity_time'::text AS dau_methodology
    FROM {{ source('macondo', 'daily_project_activity') }} dpa
    WHERE dpa.day::text ~ '^\d{4}-\d{2}-\d{2}$'
      AND dpa.day::date >= DATE '2026-03-23'
      AND dpa.day::date < CURRENT_DATE
    GROUP BY 1, dpa.day::date
),

horizons_dau AS (
    SELECT
        'horizons'::text AS program_name,
        uda.local_date::date AS activity_date,
        COUNT(DISTINCT uda.user_id) AS dau,
        'daily_user_activity_time'::text AS dau_methodology
    FROM {{ source('horizons', 'user_daily_activity') }} uda
    WHERE uda.local_date::date >= DATE '2026-04-22'
      AND uda.local_date::date < CURRENT_DATE
      AND uda.qualified
      AND uda.seconds > 0
    GROUP BY 1, uda.local_date::date
),

-- Fallout (hardware): logged time = build-time timelapses (lookout/lapse,
-- duration>0) or a devlog journal entry.
fallout_norm AS (
    SELECT id,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(email)), '+', 1)
        END AS user_email
    FROM {{ source('fallout', 'users') }}
),

fallout_activity AS (
    SELECT u.user_email, (l.created_at AT TIME ZONE 'UTC')::date AS activity_date
    FROM {{ source('fallout', 'lookout_timelapses') }} l
    JOIN fallout_norm u ON u.id = l.user_id
    WHERE l.duration > 0 AND (l.created_at AT TIME ZONE 'UTC') <= NOW()
    UNION ALL
    SELECT u.user_email, (l.created_at AT TIME ZONE 'UTC')::date
    FROM {{ source('fallout', 'lapse_timelapses') }} l
    JOIN fallout_norm u ON u.id = l.user_id
    WHERE l.duration > 0 AND (l.created_at AT TIME ZONE 'UTC') <= NOW()
    UNION ALL
    SELECT u.user_email, (j.created_at AT TIME ZONE 'UTC')::date
    FROM {{ source('fallout', 'journal_entries') }} j
    JOIN fallout_norm u ON u.id = j.user_id
    WHERE j.discarded_at IS NULL AND (j.created_at AT TIME ZONE 'UTC') <= NOW()
),

fallout_dau AS (
    SELECT
        'fallout'::text AS program_name,
        activity_date,
        COUNT(DISTINCT user_email) AS dau,
        'hardware_build_time_and_journals'::text AS dau_methodology
    FROM fallout_activity
    WHERE activity_date >= DATE '2026-03-01'
    GROUP BY 1, activity_date
),

-- Highway (hardware, Summer 2025): journals were JOURNAL.md files committed to
-- each submitted GitHub repo, so a participant's daily activity trace is the
-- commit history of their submitted repos (highway_github backfill scrape).
-- QUALITY CONTROLS (audited 2026-06-12):
--   * 'Purged' submissions excluded — the program's purge bin holds duplicates,
--     all-AI journals, and fraud (123 of 1,692 submissions, 88 emails; 51 of
--     those emails also have legit Fulfilled projects, which still count).
--     Rejected submissions are kept: rejection was quality review (bad README,
--     BOM issues), not a fraud signal, and the build activity was real.
--   * A commit day credits the SUBMITTER(S) of the repo, not the commit author:
--     commit author emails/logins are unreliable (noreply addresses) and team
--     projects were submitted per-member. Identity = normalized email.
--   * Window 2025-05-01 .. 2025-11-01 (exclusive): submissions span 2025-05 to
--     2025-11 (peak Jul, Undercity 2025-07-11..14); later unified-db approvals
--     are review lag, not activity. Commits outside the window (repos predating
--     or outliving the program) are not Highway activity.
highway_norm AS (
    SELECT
        CASE WHEN POSITION('@' IN LOWER(BTRIM(r.fields->>'Email'))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 2)
             ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '+', 1), '')
        END AS user_email,
        LOWER((REGEXP_MATCH(r.fields->>'Github_Url',
                            'github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)'))[1])
        || '/' ||
        REGEXP_REPLACE(
            LOWER((REGEXP_MATCH(r.fields->>'Github_Url',
                                'github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)'))[2]),
            '\.git$', '') AS repo_key
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    WHERE r.base_id = 'appuDQSHCdCHyOrxw'
      AND r.table_id = 'tbl9QnZ320NTGJHJj'          -- Highway Projects table
      AND COALESCE(r.fields->>'Status', '') <> 'Purged'
      AND r.fields->>'Github_Url' ~* 'github\.com/'
),

highway_dau AS (
    SELECT
        'highway'::text AS program_name,
        (c.authored_at AT TIME ZONE 'UTC')::date AS activity_date,
        COUNT(DISTINCT h.user_email) AS dau,
        'github_commit_days'::text AS dau_methodology
    FROM highway_norm h
    JOIN {{ source('highway_github', 'repos') }} gr
        ON gr.repo_key = h.repo_key
        AND gr.scrape_status = 'ok'
    JOIN {{ source('highway_github', 'commits') }} c ON c.repo_key = h.repo_key
    WHERE h.user_email IS NOT NULL
      AND (c.authored_at AT TIME ZONE 'UTC') <= NOW()
      AND (c.authored_at AT TIME ZONE 'UTC')::date >= DATE '2025-05-01'
      AND (c.authored_at AT TIME ZONE 'UTC')::date < DATE '2025-11-01'
    GROUP BY 1, 2
),

combined AS (
    SELECT program_name, activity_date, dau, dau_methodology FROM coding_dau
    UNION ALL
    SELECT program_name, activity_date, dau, dau_methodology FROM macondo_dau
    UNION ALL
    SELECT program_name, activity_date, dau, dau_methodology FROM horizons_dau
    UNION ALL
    SELECT program_name, activity_date, dau, dau_methodology FROM fallout_dau
    UNION ALL
    SELECT program_name, activity_date, dau, dau_methodology FROM highway_dau
)

SELECT program_name, activity_date, dau, dau_methodology
FROM combined
WHERE activity_date < CURRENT_DATE
ORDER BY activity_date DESC, program_name
