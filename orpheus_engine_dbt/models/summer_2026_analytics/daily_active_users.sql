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
--   dau             — distinct active users for that program/date. A person who
--                     is active in N programs on a day counts 1 toward EACH of
--                     those programs, so SUM(dau) across programs OVERCOUNTS the
--                     unique overall headcount — use summer_unified_time_log's
--                     row-level dau_deduped (summed) for a reconciling stacked
--                     "unique overall DAU by program" chart.
--   dau_methodology — short label explaining which activity source(s) define DAU.
--
-- Sources:
--   Coding/journal/work-session programs (stardance, flavortown, stack,
--   offtrack, beest, stasis, horizons, blueprint, summer_of_making,
--   hack_club_the_game, shipwrecked, siege, athena_award, milkyway,
--   sleepover, neighborhood, construct, shiba, midnight, carnival, moonshot,
--   high_seas, arcade, juice)
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
--
-- All programs — including the formerly daily-grain/activity-only ones (macondo,
-- fallout, highway, and Horizons app-native activity from 2026-04-22) — now flow
-- through summer_unified_time_log, so this model reads from it alone. Each
-- program's DAU methodology is recovered from the row's logging_method token:
--   github_commit_days        -> 'github_commit_days'        (highway)
--   daily_project_activity    -> 'daily_project_activity_time' (macondo)
--   daily_user_activity       -> 'daily_user_activity_time'  (horizons, post-handoff)
--   hardware_build            -> 'hardware_build_time_and_journals' (fallout)
--   hackatime / custom        -> hackatime_time / custom_time / both
-- Macondo and Horizons app-native DAU are now email-keyed (the time log's single
-- identity), where the prior program-native models keyed on user_id.
-- ============================================================================

WITH program_dau AS (
    SELECT
        program_name,
        (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
        COUNT(DISTINCT user_email) AS dau,
        CASE
            WHEN BOOL_OR(logging_method = 'github_commit_days') THEN 'github_commit_days'
            WHEN BOOL_OR(logging_method = 'daily_project_activity') THEN 'daily_project_activity_time'
            WHEN BOOL_OR(logging_method = 'daily_user_activity') THEN 'daily_user_activity_time'
            WHEN BOOL_OR(logging_method = 'hardware_build') THEN 'hardware_build_time_and_journals'
            WHEN BOOL_OR(logging_method = 'hackatime') AND BOOL_OR(logging_method = 'custom')
                THEN 'hackatime_and_custom_time'
            WHEN BOOL_OR(logging_method = 'hackatime') THEN 'hackatime_time'
            ELSE 'custom_time'
        END AS dau_methodology
    FROM {{ ref('summer_unified_time_log') }}
    GROUP BY program_name, (activity_hour AT TIME ZONE 'UTC')::date
)

SELECT program_name, activity_date, dau, dau_methodology
FROM program_dau
-- Exclude the in-progress day. activity_date is a UTC date, so the cutoff must
-- also be the UTC date — bare CURRENT_DATE follows the session timezone and, in
-- a session east of UTC, can already be tomorrow-UTC, letting today leak in.
WHERE activity_date < (NOW() AT TIME ZONE 'UTC')::date
ORDER BY activity_date DESC, program_name
