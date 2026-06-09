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
--   Coding/journal programs (stardance, flavortown, stack, offtrack, beest) come
--   from {{ ref('summer_unified_time_log') }}, inheriting its cross-program
--   equal-split dedup and run windows.
--   macondo uses its own per-day rollup (daily_project_activity: hackatime_seconds
--   + journal_seconds).
--   fallout (hardware) logs time via build timelapses (lookout + lapse, duration>0)
--   and devlog journals. Ships are intentionally not counted as DAU.
--
-- PROGRAM STATUS:
--   active here : stardance, flavortown (historical baseline), fallout, macondo,
--                 stack, offtrack (pre-launch: 0 until it starts), beest
--   PAUSED      : horizons, stasis — warehouse mirrors stale; re-add once remirrored.
--
-- Identity = normalized email; a person counts once per program per day.
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
      AND dpa.day::date <= CURRENT_DATE
    GROUP BY 1, dpa.day::date
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
)

SELECT program_name, activity_date, dau, dau_methodology FROM coding_dau
UNION ALL
SELECT program_name, activity_date, dau, dau_methodology FROM macondo_dau
UNION ALL
SELECT program_name, activity_date, dau, dau_methodology FROM fallout_dau
ORDER BY activity_date DESC, program_name
