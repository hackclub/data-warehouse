{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_hours_by_program ON {{ this }} (program_name, activity_date);"
) }}

-- Summer 2026 logged hours by program.
-- Grain: one row per (program_name, activity_date) in UTC.
--
-- Coding/work-session programs use credited_hours_logged from the unified time
-- log so overlapping Hackatime sessions are split consistently with DAU.
-- Macondo and Fallout use their program-native daily activity/timelapse sources.
-- Highway is intentionally ABSENT: its DAU is GitHub-commit-day based (commits
-- carry no durations), and the only duration data is reviewer-approved totals
-- (no daily grain) plus a ~37%-coverage Hackatime linkage that would understate
-- hours ~2x. The dashboard mart COALESCEs missing hours to 0.
-- Neighborhood, Construct, and Shiba are included through the unified log:
-- Neighborhood via Hackatime alias claims, Construct via capped devlog minutes,
-- Shiba via capped Airtable creator posts + logged-in play telemetry, and
-- Midnight via Hackatime alias claims. Horizons uses the unified log before
-- its app-native daily rollup begins on 2026-04-22, then qualified
-- user_daily_activity seconds from there.
-- Current date is excluded because same-day data is incomplete.

WITH coding_hours AS (
    SELECT
        program_name,
        (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
        SUM(credited_hours_logged)::numeric AS hours_logged
    FROM {{ ref('summer_unified_time_log') }}
    WHERE program_name <> 'horizons'
       OR (activity_hour AT TIME ZONE 'UTC')::date < DATE '2026-04-22'
    GROUP BY 1, 2
),

macondo_hours AS (
    SELECT
        'macondo'::text AS program_name,
        dpa.day::date AS activity_date,
        SUM(COALESCE(dpa.hackatime_seconds, 0) + COALESCE(dpa.journal_seconds, 0))::numeric / 3600.0 AS hours_logged
    FROM {{ source('macondo', 'daily_project_activity') }} dpa
    WHERE dpa.day::text ~ '^\d{4}-\d{2}-\d{2}$'
      AND dpa.day::date >= DATE '2026-03-23'
      AND dpa.day::date < CURRENT_DATE
    GROUP BY 1, 2
),

horizons_hours AS (
    SELECT
        'horizons'::text AS program_name,
        uda.local_date::date AS activity_date,
        SUM(uda.seconds)::numeric / 3600.0 AS hours_logged
    FROM {{ source('horizons', 'user_daily_activity') }} uda
    WHERE uda.local_date::date >= DATE '2026-04-22'
      AND uda.local_date::date < CURRENT_DATE
      AND uda.qualified
      AND uda.seconds > 0
    GROUP BY 1, uda.local_date::date
),

fallout_hours AS (
    SELECT
        'fallout'::text AS program_name,
        activity_date,
        SUM(hours_logged)::numeric AS hours_logged
    FROM (
        SELECT
            (created_at AT TIME ZONE 'UTC')::date AS activity_date,
            duration::numeric / 3600.0 AS hours_logged
        FROM {{ source('fallout', 'lookout_timelapses') }}
        WHERE duration > 0
          AND (created_at AT TIME ZONE 'UTC') <= NOW()

        UNION ALL

        SELECT
            (created_at AT TIME ZONE 'UTC')::date AS activity_date,
            duration::numeric / 3600.0 AS hours_logged
        FROM {{ source('fallout', 'lapse_timelapses') }}
        WHERE duration > 0
          AND (created_at AT TIME ZONE 'UTC') <= NOW()

        UNION ALL

        SELECT
            (created_at AT TIME ZONE 'UTC')::date AS activity_date,
            burnout_duration_seconds::numeric / 3600.0 AS hours_logged
        FROM {{ source('fallout', 'journal_entries') }}
        WHERE discarded_at IS NULL
          AND burnout_duration_seconds > 0
          AND (created_at AT TIME ZONE 'UTC') <= NOW()
    ) h
    WHERE activity_date >= DATE '2026-03-01'
      AND activity_date < CURRENT_DATE
    GROUP BY 1, 2
),

combined AS (
    SELECT program_name, activity_date, hours_logged FROM coding_hours
    UNION ALL
    SELECT program_name, activity_date, hours_logged FROM macondo_hours
    UNION ALL
    SELECT program_name, activity_date, hours_logged FROM horizons_hours
    UNION ALL
    SELECT program_name, activity_date, hours_logged FROM fallout_hours
)

SELECT
    program_name,
    activity_date,
    ROUND(SUM(hours_logged), 2) AS hours_logged
FROM combined
WHERE activity_date < CURRENT_DATE
GROUP BY 1, 2
ORDER BY activity_date DESC, program_name
