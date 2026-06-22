{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_hours_by_program ON {{ this }} (program_name, activity_date);"
) }}

-- Summer 2026 logged hours by program.
-- Grain: one row per (program_name, activity_date) in UTC.
--
-- All programs now flow through summer_unified_time_log, so credited hours come
-- from it alone (overlapping Hackatime sessions are split consistently with DAU,
-- and macondo / fallout / Horizons app-native seconds are unioned in there).
-- Highway is intentionally ABSENT from this mart: its activity is GitHub
-- commit days with no durations, so its time-log rows carry 0 hours and the
-- HAVING SUM > 0 below drops them (the dashboard mart COALESCEs missing hours to
-- 0). The same HAVING drops fallout days whose only activity was a journal with
-- no logged duration, preserving the prior no-zero-row behavior.
-- Current date is excluded because same-day data is incomplete.

SELECT
    program_name,
    (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
    ROUND(SUM(credited_hours_logged), 2) AS hours_logged
FROM {{ ref('summer_unified_time_log') }}
-- UTC cutoff to match the UTC activity_date (bare CURRENT_DATE would follow the
-- session timezone and could let the in-progress day leak in east of UTC).
WHERE (activity_hour AT TIME ZONE 'UTC')::date < (NOW() AT TIME ZONE 'UTC')::date
GROUP BY 1, 2
HAVING SUM(credited_hours_logged) > 0
ORDER BY activity_date DESC, program_name
