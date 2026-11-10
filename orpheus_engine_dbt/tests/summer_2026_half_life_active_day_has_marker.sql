{{ config(severity='warn') }}

-- Half Life's DAU is a per-day rule (>=15 minutes), not row presence, so a day whose
-- journalled hours clear the bar must also carry the `daily_user_activity` marker that
-- rule keys on. A day at or over the bar with no marker means the marker and the hours
-- were placed on different days — the drift `half_life_sessions` exists to prevent —
-- and the person would drop out of DAU while their hours stayed in the hours series.
--
-- Warn rather than error: the rule also ignores entries on a soft-deleted project
-- (exactly as the app's rebuild does), and this table cannot tell those rows apart, so
-- such a user-day is a legitimate miss. Investigate anything that fires here that is
-- not that case — it is the shape of the bug this model was fixed for.

SELECT
    program_name,
    user_email,
    activity_date,
    ROUND(SUM(raw_hours_logged), 2) AS hours_logged
FROM {{ ref('summer_unified_time_log') }}
WHERE program_name = 'half_life'
GROUP BY 1, 2, 3
HAVING SUM(raw_hours_logged) >= 0.25
   AND COUNT(*) FILTER (WHERE logging_method = 'daily_user_activity') = 0
