{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_dau_deduped ON {{ this }} (activity_date);"
) }}

-- Summer 2026 daily active users, deduped across programs by normalized email.
-- Grain: one row per complete US Eastern (America/New_York) activity date. The
-- in-progress Eastern day is excluded because same-day data is incomplete.
--
-- Every program now flows through summer_unified_time_log (macondo, fallout,
-- highway, and Horizons app-native activity included), so the deduped count is a
-- single COUNT(DISTINCT user_email) over the log. user_email is the same
-- normalized-email / stable-fallback key each source contributes there, so a
-- person who is active in several programs on a day is counted once. This also
-- restores Horizons app-native users (2026-04-22+) to the total, which the prior
-- program-native models left out.

SELECT
    (activity_hour AT TIME ZONE 'America/New_York')::date AS activity_date,
    COUNT(DISTINCT user_email) AS dau
FROM {{ ref('summer_unified_time_log') }}
WHERE user_email IS NOT NULL
  -- Bucket and gate on the US Eastern day; exclude the in-progress Eastern day
  -- (and any future-dated rows) so the latest row is the last complete day.
  AND (activity_hour AT TIME ZONE 'America/New_York')::date < (NOW() AT TIME ZONE 'America/New_York')::date
GROUP BY 1
ORDER BY activity_date DESC
