{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_dashboard_daily_totals ON {{ this }} (activity_date);"
) }}

-- Chart-ready daily dashboard totals for Summer 2026.
-- Grain: one row per complete UTC activity date.
--
-- This model centralizes the rolling-window and latest-date flags that the
-- Metabase dashboard needs, so chart SQL can stay as simple SELECTs.

WITH daily_hours AS (
    SELECT
        activity_date,
        SUM(hours_logged)::numeric AS total_hours_logged
    FROM {{ ref('daily_hours_logged_by_program') }}
    GROUP BY 1
),

program_dau AS (
    SELECT
        activity_date,
        SUM(dau)::integer AS program_reported_dau,
        COUNT(DISTINCT program_name) AS active_programs
    FROM {{ ref('daily_active_users') }}
    GROUP BY 1
),

combined AS (
    SELECT
        COALESCE(d.activity_date, h.activity_date, p.activity_date) AS activity_date,
        d.dau AS deduped_dau,
        p.program_reported_dau,
        p.active_programs,
        ROUND(COALESCE(h.total_hours_logged, 0), 2) AS total_hours_logged
    FROM {{ ref('daily_active_users_deduped') }} d
    FULL OUTER JOIN daily_hours h ON h.activity_date = d.activity_date
    FULL OUTER JOIN program_dau p ON p.activity_date = COALESCE(d.activity_date, h.activity_date)
),

bounds AS (
    SELECT MAX(activity_date) AS latest_complete_date
    FROM combined
    WHERE activity_date < CURRENT_DATE
)

SELECT
    c.activity_date,
    c.deduped_dau,
    c.program_reported_dau,
    c.active_programs,
    c.total_hours_logged,
    ROUND(c.total_hours_logged / NULLIF(c.deduped_dau, 0), 2) AS hours_per_deduped_active_user,
    b.latest_complete_date,
    (c.activity_date = b.latest_complete_date) AS is_latest_complete_date,
    (c.activity_date >= b.latest_complete_date - INTERVAL '45 days') AS in_45_day_chart_window,
    (b.latest_complete_date - c.activity_date)::integer AS days_before_latest
FROM combined c
CROSS JOIN bounds b
WHERE c.activity_date < CURRENT_DATE
ORDER BY c.activity_date DESC
