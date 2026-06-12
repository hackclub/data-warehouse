{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_dashboard_program_daily_metrics ON {{ this }} (program_name, activity_date);"
) }}

-- Chart-ready per-program daily metrics for Summer 2026.
-- Grain: one row per (program_name, complete UTC activity_date).

WITH combined AS (
    SELECT
        COALESCE(d.program_name, h.program_name) AS program_name,
        COALESCE(d.activity_date, h.activity_date) AS activity_date,
        d.dau,
        d.dau_methodology,
        h.hours_logged
    FROM {{ ref('daily_active_users') }} d
    FULL OUTER JOIN {{ ref('daily_hours_logged_by_program') }} h
        ON h.program_name = d.program_name
        AND h.activity_date = d.activity_date
),

bounds AS (
    SELECT MAX(activity_date) AS latest_complete_date
    FROM combined
    WHERE activity_date < CURRENT_DATE
),

methodology_labels AS (
    SELECT * FROM (VALUES
        ('hackatime_and_custom_time', 'Hackatime + custom time'),
        ('hackatime_time', 'Hackatime time'),
        ('custom_time', 'Custom time'),
        ('daily_project_activity_time', 'Daily project activity time'),
        ('hardware_build_time_and_journals', 'Hardware build time + journals')
    ) AS t(dau_methodology, dau_methodology_label)
),

-- Display labels for multi-word program names (INITCAP keeps underscores).
program_labels AS (
    SELECT * FROM (VALUES
        ('summer_of_making', 'Summer of Making'),
        ('hack_club_the_game', 'Hack Club: The Game'),
        ('athena_award', 'Athena Award')
    ) AS t(program_name, program_label)
)

SELECT
    c.program_name,
    COALESCE(pl.program_label, INITCAP(c.program_name)) AS program_label,
    c.activity_date,
    c.dau,
    ROUND(COALESCE(c.hours_logged, 0), 2) AS hours_logged,
    ROUND(COALESCE(c.hours_logged, 0) / NULLIF(c.dau, 0), 2) AS hours_per_active_user,
    c.dau_methodology,
    COALESCE(ml.dau_methodology_label, REPLACE(c.dau_methodology, '_', ' ')) AS dau_methodology_label,
    m.source_freshness_status AS program_source_health,
    m.last_source_updated_at AS program_last_source_updated_at,
    b.latest_complete_date,
    (c.activity_date = b.latest_complete_date) AS is_latest_complete_date,
    (c.activity_date >= b.latest_complete_date - INTERVAL '45 days') AS in_45_day_chart_window,
    (b.latest_complete_date - c.activity_date)::integer AS days_before_latest
FROM combined c
CROSS JOIN bounds b
LEFT JOIN methodology_labels ml ON ml.dau_methodology = c.dau_methodology
LEFT JOIN program_labels pl ON pl.program_name = c.program_name
LEFT JOIN {{ ref('summer_2026_metadata') }} m
    ON m.program_name = c.program_name
    AND m.source_type = 'program db'
WHERE c.activity_date < CURRENT_DATE
ORDER BY c.activity_date DESC, c.program_name
