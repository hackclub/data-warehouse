{{ config(
    schema='summer_2026_analytics',
    materialized='table'
) }}

-- Display-ready DAU methodology rows for the Summer 2026 dashboard.
-- Grain: one row per (program_name, dau_methodology).

SELECT DISTINCT
    program_name,
    CASE program_name
        WHEN 'summer_of_making' THEN 'Summer of Making'
        ELSE INITCAP(program_name)
    END AS program,
    dau_methodology,
    CASE dau_methodology
        WHEN 'hackatime_and_custom_time' THEN 'Hackatime + custom time'
        WHEN 'hackatime_time' THEN 'Hackatime time'
        WHEN 'custom_time' THEN 'Custom time'
        WHEN 'daily_project_activity_time' THEN 'Daily project activity time'
        WHEN 'hardware_build_time_and_journals' THEN 'Hardware build time + journals'
        ELSE REPLACE(dau_methodology, '_', ' ')
    END AS methodology
FROM {{ ref('daily_active_users') }}
ORDER BY program_name, methodology
