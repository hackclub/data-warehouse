{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_dashboard_data_health ON {{ this }} (health_sort_key, source_age_hours DESC NULLS FIRST);"
) }}

-- Display-ready data-health rows for the Summer 2026 dashboard.
-- Grain: one row per dashboard input source.

SELECT
    CASE source_freshness_status
        WHEN 'stale' THEN 'Stale'
        WHEN 'lagging' THEN 'Lagging'
        WHEN 'unknown' THEN 'Unknown'
        ELSE 'Fresh'
    END AS health,
    program_name AS source,
    source_type AS type,
    TO_CHAR(last_source_updated_at AT TIME ZONE 'America/New_York', 'Mon FMDD, HH12:MI AM') AS latest_update_et,
    CASE
        WHEN source_age_hours IS NULL THEN NULL
        WHEN source_age_hours < 1 THEN '<1h'
        WHEN source_age_hours < 24 THEN source_age_hours::text || 'h'
        ELSE ROUND(source_age_hours / 24.0, 1)::text || 'd'
    END AS age,
    source_freshness_status,
    source_age_hours,
    last_source_updated_at,
    last_materialized_at,
    CASE source_freshness_status
        WHEN 'stale' THEN 0
        WHEN 'unknown' THEN 1
        WHEN 'lagging' THEN 2
        ELSE 3
    END AS health_sort_key
FROM {{ ref('summer_2026_metadata') }}
ORDER BY health_sort_key, source_age_hours DESC NULLS FIRST, source
