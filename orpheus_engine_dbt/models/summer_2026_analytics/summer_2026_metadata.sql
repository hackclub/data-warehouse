{{ config(
    schema='summer_2026_analytics',
    materialized='table'
) }}

-- Program metadata for the Summer 2026 dashboard.
-- `last_source_updated_at` is the latest source-level updated/activity timestamp
-- currently present in the warehouse mirror. The mirrored tables do not include a
-- Sling load timestamp, so this is a data freshness signal rather than the exact
-- Dagster/Sling materialization timestamp.

WITH program_info AS (
    SELECT * FROM (VALUES
        ('flavortown', DATE '2025-12-24'),
        ('fallout', DATE '2026-03-01'),
        ('macondo', DATE '2026-03-23'),
        ('beest', DATE '2026-04-06'),
        ('stack', DATE '2026-05-01'),
        ('offtrack', DATE '2026-05-01'),
        ('stardance', DATE '2026-05-31')
    ) AS t(program_name, program_start_date)
),

source_updates AS (
    SELECT 'flavortown'::text AS program_name, MAX(last_updated_at) AS last_source_updated_at
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('flavortown', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('flavortown', 'posts') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('flavortown', 'projects') }}
    ) s

    UNION ALL
    SELECT 'fallout', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('fallout', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('fallout', 'journal_entries') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('fallout', 'lookout_timelapses') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('fallout', 'lapse_timelapses') }}
    ) s

    UNION ALL
    SELECT 'macondo', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('macondo', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('macondo', 'daily_project_activity') }}
    ) s

    UNION ALL
    SELECT 'beest', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('beest', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('beest', 'projects') }}
    ) s

    UNION ALL
    SELECT 'stack', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('stack', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('stack', 'journal_entries') }}
    ) s

    UNION ALL
    SELECT 'offtrack', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('offtrack', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('offtrack', 'journal_entries') }}
    ) s

    UNION ALL
    SELECT 'stardance', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('stardance', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('stardance', 'posts') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('stardance', 'projects') }}
    ) s
)

SELECT
    pi.program_name,
    pi.program_start_date,
    NOW() AS last_materialized_at,
    su.last_source_updated_at,
    CASE
        WHEN su.last_source_updated_at IS NULL THEN 'unknown'
        WHEN su.last_source_updated_at < NOW() - INTERVAL '24 hours' THEN 'stale'
        ELSE 'fresh'
    END AS source_freshness_status
FROM program_info pi
LEFT JOIN source_updates su ON su.program_name = pi.program_name
ORDER BY pi.program_start_date
