{{ config(
    schema='summer_2026_analytics',
    materialized='table'
) }}

-- Program metadata for the Summer 2026 dashboard.
-- `last_source_updated_at` is the latest source-level updated/activity timestamp
-- currently present in the warehouse mirror. The mirrored tables do not include a
-- Sling load timestamp, so this is a data freshness signal rather than the exact
-- Dagster/Sling materialization timestamp.

WITH source_info AS (
    SELECT * FROM (VALUES
        ('hackatime', 'shared activity source', NULL::date),
        ('athena_award', 'program db', DATE '2025-05-21'),
        ('summer_of_making', 'program db', DATE '2025-06-16'),
        ('shipwrecked', 'program db', DATE '2025-05-28'),
        ('blueprint', 'program db', DATE '2025-09-23'),
        ('flavortown', 'program db', DATE '2025-12-24'),
        ('hack_club_the_game', 'program db', DATE '2026-01-16'),
        ('fallout', 'program db', DATE '2026-03-01'),
        ('stasis', 'program db', DATE '2026-03-03'),
        ('horizons', 'program db', DATE '2026-02-22'),
        ('macondo', 'program db', DATE '2026-03-23'),
        ('beest', 'program db', DATE '2026-04-06'),
        ('stack', 'program db', DATE '2026-05-01'),
        ('offtrack', 'program db', DATE '2026-05-01'),
        ('stardance', 'program db', DATE '2026-05-31')
    ) AS t(program_name, source_type, program_start_date)
),

source_updates AS (
    SELECT 'hackatime'::text AS program_name, MAX(updated_at)::timestamptz AS last_source_updated_at
    FROM {{ source('hackatime_raw', 'heartbeats') }}
    WHERE category = 'coding'

    UNION ALL
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

    UNION ALL
    SELECT 'stasis', MAX(last_updated_at)
    FROM (
        SELECT MAX("updatedAt")::timestamptz AS last_updated_at FROM {{ source('stasis', 'user') }}
        UNION ALL
        SELECT MAX("updatedAt")::timestamptz FROM {{ source('stasis', 'project') }}
        UNION ALL
        SELECT MAX("createdAt")::timestamptz FROM {{ source('stasis', 'work_session') }}
        UNION ALL
        SELECT MAX("createdAt")::timestamptz FROM {{ source('stasis', 'hackatime_project') }}
    ) s

    UNION ALL
    SELECT 'horizons', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('horizons', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('horizons', 'projects') }}
        UNION ALL
        SELECT MAX(created_at)::timestamptz FROM {{ source('horizons', 'user_sessions') }}
    ) s

    UNION ALL
    SELECT 'blueprint', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('blueprint', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('blueprint', 'projects') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('blueprint', 'journal_entries') }}
    ) s

    UNION ALL
    SELECT 'hack_club_the_game', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('hack_club_the_game', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('hack_club_the_game', 'projects') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('hack_club_the_game', 'hackatime_projects') }}
    ) s

    -- SoM 2025 ended 2025-10-02, but the app is still live and its mirror still
    -- syncs, so users/projects updated_at reflects mirror freshness (not
    -- program activity).
    UNION ALL
    SELECT 'summer_of_making', MAX(last_updated_at)
    FROM (
        SELECT MAX(updated_at)::timestamptz AS last_updated_at FROM {{ source('summer_of_making_2025', 'users') }}
        UNION ALL
        SELECT MAX(updated_at)::timestamptz FROM {{ source('summer_of_making_2025', 'projects') }}
    ) s

    -- Athena Award ended ~2026-02 and its backend is the Airtable base itself
    -- (no app db), so base rows no longer change; the dlt load time is the
    -- mirror-freshness signal instead.
    UNION ALL
    SELECT 'athena_award', MAX(inserted_at)::timestamptz
    FROM {{ source('airtable_athena_award', '_dlt_loads') }}

    -- Shipwrecked ended 2025-09-03, but the app is still live and its mirror
    -- still syncs, so these timestamps reflect mirror freshness (not program
    -- activity). Project has no timestamp columns, so links stand in for it.
    UNION ALL
    SELECT 'shipwrecked', MAX(last_updated_at)
    FROM (
        SELECT MAX("updatedAt")::timestamptz AS last_updated_at FROM {{ source('shipwrecked_the_bay', 'User') }}
        UNION ALL
        SELECT MAX("createdAt")::timestamptz FROM {{ source('shipwrecked_the_bay', 'HackatimeProjectLink') }}
    ) s
)

SELECT
    si.program_name,
    si.source_type,
    si.program_start_date,
    NOW() AS last_materialized_at,
    su.last_source_updated_at,
    ROUND(EXTRACT(EPOCH FROM (NOW() - su.last_source_updated_at)) / 3600.0, 1) AS source_age_hours,
    CASE
        WHEN su.last_source_updated_at IS NULL THEN 'unknown'
        WHEN si.program_name = 'hackatime' AND su.last_source_updated_at < NOW() - INTERVAL '6 hours' THEN 'stale'
        WHEN si.program_name = 'hackatime' AND su.last_source_updated_at < NOW() - INTERVAL '2 hours' THEN 'lagging'
        WHEN su.last_source_updated_at < NOW() - INTERVAL '24 hours' THEN 'stale'
        WHEN su.last_source_updated_at < NOW() - INTERVAL '6 hours' THEN 'lagging'
        ELSE 'fresh'
    END AS source_freshness_status
FROM source_info si
LEFT JOIN source_updates su ON su.program_name = si.program_name
ORDER BY si.source_type, si.program_start_date NULLS FIRST
