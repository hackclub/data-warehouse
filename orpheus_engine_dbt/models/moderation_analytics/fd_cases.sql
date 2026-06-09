{{ config(
    schema='moderation_analytics',
    materialized='table'
) }}

SELECT
    record_id,
    fields ->> 'Status' AS status,
    (fields ->> 'Created')::timestamptz AS created_at,
    (fields ->> 'Closed at')::timestamptz AS closed_at,
    fields ->> 'content' AS content,
    length(btrim(COALESCE(fields ->> 'content', ''), E'\n\r ')) > 0 AS has_content,
    (fields ->> 'Hang time in hours')::numeric AS hang_time_hours,
    (fields ->> 'reply_time')::numeric AS reply_time_seconds,
    round((fields ->> 'reply_time')::numeric / 60.0, 1) AS reply_time_minutes,
    (fields ->> 'resolve_time')::numeric AS resolve_time_seconds,
    round((fields ->> 'resolve_time')::numeric / 3600.0, 1) AS resolve_time_hours,
    fields ->> 'forwarded_ts' AS forwarded_ts,
    fields ->> 'dm_ts' AS dm_ts,
    fields ->> 'dm_channel' AS dm_channel,
    COALESCE(
        (fields -> 'Thread') ->> 'url',
        fields ->> 'Thread link',
        fields ->> 'link'
    ) AS thread_url,
    COALESCE((fields ->> 'ban')::boolean, false) AS is_ban,
    COALESCE((fields ->> 'bangbang')::boolean, false) AS is_urgent,
    COALESCE((fields ->> 'hourglass')::boolean, false) AS is_pending,
    COALESCE((fields ->> 'white_check_mark')::boolean, false) AS is_resolved_reaction,
    COALESCE((fields ->> 'red-x')::boolean, false) AS is_rejected,
    fields ->> 'selection' AS selection,
    fields ->> 'selection_ts' AS selection_ts,
    fields ->> 'LYLA Records' AS lyla_records,
    CASE
        WHEN (fields ->> 'Hang time in hours')::numeric < 4 THEN '1_fresh'
        WHEN (fields ->> 'Hang time in hours')::numeric < 24 THEN '2_active'
        WHEN (fields ->> 'Hang time in hours')::numeric < 72 THEN '3_warm'
        WHEN (fields ->> 'Hang time in hours')::numeric < 168 THEN '4_aging'
        WHEN (fields ->> 'Hang time in hours')::numeric < 720 THEN '5_old'
        WHEN (fields ->> 'Hang time in hours')::numeric < 2160 THEN '6_stale'
        ELSE '7_ancient'
    END AS hang_time_bucket,
    (fields ->> 'Status') = 'Open'
        AND (fields ->> 'forwarded_ts') IS NOT NULL
        AND length(btrim(COALESCE(fields ->> 'content', ''), E'\n\r ')) > 0
        AS is_genuinely_open,
    _synced_at
FROM {{ source('airtable_raw_all_bases', 'records') }}
WHERE base_id = 'appeRwoAIzrNeKwDr'
  AND table_id = 'tblN1FY4esFqClydq'
