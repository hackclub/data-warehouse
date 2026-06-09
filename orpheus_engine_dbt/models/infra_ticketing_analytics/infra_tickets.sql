{{ config(
    schema='infra_ticketing_analytics',
    materialized='table'
) }}

SELECT
    record_id,
    fields ->> 'title' AS title,
    fields ->> 'status' AS status,
    fields ->> 'blocker' AS is_blocker,
    fields ->> 'system' AS system,
    fields ->> 'created_at' AS created_at,
    fields ->> 'status_last_changed_at' AS status_changed_at,
    fields ->> 'feature_request' AS feature_request
FROM {{ source('airtable_raw_all_bases', 'records') }}
WHERE base_id = 'appY8NA5t2YX53RYg'
  AND table_id = 'tblmZhqSMRKDyl0og'
