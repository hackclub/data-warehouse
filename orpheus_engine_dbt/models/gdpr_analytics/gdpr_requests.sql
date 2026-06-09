{{ config(
    schema='gdpr_analytics',
    materialized='table'
) }}

SELECT
    record_id,
    fields ->> 'Status' AS status,
    fields ->> 'Request Type' AS request_type,
    fields ->> 'Submitted At' AS submitted_at,
    fields ->> '_calculated_deadline' AS deadline,
    (fields ->> '_over_deadline')::integer AS over_deadline,
    (fields ->> '_extension_days')::integer AS extension_days,
    fields ->> 'Resolution' AS resolution
FROM {{ source('airtable_raw_all_bases', 'records') }}
WHERE base_id = 'appMHGAPAdo6MbXAu'
  AND table_id = 'tblQ9p8xFi4PwQwup'
