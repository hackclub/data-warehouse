{{ config(
    schema='finance_2026_analytics',
    materialized='table'
) }}

SELECT
  internal_name AS donor,
  amount,
  COALESCE(
    CASE
      WHEN NULLIF(received_at, '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        THEN TO_DATE(received_at, 'MM/DD/YYYY')
      WHEN NULLIF(received_at, '') ~ '^\d{1,2}/\d{1,2}/\d{2}$'
        THEN TO_DATE(received_at, 'MM/DD/YY')
    END,
    CASE
      WHEN NULLIF(leadership_flagged_donation_at, '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        THEN TO_DATE(leadership_flagged_donation_at, 'MM/DD/YYYY')
      WHEN NULLIF(leadership_flagged_donation_at, '') ~ '^\d{1,2}/\d{1,2}/\d{2}$'
        THEN TO_DATE(leadership_flagged_donation_at, 'MM/DD/YY')
    END
  ) AS date,
  CASE
    WHEN NULLIF(received_at, '') IS NOT NULL THEN 'Received'
    ELSE 'Awaiting Receipt'
  END AS status,
  donor_origin AS country,
  _fivetran_synced AS source_synced_at
FROM {{ source('finance_2026', 'major_gifts') }}
-- Fivetran syncs trailing blank spreadsheet rows (and stray keystrokes) as real
-- rows; a gift needs at least a donor and an amount to be usable downstream.
WHERE NULLIF(BTRIM(internal_name), '') IS NOT NULL
  AND amount IS NOT NULL
ORDER BY date DESC
