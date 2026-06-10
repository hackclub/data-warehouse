-- The daily totals mart should have exactly one latest complete date row.

SELECT latest_rows
FROM (
    SELECT COUNT(*) AS latest_rows
    FROM {{ ref('dashboard_daily_totals') }}
    WHERE is_latest_complete_date
) c
WHERE latest_rows <> 1
