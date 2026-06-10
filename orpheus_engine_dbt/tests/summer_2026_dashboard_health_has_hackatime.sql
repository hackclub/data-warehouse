-- Hackatime is a shared dependency for several Summer 2026 metrics and should
-- always be visible in the dashboard health table.

SELECT 'missing_hackatime_health_row' AS failure
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('dashboard_data_health') }}
    WHERE source = 'hackatime'
      AND type = 'shared activity source'
)
