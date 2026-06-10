-- Dashboard time-series marts should never include the current partial UTC date.

SELECT 'dashboard_daily_totals' AS model_name, activity_date
FROM {{ ref('dashboard_daily_totals') }}
WHERE activity_date >= CURRENT_DATE

UNION ALL

SELECT 'dashboard_program_daily_metrics' AS model_name, activity_date
FROM {{ ref('dashboard_program_daily_metrics') }}
WHERE activity_date >= CURRENT_DATE
