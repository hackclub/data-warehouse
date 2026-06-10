-- Dashboard marts should keep their documented grain.

SELECT 'dashboard_daily_totals' AS model_name, activity_date, NULL::text AS program_name
FROM {{ ref('dashboard_daily_totals') }}
GROUP BY activity_date
HAVING COUNT(*) > 1

UNION ALL

SELECT 'dashboard_program_daily_metrics' AS model_name, activity_date, program_name
FROM {{ ref('dashboard_program_daily_metrics') }}
GROUP BY activity_date, program_name
HAVING COUNT(*) > 1

UNION ALL

SELECT 'dashboard_data_health' AS model_name, NULL::date AS activity_date, source AS program_name
FROM {{ ref('dashboard_data_health') }}
GROUP BY source
HAVING COUNT(*) > 1
