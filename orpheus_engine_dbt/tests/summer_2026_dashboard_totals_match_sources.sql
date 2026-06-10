-- Chart marts should preserve the current source-table metric values.

WITH source_latest AS (
    SELECT
        d.activity_date,
        d.dau AS deduped_dau,
        ROUND(SUM(h.hours_logged), 2) AS total_hours_logged
    FROM {{ ref('daily_active_users_deduped') }} d
    JOIN {{ ref('daily_hours_logged_by_program') }} h
        ON h.activity_date = d.activity_date
    WHERE d.activity_date = (
        SELECT MAX(activity_date)
        FROM {{ ref('daily_active_users_deduped') }}
    )
    GROUP BY 1, 2
),

dashboard_latest AS (
    SELECT activity_date, deduped_dau, total_hours_logged
    FROM {{ ref('dashboard_daily_totals') }}
    WHERE is_latest_complete_date
)

SELECT
    s.activity_date,
    s.deduped_dau AS source_deduped_dau,
    d.deduped_dau AS dashboard_deduped_dau,
    s.total_hours_logged AS source_total_hours_logged,
    d.total_hours_logged AS dashboard_total_hours_logged
FROM source_latest s
FULL OUTER JOIN dashboard_latest d ON d.activity_date = s.activity_date
WHERE s.activity_date IS NULL
   OR d.activity_date IS NULL
   OR s.deduped_dau <> d.deduped_dau
   OR s.total_hours_logged <> d.total_hours_logged
