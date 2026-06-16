-- The row-level DAU weights on summer_unified_time_log must aggregate back to the
-- published headcounts (small tolerance for numeric rounding of the fractions):
--   SUM(dau_deduped) per day              == daily_active_users_deduped.dau
--   SUM(dau)         per (program, day)   == daily_active_users.dau
-- A failure means the weighting math broke or identity/NULL handling drifted
-- between the time log and the DAU marts.

WITH log_day AS (
    SELECT (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
           SUM(dau_deduped) AS stacked_dau_deduped
    FROM {{ ref('summer_unified_time_log') }}
    GROUP BY 1
),

day_check AS (
    SELECT
        'day:' || d.activity_date::text AS scope,
        ld.stacked_dau_deduped AS log_value,
        d.dau AS mart_value
    FROM {{ ref('daily_active_users_deduped') }} d
    JOIN log_day ld ON ld.activity_date = d.activity_date
),

log_program_day AS (
    SELECT program_name,
           (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
           SUM(dau) AS stacked_dau
    FROM {{ ref('summer_unified_time_log') }}
    GROUP BY 1, 2
),

program_day_check AS (
    SELECT
        'program_day:' || p.program_name || ':' || p.activity_date::text AS scope,
        lpd.stacked_dau AS log_value,
        p.dau::numeric AS mart_value
    FROM {{ ref('daily_active_users') }} p
    JOIN log_program_day lpd
        ON lpd.program_name = p.program_name
       AND lpd.activity_date = p.activity_date
)

SELECT * FROM day_check     WHERE ABS(log_value - mart_value) > 0.01
UNION ALL
SELECT * FROM program_day_check WHERE ABS(log_value - mart_value) > 0.01
