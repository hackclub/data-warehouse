{{ config(
    schema='summer_2026_analytics',
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_summer_2026_dau_deduped ON {{ this }} (activity_date);"
) }}

-- Summer 2026 daily active users, deduped across programs by normalized email.
-- Grain: one row per complete UTC activity date. Current date is excluded because
-- same-day data is incomplete.

WITH coding_active_users AS (
    SELECT DISTINCT
        (activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
        user_email AS user_key
    FROM {{ ref('summer_unified_time_log') }}
    WHERE user_email IS NOT NULL
),

macondo_active_users AS (
    SELECT DISTINCT
        dpa.day::date AS activity_date,
        COALESCE(
            CASE
                WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
                    THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                         || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
                ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1), '')
            END,
            'macondo:' || dpa.user_id
        ) AS user_key
    FROM {{ source('macondo', 'daily_project_activity') }} dpa
    LEFT JOIN {{ source('macondo', 'users') }} u ON u.id = dpa.user_id
    WHERE dpa.day::text ~ '^\d{4}-\d{2}-\d{2}$'
      AND dpa.day::date >= DATE '2026-03-23'
      AND (dpa.hackatime_seconds > 0 OR dpa.journal_seconds > 0)
),

fallout_norm AS (
    SELECT
        id,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(email))) > 0
                THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)), '@', 1), '+', 1)
                     || '@' || SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
            ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(email)), '+', 1), '')
        END AS user_key
    FROM {{ source('fallout', 'users') }}
),

fallout_active_users AS (
    SELECT DISTINCT (l.created_at AT TIME ZONE 'UTC')::date AS activity_date, u.user_key
    FROM {{ source('fallout', 'lookout_timelapses') }} l
    JOIN fallout_norm u ON u.id = l.user_id
    WHERE l.duration > 0 AND (l.created_at AT TIME ZONE 'UTC') <= NOW()
    UNION
    SELECT DISTINCT (l.created_at AT TIME ZONE 'UTC')::date, u.user_key
    FROM {{ source('fallout', 'lapse_timelapses') }} l
    JOIN fallout_norm u ON u.id = l.user_id
    WHERE l.duration > 0 AND (l.created_at AT TIME ZONE 'UTC') <= NOW()
    UNION
    SELECT DISTINCT (j.created_at AT TIME ZONE 'UTC')::date, u.user_key
    FROM {{ source('fallout', 'journal_entries') }} j
    JOIN fallout_norm u ON u.id = j.user_id
    WHERE j.discarded_at IS NULL AND (j.created_at AT TIME ZONE 'UTC') <= NOW()
),

all_active_users AS (
    SELECT activity_date, user_key FROM coding_active_users
    UNION ALL
    SELECT activity_date, user_key FROM macondo_active_users
    UNION ALL
    SELECT activity_date, user_key FROM fallout_active_users
)

SELECT
    activity_date,
    COUNT(DISTINCT user_key) AS dau
FROM all_active_users
WHERE activity_date < CURRENT_DATE
  AND user_key IS NOT NULL
GROUP BY activity_date
ORDER BY activity_date DESC
