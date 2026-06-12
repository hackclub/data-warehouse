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

-- Highway: commit days on submitted repos; see daily_active_users.highway_dau
-- for the methodology and quality-control audit.
highway_norm AS (
    SELECT
        CASE WHEN POSITION('@' IN LOWER(BTRIM(r.fields->>'Email'))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 2)
             ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '+', 1), '')
        END AS user_key,
        LOWER((REGEXP_MATCH(r.fields->>'Github_Url',
                            'github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)'))[1])
        || '/' ||
        REGEXP_REPLACE(
            LOWER((REGEXP_MATCH(r.fields->>'Github_Url',
                                'github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)'))[2]),
            '\.git$', '') AS repo_key
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    WHERE r.base_id = 'appuDQSHCdCHyOrxw'
      AND r.table_id = 'tbl9QnZ320NTGJHJj'
      AND COALESCE(r.fields->>'Status', '') <> 'Purged'
      AND r.fields->>'Github_Url' ~* 'github\.com/'
),

highway_active_users AS (
    SELECT DISTINCT
        (c.authored_at AT TIME ZONE 'UTC')::date AS activity_date,
        h.user_key
    FROM highway_norm h
    JOIN {{ source('highway_github', 'repos') }} gr
        ON gr.repo_key = h.repo_key
        AND gr.scrape_status = 'ok'
    JOIN {{ source('highway_github', 'commits') }} c ON c.repo_key = h.repo_key
    WHERE h.user_key IS NOT NULL
      AND (c.authored_at AT TIME ZONE 'UTC') <= NOW()
      AND (c.authored_at AT TIME ZONE 'UTC')::date >= DATE '2025-05-01'
      AND (c.authored_at AT TIME ZONE 'UTC')::date < DATE '2025-11-01'
),

all_active_users AS (
    SELECT activity_date, user_key FROM coding_active_users
    UNION ALL
    SELECT activity_date, user_key FROM macondo_active_users
    UNION ALL
    SELECT activity_date, user_key FROM fallout_active_users
    UNION ALL
    SELECT activity_date, user_key FROM highway_active_users
)

SELECT
    activity_date,
    COUNT(DISTINCT user_key) AS dau
FROM all_active_users
WHERE activity_date < CURRENT_DATE
  AND user_key IS NOT NULL
GROUP BY activity_date
ORDER BY activity_date DESC
