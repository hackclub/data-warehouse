{{ config(
    schema='stardance_analytics',
    materialized='table',
    grants={
        'select': [
            'metabase',
            'metabase_unified_analytics_reader',
            '"stardance-mcp"',
            '"stardance-sankey"'
        ]
    },
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_stardance_journey_user_stages_user_id ON {{ this }} (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_stardance_journey_user_stages_email ON {{ this }} (email)",
        "CREATE INDEX IF NOT EXISTS idx_stardance_journey_user_stages_stage ON {{ this }} (stage_order)",
        "CREATE INDEX IF NOT EXISTS idx_stardance_journey_user_stages_country ON {{ this }} (country_code)"
    ]
) }}

-- ============================================================================
-- Stardance -> YSWS DB journey -- per-user, per-stage long table
--
-- Grain: ONE ROW PER (signup email, stage the email reached). This is the
-- single table to query for the journey. It is the granular form of the
-- funnel Sankey: a user appears once for every stage they reached, so the
-- diagram / funnel is just an aggregation of this table.
--
-- Population: stardance.materialized_all_signups -- one row per distinct email
-- ever entered into stardance.hackclub.com (RSVPs + users; fraud/banned already
-- excluded). Emails are LEFT JOINed to a deduped stardance.users row, so
-- user_id is NULL only at stage 1 for RSVP-only emails that never became a user.
--
-- How to use it:
--   * Node / funnel size at a stage : COUNT(*)            WHERE stage_code = '10'
--   * Per-country funnel            : ... WHERE country_code = 'US' (or GROUP BY)
--   * Sankey nodes                  : SELECT stage_label, COUNT(*) GROUP BY 1,2 ...
--   * Sankey edges (flows)          : self-join consecutive stages per user, or
--                                     split a stage's population on the branch
--                                     flags below (known_email / verified_during
--                                     / already_verified) -- see notes per stage.
--   * Detailed / drill-down         : JOIN user_id -> stardance.users (or any
--                                     user-keyed table), or JOIN email across
--                                     systems. Filter to a single stage first to
--                                     avoid fan-out (a user has many stage rows).
--
-- Stages (stage_order / stage_code / stage_label) and membership condition:
--    1  1    Enter email on stardance.hackclub.com        every signup email
--    2  2A   Complete auth.hackclub.com (email known)     known_email AND auth_done
--    3  3    Complete Stardance onboarding                onboarded
--    4  4    Create a Stardance project                   + created a project
--    5  5B   Create account on auth.hackclub.com (new)    project AND new email AND auth
--    6  6    Send >=1 Hackatime heartbeat                 + >=1 heartbeat since signup
--    7  7    Link Hackatime to Stardance                  + linked Hackatime identity
--    8  8    Log >=15 min on Stardance project            + a project with >=900s
--    9  9    Link Hackatime project to Stardance project  + HT project -> SD project
--   10  10   Post >=1 devlog                              + a non-deleted devlog
--   11  11C  Verify identity on auth.hackclub.com         devlog AND verified_during
--   12  12   Place a free sticker order                   + a non-rejected free sticker order
--   13  13   Ship a project                               + a ship event
--   14  14   Project is ship certified                    + ship event approved
--   16  16   Project in the YSWS DB                        + approved Stardance row in YSWS DB
--
-- Branch flags carried on every row (the two-path splits in the Sankey):
--   known_email      : email was in Loops before first hit Stardance
--                      (auth at step 2A vs. new-account at step 5B)
--   verified_during  : verified identity >1h after account creation (-> 11C)
--   already_verified : was verified at (or within 1h of) account creation,
--                      so the user skips 11C and flows 10 -> 12 directly
--
-- Country columns carried on every row (filter / segment the whole funnel):
--   country_code : ISO 3166-1 alpha-2 from the signup-time IP geocode in
--                  materialized_all_signups (RSVP geocode for RSVP-first
--                  emails, account-creation geocode otherwise). ~99% coverage.
--   country      : display name from the same source. Falls back to the code
--                  itself where the app has no name mapping.
-- ============================================================================

WITH u1 AS (
    -- one non-banned user row per email (a handful of emails map to 2 users)
    SELECT DISTINCT ON (LOWER(email))
        id, email, created_at, onboarded_at, verification_status
    FROM {{ source('stardance', 'users') }}
    WHERE NOT banned
    ORDER BY LOWER(email), created_at
),

idents AS (
    SELECT user_id,
        BOOL_OR(provider = 'hack_club') AS auth_done,
        BOOL_OR(provider = 'hackatime') AS ht_linked
    FROM {{ source('stardance', 'user_identities') }}
    GROUP BY 1
),

-- >=1 Hackatime heartbeat on/after the user's Stardance signup date
hb AS (
    SELECT DISTINCT i.user_id
    FROM {{ source('stardance', 'user_identities') }} i
    JOIN {{ source('stardance', 'users') }} u ON u.id = i.user_id
    JOIN {{ ref('daily_activity') }} da
        ON da.hackatime_user_id::text = i.uid
        AND da.activity_date >= u.created_at::date
    WHERE i.provider = 'hackatime'
),

proj AS (
    SELECT pm.user_id,
        TRUE AS has_project,
        BOOL_OR(p.duration_seconds >= 900) AS min15
    FROM {{ source('stardance', 'project_memberships') }} pm
    JOIN {{ source('stardance', 'projects') }} p ON p.id = pm.project_id AND p.deleted_at IS NULL
    GROUP BY 1
),

hp AS (  -- Hackatime project linked to a Stardance project
    SELECT DISTINCT user_id
    FROM {{ source('stardance', 'user_hackatime_projects') }}
    WHERE project_id IS NOT NULL
),

dl AS (  -- posted >=1 (non-deleted) devlog
    SELECT DISTINCT po.user_id
    FROM {{ source('stardance', 'posts') }} po
    JOIN {{ source('stardance', 'post_devlogs') }} d ON d.id = po.postable_id
    WHERE po.postable_type = 'Post::Devlog' AND d.deleted_at IS NULL
),

st AS (  -- placed a free sticker order
    SELECT DISTINCT o.user_id
    FROM {{ source('stardance', 'shop_orders') }} o
    JOIN {{ source('stardance', 'shop_items') }} si ON si.id = o.shop_item_id
    WHERE si.type = 'ShopItem::FreeStickers'
        AND o.aasm_state NOT IN ('rejected', 'refunded')
),

sh AS (  -- shipped / ship-certified
    SELECT po.user_id,
        TRUE AS shipped,
        BOOL_OR(se.certification_status = 'approved') AS ship_certified
    FROM {{ source('stardance', 'posts') }} po
    JOIN {{ source('stardance', 'post_ship_events') }} se ON se.id = po.postable_id
    WHERE po.postable_type = 'Post::ShipEvent'
    GROUP BY 1
),

-- first time each user transitioned to 'verified' (PaperTrail audit log)
first_verified AS (
    SELECT (item_id)::bigint AS user_id, MIN(created_at) AS verified_at
    FROM {{ source('stardance', 'versions') }}
    WHERE item_type = 'User'
        AND object_changes ? 'verification_status'
        AND object_changes->'verification_status'->>1 = 'verified'
    GROUP BY 1
),

-- emails with an approved Stardance project in the actual unified YSWS DB
ysws AS (
    SELECT DISTINCT LOWER(p.email_trimmed_lowercased) AS email
    FROM {{ source('unified_ysws', 'approved_projects') }} p
    JOIN {{ source('unified_ysws', 'approved_projects__ysws_name') }} n
        ON n._dlt_parent_id = p._dlt_id
    WHERE n.value = 'Stardance'
),

flags AS (
    SELECT
        s.email,
        u.id                                                                   AS user_id,
        -- signup-time IP geocode (already unified across RSVPs + users)
        s.country_code                                                         AS country_code,
        s.country                                                              AS country,
        -- email already known to Hack Club before it first hit Stardance (Loops)
        (l.email IS NOT NULL AND l.sign_up_at < s.first_seen_at_utc::date)     AS known_email,
        COALESCE(i.auth_done, FALSE)                                           AS auth_done,
        COALESCE(i.ht_linked, FALSE)                                           AS ht_linked,
        (u.verification_status = 'verified'
            AND (fv.user_id IS NULL
                OR fv.verified_at <= u.created_at + INTERVAL '1 hour'))        AS already_verified,
        (u.verification_status = 'verified'
            AND fv.verified_at > u.created_at + INTERVAL '1 hour')             AS verified_during,
        (u.onboarded_at IS NOT NULL)                                           AS onboarded,
        COALESCE(p.has_project, FALSE)                                         AS has_project,
        COALESCE(p.min15, FALSE)                                               AS min15,
        (hb.user_id IS NOT NULL)                                               AS heartbeat,
        (hp.user_id IS NOT NULL)                                               AS proj_linked,
        (dl.user_id IS NOT NULL)                                               AS devlog,
        (st.user_id IS NOT NULL)                                               AS sticker,
        COALESCE(sh.shipped, FALSE)                                            AS shipped,
        COALESCE(sh.ship_certified, FALSE)                                     AS ship_certified,
        (y.email IS NOT NULL)                                                  AS in_ysws_db
    FROM {{ source('stardance', 'materialized_all_signups') }} s
    LEFT JOIN u1 u              ON LOWER(u.email) = LOWER(s.email)
    LEFT JOIN {{ ref('hack_clubbers') }} l ON LOWER(l.email) = LOWER(s.email)
    LEFT JOIN idents i          ON i.user_id  = u.id
    LEFT JOIN hb                ON hb.user_id = u.id
    LEFT JOIN proj p            ON p.user_id  = u.id
    LEFT JOIN hp                ON hp.user_id = u.id
    LEFT JOIN dl                ON dl.user_id = u.id
    LEFT JOIN st                ON st.user_id = u.id
    LEFT JOIN sh                ON sh.user_id = u.id
    LEFT JOIN first_verified fv ON fv.user_id = u.id
    LEFT JOIN ysws y            ON y.email    = LOWER(s.email)
),

cum AS (
    SELECT *,
        onboarded                                                                                  AS m3,
        onboarded AND has_project                                                                  AS m4,
        onboarded AND has_project AND heartbeat                                                    AS m6,
        onboarded AND has_project AND heartbeat AND ht_linked                                      AS m7,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15                            AS m8,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15 AND proj_linked            AS m9,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15 AND proj_linked AND devlog AS m10,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15 AND proj_linked AND devlog
            AND sticker                                                                            AS m12,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15 AND proj_linked AND devlog
            AND sticker AND shipped                                                                AS m13,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15 AND proj_linked AND devlog
            AND sticker AND shipped AND ship_certified                                             AS m14,
        onboarded AND has_project AND heartbeat AND ht_linked AND min15 AND proj_linked AND devlog
            AND sticker AND shipped AND ship_certified AND in_ysws_db                              AS m16
    FROM flags
)

-- Explode each user into one row per stage they reached (single pass).
SELECT
    f.email,
    f.user_id,
    -- country of the signup (filter the whole funnel on these)
    f.country_code,
    f.country,
    v.stage_order,
    v.stage_code,
    v.stage_label,
    -- branch flags for reconstructing the two-path splits / filtering
    f.known_email,
    f.auth_done,
    f.verified_during,
    f.already_verified,
    f.in_ysws_db
FROM cum f
CROSS JOIN LATERAL (
    VALUES
        (1,  '1',   '1. Enter email on stardance.hackclub.com',         TRUE),
        (2,  '2A',  '2A. Complete auth.hackclub.com (email known)',     f.known_email AND f.auth_done),
        (3,  '3',   '3. Complete Stardance onboarding',                 f.m3),
        (4,  '4',   '4. Create a Stardance project',                    f.m4),
        (5,  '5B',  '5B. Create account on auth.hackclub.com (new)',    f.m4 AND NOT f.known_email AND f.auth_done),
        (6,  '6',   '6. Send >=1 Hackatime heartbeat',                  f.m6),
        (7,  '7',   '7. Link Hackatime to Stardance',                   f.m7),
        (8,  '8',   '8. Log >=15 min on Stardance project',             f.m8),
        (9,  '9',   '9. Link Hackatime project to Stardance project',   f.m9),
        (10, '10',  '10. Post >=1 devlog',                              f.m10),
        (11, '11C', '11C. Verify identity on auth.hackclub.com',        f.m10 AND f.verified_during),
        (12, '12',  '12. Place a free sticker order',                   f.m12),
        (13, '13',  '13. Ship a project',                               f.m13),
        (14, '14',  '14. Project is ship certified',                    f.m14),
        (16, '16',  '16. Project in the YSWS DB',                       f.m16)
) AS v(stage_order, stage_code, stage_label, reached)
WHERE v.reached
ORDER BY f.email, v.stage_order
