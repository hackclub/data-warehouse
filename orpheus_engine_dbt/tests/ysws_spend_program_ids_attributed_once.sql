/*
    Every Airtable YSWS program that links to a resolvable HCB org must be
    attributed to exactly one spend-program root through member_ids.

    member_ids is the deterministic join key ysws_spend_by_program uses to pull
    approved projects and weighted hours onto a program. If a program's record
    id is missing from every root, its projects and hours silently vanish from
    the rollup; if it appears under two roots, its hours are double counted and
    two programs both look cheaper per hour than they are. Neither failure is
    visible in the totals, so it has to be a test.
*/

WITH linked AS (
    SELECT
        p.id AS program_id,
        regexp_replace(p.hcb, '^https://hcb\.hackclub\.com/([^/?#]+).*$', '\1') AS root_slug
    FROM {{ source('unified_ysws', 'ysws_programs') }} p
    WHERE p.hcb ~ '^https://hcb\.hackclub\.com/'
),

resolvable AS (
    -- ysws_spend_programs can only register a program whose slug is a real,
    -- live org. A link to a soft-deleted org is a different gap, reported by
    -- ysws_unlinked_programs as gap_type = 'org_deleted'; counting it here
    -- would fail this test for a mapping the model is right to skip.
    SELECT l.program_id
    FROM linked l
    JOIN {{ ref('orgs') }} o ON o.slug = l.root_slug
    WHERE NOT o.is_deleted
),

attributed AS (
    SELECT unnest(member_ids) AS program_id, root_slug
    FROM {{ ref('ysws_spend_programs') }}
)

SELECT
    r.program_id,
    COUNT(a.root_slug) AS root_count,
    string_agg(a.root_slug, ', ') AS roots
FROM resolvable r
LEFT JOIN attributed a ON a.program_id = r.program_id
GROUP BY r.program_id
HAVING COUNT(a.root_slug) <> 1
