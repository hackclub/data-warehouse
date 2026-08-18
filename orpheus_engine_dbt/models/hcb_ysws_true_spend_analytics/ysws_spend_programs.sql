{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS spend program registry — one row per canonical program ROOT HCB org.

    Roots come from two places:
    1. Airtable ysws_programs rows with an HCB link (hcb URL -> slug).
    2. Manual entries for programs whose money lives in HCB but that have no
       (correct) HCB link in Airtable:
         - Outpost, whose fund is the stardance-hardware org (a sub-org of
           stardance that actually holds a different program's money)

    Airtable can contain both an umbrella org and one or more programs whose
    HCB orgs are below that umbrella (for example a flagship or grant-
    distribution org).  Those satellites must not become second spend-tree
    roots, or they get carved out of the umbrella they belong to.

    Rolling up on HCB nesting ALONE would be wrong: unrelated programs are
    routinely banked under another program's org (Sleepover under Athena) or
    under a personal reviewer pot (Ceiling under ysws-budget-tongyu). So a
    satellite must be nested AND slug-branded as a child of the umbrella
    (campfire-flagship under campfire), and personal author/reviewer pots are
    never valid attribution roots. Anything else stays its own root, which is
    the pre-existing behaviour.

    Descendant Airtable names are retained in member_names so approved-project
    hour attribution is unchanged.

    manual_members attaches extra orgs to a program's tree when they are
    related but NOT HCB sub-orgs of the root (no parent_id link), e.g.
    som-sticker-shipments is Summer of Making fulfillment funded by HQ.

    Grain: root_event_id. If several Airtable rows point at the same HCB org,
    their names are aggregated into one row.
*/

WITH RECURSIVE airtable_roots AS (
    SELECT
        p.name AS program_name,
        regexp_replace(p.hcb, '^https://hcb\.hackclub\.com/([^/?#]+).*$', '\1') AS root_slug,
        'program' AS bucket
    FROM {{ source('unified_ysws', 'ysws_programs') }} p
    WHERE p.hcb ~ '^https://hcb\.hackclub\.com/'
),

manual_roots (program_name, root_slug, bucket) AS (
    VALUES
        -- Umbrella label: Airtable only knows this org as "Campfire Satellites",
        -- but the tree also holds the flagship, so keep the umbrella name.
        ('Campfire', 'campfire', 'program'),
        ('Outpost', 'stardance-hardware', 'program'),
        ('Marketing', 'ysws-marketing', 'marketing')
),

all_roots AS (
    SELECT program_name, root_slug, bucket, 'airtable' AS source FROM airtable_roots
    UNION ALL
    SELECT program_name, root_slug, bucket, 'manual' AS source FROM manual_roots
),

resolved AS (
    SELECT
        o.event_id AS root_event_id,
        o.slug AS root_slug,
        o.name AS root_org_name,
        r.program_name,
        r.bucket,
        r.source
    FROM all_roots r
    JOIN {{ ref('orgs') }} o ON o.slug = r.root_slug
),

-- Walk upward from every registered root. A registered ancestor is a more
-- authoritative attribution point than the descendant root itself.
root_ancestors AS (
    SELECT
        r.root_event_id AS member_event_id,
        r.root_event_id AS ancestor_event_id,
        e.parent_id,
        0 AS depth
    FROM resolved r
    JOIN {{ source('hcb', 'events') }} e ON e.id = r.root_event_id

    UNION ALL

    SELECT
        a.member_event_id,
        e.id AS ancestor_event_id,
        e.parent_id,
        a.depth + 1
    FROM root_ancestors a
    JOIN {{ source('hcb', 'events') }} e ON e.id = a.parent_id
),

canonical_roots AS (
    SELECT DISTINCT ON (a.member_event_id)
        a.member_event_id,
        ancestor.root_event_id AS canonical_root_event_id
    FROM root_ancestors a
    JOIN resolved member ON member.root_event_id = a.member_event_id
    JOIN resolved ancestor ON ancestor.root_event_id = a.ancestor_event_id
        AND ancestor.bucket = member.bucket
        AND (member.source <> 'manual' OR ancestor.root_event_id = member.root_event_id)
        AND (
            -- Every root is trivially its own canonical root.
            ancestor.root_event_id = member.root_event_id
            OR (
                -- Nesting alone is NOT evidence of the same program: unrelated
                -- programs are routinely banked under another program's org
                -- (e.g. Sleepover under Athena). Require the slug to also be
                -- branded as a child of the umbrella, which is what an actual
                -- flagship/satellite/grant-distribution org looks like.
                left(member.root_slug, length(ancestor.root_slug) + 1)
                    = ancestor.root_slug || '-'
                -- Never attribute a program to a personal author/reviewer pot.
                -- ysws_spend_org_tree refuses to descend through these, so they
                -- must not become attribution roots either.
                AND ancestor.root_slug NOT LIKE 'ysws-budget-%'
                AND ancestor.root_slug NOT LIKE 'ysws-resolution-%'
                AND ancestor.root_slug NOT LIKE '%-earnings'
                AND ancestor.root_slug NOT LIKE '%-jemoney'
                AND COALESCE(ancestor.root_org_name, '') NOT ILIKE '%budget%'
                AND COALESCE(ancestor.root_org_name, '') NOT ILIKE '%earnings%'
            )
        )
    ORDER BY a.member_event_id, a.depth DESC
),

canonical_members AS (
    SELECT
        canonical.root_event_id,
        canonical.root_slug,
        canonical.program_name AS canonical_program_name,
        member.program_name AS member_program_name,
        canonical.bucket,
        member.source
    FROM resolved member
    JOIN canonical_roots mapping
        ON mapping.member_event_id = member.root_event_id
    JOIN resolved canonical
        ON canonical.root_event_id = mapping.canonical_root_event_id
)

SELECT
    root_event_id,
    root_slug,
    string_agg(DISTINCT canonical_program_name, ' / ' ORDER BY canonical_program_name) AS program_name,
    -- Constituent Airtable names (an org can back several program versions,
    -- e.g. Jumpstart V1/V2/V3) — used to join approved_projects hours.
    array_agg(DISTINCT member_program_name) AS member_names,
    -- MIN so a manual 'marketing' tag wins if an Airtable row later points at
    -- the same org with the default 'program' bucket.
    MIN(bucket) AS bucket,
    string_agg(DISTINCT source, '+') AS source
FROM canonical_members
GROUP BY root_event_id, root_slug
