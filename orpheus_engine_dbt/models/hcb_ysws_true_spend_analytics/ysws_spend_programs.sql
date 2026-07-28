{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS spend program registry — one row per program ROOT HCB org.

    Roots come from two places:
    1. Airtable ysws_programs rows with an HCB link (hcb URL -> slug).
    2. Manual entries for programs whose money lives in HCB but that have no
       (correct) HCB link in Airtable:
         - daydream / campfire umbrella orgs (whole city-org trees)
         - Outpost, whose fund is the stardance-hardware org (a sub-org of
           stardance that actually holds a different program's money)

    manual_members attaches extra orgs to a program's tree when they are
    related but NOT HCB sub-orgs of the root (no parent_id link), e.g.
    som-sticker-shipments is Summer of Making fulfillment funded by HQ.

    Grain: root_event_id. If several Airtable rows point at the same HCB org,
    their names are aggregated into one row.
*/

WITH airtable_roots AS (
    SELECT
        p.name AS program_name,
        regexp_replace(p.hcb, '^https://hcb\.hackclub\.com/([^/?#]+).*$', '\1') AS root_slug,
        'program' AS bucket
    FROM {{ source('unified_ysws', 'ysws_programs') }} p
    WHERE p.hcb ~ '^https://hcb\.hackclub\.com/'
),

manual_roots (program_name, root_slug, bucket) AS (
    VALUES
        ('Daydream', 'daydream', 'program'),
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
        r.program_name,
        r.bucket,
        r.source
    FROM all_roots r
    JOIN {{ ref('orgs') }} o ON o.slug = r.root_slug
)

SELECT
    root_event_id,
    root_slug,
    string_agg(DISTINCT program_name, ' / ' ORDER BY program_name) AS program_name,
    -- Constituent Airtable names (an org can back several program versions,
    -- e.g. Jumpstart V1/V2/V3) — used to join approved_projects hours.
    array_agg(DISTINCT program_name) AS member_names,
    -- MIN so a manual 'marketing' tag wins if an Airtable row later points at
    -- the same org with the default 'program' bucket.
    MIN(bucket) AS bucket,
    string_agg(DISTINCT source, '+') AS source
FROM resolved
GROUP BY root_event_id, root_slug
