{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Unified YSWS DB programs whose HCB link cannot be used to map money — the
    other half of the fix-it list (see ysws_unmatched_orgs for the org side).

      gap_type = 'no_hcb_link'      the Airtable record has no hcb URL, so none
                                    of its spend can ever be attributed.
      gap_type = 'unparseable_hcb'  the hcb field is filled in but is not an
                                    hcb.hackclub.com org URL.
      gap_type = 'org_not_found'    the URL's slug does not match any HCB org
                                    (renamed slug, or a typo).
      gap_type = 'org_deleted'      the linked org is soft-deleted in HCB.

    Grain: Airtable program record id.
*/

WITH programs AS (
    SELECT
        p.id AS program_id,
        p.name AS program_name,
        p.hcb AS hcb_field,
        NULLIF(regexp_replace(COALESCE(p.hcb, ''), '^https://hcb\.hackclub\.com/([^/?#]+).*$', '\1'), '')
            AS linked_slug,
        COALESCE(p.hcb, '') ~ '^https://hcb\.hackclub\.com/' AS looks_like_org_url
    FROM {{ source('unified_ysws', 'ysws_programs') }} p
),

matched AS (
    SELECT
        pr.*,
        o.event_id,
        o.is_deleted
    FROM programs pr
    LEFT JOIN {{ ref('orgs') }} o
        ON pr.looks_like_org_url AND o.slug = pr.linked_slug
)

SELECT
    program_id,
    program_name,
    hcb_field,
    linked_slug,
    CASE
        WHEN COALESCE(hcb_field, '') = '' THEN 'no_hcb_link'
        WHEN NOT looks_like_org_url THEN 'unparseable_hcb'
        WHEN event_id IS NULL THEN 'org_not_found'
        WHEN is_deleted THEN 'org_deleted'
    END AS gap_type
FROM matched
WHERE COALESCE(hcb_field, '') = ''
   OR NOT looks_like_org_url
   OR event_id IS NULL
   OR is_deleted
ORDER BY gap_type, program_name
