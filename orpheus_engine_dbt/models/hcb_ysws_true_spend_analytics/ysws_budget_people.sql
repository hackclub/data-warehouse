{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Who each YSWS budget pot belongs to — one row per record in the "YSWS
    Authors" table of the Unified YSWS DB (the reviewer/staff roster), with the
    HCB org their "HCB Budget Fund" field points at.

    That field is the ONLY person -> budget link there is; it is hand-typed,
    it is a URL rather than a record link, and it is under-maintained, so
    link_status reports the shape of every gap the same way
    ysws_unlinked_programs does for programs:

      linked                   the URL resolves to an HCB org we hold
      no_budget_link           the field is empty
      unparseable_budget_link  the field is not an hcb.hackclub.com org URL
      org_not_found            the slug matches no HCB org (renamed or typo'd)

    Only linked rows attach a person to a pot. Everything else is a fix-it
    item, published on the site so the roster gets repaired rather than
    silently under-reporting who spent what.

    NOT published: the roster's email column, and pay ("temp - total payouts").
    grants_attributed_dollars is the dollar value of grants credited to the
    person as a reviewer, which is what makes the gap list rankable.
*/

WITH authors AS (
    SELECT
        id AS person_record_id,
        NULLIF(TRIM(name), '') AS person_name,
        NULLIF(TRIM(hcb_budget_fund), '') AS hcb_budget_field,
        usd_for_weighted_grants,
        weighted_grants_total
    FROM {{ source('unified_ysws', 'ysws_authors') }}
),

parsed AS (
    SELECT
        a.*,
        -- HCB org URLs are https://hcb.hackclub.com/<slug>, sometimes with a
        -- trailing path (/transactions) or the ui3. host.
        lower(SUBSTRING(
            a.hcb_budget_field FROM '^https?://(?:ui3\.)?hcb\.hackclub\.com/([^/?#]+)'
        )) AS linked_slug
    FROM authors a
)

SELECT
    p.person_record_id,
    p.person_name,
    -- app3A5kJwYqxMLOgh = "Unified YSWS Projects DB", tblRf1BQs5H8298gW = "YSWS Authors"
    'https://airtable.com/app3A5kJwYqxMLOgh/tblRf1BQs5H8298gW/' || p.person_record_id
        AS airtable_record_url,
    p.hcb_budget_field,
    p.linked_slug,
    o.event_id AS budget_event_id,
    o.slug AS budget_slug,
    o.name AS budget_name,
    CASE
        WHEN p.hcb_budget_field IS NULL THEN 'no_budget_link'
        WHEN p.linked_slug IS NULL THEN 'unparseable_budget_link'
        WHEN o.event_id IS NULL THEN 'org_not_found'
        ELSE 'linked'
    END AS link_status,
    o.event_id IS NOT NULL AS has_budget,
    ROUND(COALESCE(p.usd_for_weighted_grants, 0)::numeric, 2) AS grants_attributed_dollars,
    ROUND(COALESCE(p.weighted_grants_total, 0)::numeric, 2) AS weighted_grants
FROM parsed p
LEFT JOIN {{ ref('orgs') }} o ON o.slug = p.linked_slug
ORDER BY grants_attributed_dollars DESC
