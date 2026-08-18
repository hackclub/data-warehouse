{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Every personal YSWS budget pot, and whose it is.

    The universe used to be `slug LIKE 'ysws-budget-%'`, which silently dropped
    six real pots whose slug never followed the convention — dhamari,
    reem-s-yswss, leafd-s-spending-fund, guac, chris-s-ysws-budget and
    ysws-onekey — hiding ~$45.7K of personal spend. A pot is now claimed by any
    of three signals, in priority order (matched_by records which one):

      slug        slug LIKE 'ysws-budget-%'         the convention
      name        name ILIKE 'YSWS - Budget%'       HCB's own display name
      airtable    a roster row's HCB Budget Fund    the odd ones out

    The name rule is a strict superset of the slug rule; Airtable adds the
    pots renamed out of both (today: ysws-onekey).

    is_also_program_root flags the handful of pots that ysws_spend_org_tree
    also treats as a program root. Their dollars are counted once here and once
    on the program side, on purpose — the two views answer different questions —
    so anything summing budgets and programs together must net these out.
*/

WITH linked AS (
    SELECT DISTINCT linked_slug
    FROM {{ ref('ysws_budget_people') }}
    WHERE link_status = 'linked'
),

pots AS (
    SELECT
        o.event_id,
        o.slug,
        o.name,
        o.parent_slug,
        o.aasm_state,
        o.is_public,
        o.balance_cents,
        o.card_grants_total_cents,
        o.card_grants_active_cents,
        CASE
            WHEN o.slug LIKE 'ysws-budget-%' THEN 'slug'
            WHEN o.name ILIKE 'YSWS - Budget%' THEN 'name'
            ELSE 'airtable'
        END AS matched_by
    FROM {{ ref('orgs') }} o
    WHERE o.slug LIKE 'ysws-budget-%'
       OR o.name ILIKE 'YSWS - Budget%'
       OR o.slug IN (SELECT linked_slug FROM linked)
),

-- At most one roster row points at a given pot today; MIN keeps the model
-- one-row-per-pot if someone ever pastes the same URL twice.
owner AS (
    SELECT
        linked_slug,
        MIN(person_record_id) AS person_record_id,
        MIN(person_name) AS person_name,
        MIN(airtable_record_url) AS airtable_record_url
    FROM {{ ref('ysws_budget_people') }}
    WHERE link_status = 'linked'
    GROUP BY 1
)

SELECT
    p.event_id AS budget_event_id,
    p.slug AS budget_slug,
    p.name AS budget_name,
    'https://hcb.hackclub.com/' || p.slug AS hcb_url,
    p.matched_by,
    p.parent_slug,
    p.aasm_state,
    p.is_public,
    p.balance_cents,
    p.card_grants_total_cents,
    p.card_grants_active_cents,
    w.person_record_id,
    w.person_name,
    w.airtable_record_url,
    w.person_record_id IS NOT NULL AS has_person,
    t.program_name AS also_program_name,
    t.program_name IS NOT NULL AS is_also_program_root
FROM pots p
LEFT JOIN owner w ON w.linked_slug = p.slug
-- The three pots that HCB also registers as a program's own org, so the site
-- can name the program instead of just flagging the overlap.
LEFT JOIN {{ ref('ysws_spend_org_tree') }} t
       ON t.event_id = p.event_id AND t.event_id = t.root_event_id
ORDER BY p.slug
