{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS spend org tree — every HCB org belonging to each program, discovered
    through HCB's real sub-organization relationship (hcb.events.parent_id),
    descended recursively from the program root.

    Descent rules:
    - STOP at any org that is itself another canonical program root. Branded
      satellites (campfire-flagship under campfire) have already been folded
      into their umbrella by ysws_spend_programs, so they stay in the umbrella
      tree rather than cutting a hole in it. Programs merely banked under
      another program's org (Sleepover under Athena) are still their own roots
      and so remain boundaries here, as do manual roots like distinct funds.
    - STOP at personal author/reviewer pots (ysws-budget-*, ysws-resolution-<x>,
      names containing budget/earnings). Transfers into them are category B in
      the ledger — money for a person's future events, not this program's spend.
    - Soft-deleted events are excluded.

    manual_members rows join a program's tree without a parent_id link
    (fulfillment orgs funded directly by HQ).

    Grain: (root_event_id, event_id). An org appears in at most one program's
    tree because parent_id is single-parent and descent stops at other roots.
*/

WITH RECURSIVE manual_members (member_slug, root_slug) AS (
    VALUES
        ('som-sticker-shipments', 'summer')
),

roots AS (
    SELECT root_event_id, root_slug, program_name, bucket
    FROM {{ ref('ysws_spend_programs') }}
),

seed AS (
    -- Program roots...
    SELECT r.root_event_id, r.root_event_id AS event_id
    FROM roots r
    UNION ALL
    -- ...plus manually attached members.
    SELECT r.root_event_id, o.event_id
    FROM manual_members m
    JOIN roots r ON r.root_slug = m.root_slug
    JOIN {{ ref('orgs') }} o ON o.slug = m.member_slug
),

tree AS (
    SELECT s.root_event_id, s.event_id, 0 AS depth
    FROM seed s

    UNION ALL

    SELECT t.root_event_id, e.id AS event_id, t.depth + 1
    FROM tree t
    JOIN {{ source('hcb', 'events') }} e ON e.parent_id = t.event_id
    WHERE e.deleted_at IS NULL
      -- stop at another program's root (its spend is its own program's)
      AND e.id NOT IN (SELECT root_event_id FROM roots)
      -- stop at personal author/reviewer pots (transfers to them stay category B)
      AND e.slug NOT LIKE 'ysws-budget-%'
      AND e.slug NOT LIKE 'ysws-resolution-%'
      AND e.slug NOT LIKE '%-earnings'
      AND e.slug NOT LIKE '%-jemoney'
      AND COALESCE(e.name, '') NOT ILIKE '%budget%'
      AND COALESCE(e.name, '') NOT ILIKE '%earnings%'
)

SELECT
    r.program_name,
    r.bucket,
    t.root_event_id,
    r.root_slug,
    t.event_id,
    o.slug AS org_slug,
    o.name AS org_name,
    t.depth,
    t.depth = 0 AND t.event_id = t.root_event_id AS is_root,
    o.balance_cents,
    o.total_outflow_cents,
    o.card_grants_total_cents,
    o.card_grants_active_cents
FROM tree t
JOIN roots r ON r.root_event_id = t.root_event_id
JOIN {{ ref('orgs') }} o ON o.event_id = t.event_id
