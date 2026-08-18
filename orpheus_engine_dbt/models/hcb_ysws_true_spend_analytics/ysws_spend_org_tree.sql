{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS spend org tree — every HCB org belonging to each program: the org the
    Unified YSWS DB links to, plus every descendant of it through HCB's real
    sub-organization relationship (hcb.events.parent_id), recursively.

    See ysws_spend_programs for the mapping contract. This model adds no
    matching rules of its own. Descent stops only where it must:
    - at another program's root, so every org belongs to exactly one program
      (HCB's parent_id is single-parent, and the roots come from Airtable);
    - at soft-deleted events.

    Personal author/reviewer pots are NOT special here: a pot that is a sub-org
    of a linked program is part of that program (per Zach, 2026-08-18). Pots
    that hang off the `ysws` umbrella or another unlinked org are simply not in
    any tree, and transfers into them stay category B in the ledger.

    Orgs that no tree reaches are unmatched by definition — see
    ysws_unmatched_orgs, which lists the ones that exchange money with a mapped
    program or parent one, with the dollars at stake, so the Airtable link or
    the HCB parent relationship can be fixed.

    Grain: (root_event_id, event_id).
*/

WITH RECURSIVE roots AS (
    SELECT root_event_id, root_slug, program_name, bucket, is_ysws_program
    FROM {{ ref('ysws_spend_programs') }}
),

tree AS (
    SELECT r.root_event_id, r.root_event_id AS event_id, 0 AS depth
    FROM roots r

    UNION ALL

    SELECT t.root_event_id, e.id AS event_id, t.depth + 1
    FROM tree t
    JOIN {{ source('hcb', 'events') }} e ON e.parent_id = t.event_id
    WHERE e.deleted_at IS NULL
      -- stop at another program's root (its spend is its own program's)
      AND e.id NOT IN (SELECT root_event_id FROM roots)
)

SELECT
    r.program_name,
    r.bucket,
    r.is_ysws_program,
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
