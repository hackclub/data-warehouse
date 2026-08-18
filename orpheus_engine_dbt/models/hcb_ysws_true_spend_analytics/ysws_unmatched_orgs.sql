{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    HCB orgs that are NOT matched to any YSWS program, but look like they should
    be — the fix-it list for the mapping contract in ysws_spend_programs.

    An org is matched only by being the org a Unified YSWS DB record links to,
    or a descendant of one. This model does not guess at attribution; it reports
    the orgs whose STRUCTURE or MONEY says a link is missing, and what it costs
    to leave them out:

      reason = 'parent_of_mapped_root'
        Its child is a linked program root, but it is not itself linked. Almost
        always the Airtable link points at a sub-org (e.g. a satellite or grant
        distribution org) instead of the umbrella. Fix = move the Airtable link
        up, or leave it if the parent is genuinely unrelated.

      reason = 'funded_by_mapped_program'
        A mapped program disbursed money to it, but no program claims it, so
        those dollars leave every program's tree. Fix = link it in Airtable, or
        parent it under the program in HCB.

      reason = 'funds_mapped_program'
        It disbursed money INTO a mapped program while unmatched itself
        (upstream pot, sister org, unlinked umbrella).

    Deliberately NOT a reason: name or slug resemblance. If nothing structural
    or financial connects an org to a program, it does not belong here.

    Grain: (event_id, reason) — an org can appear for several reasons.
*/

WITH tree AS (
    SELECT root_event_id, root_slug, program_name, event_id, org_slug, bucket
    FROM {{ ref('ysws_spend_org_tree') }}
),

roots AS (
    SELECT root_event_id, root_slug, program_name FROM {{ ref('ysws_spend_programs') }}
),

unmatched AS (
    SELECT o.*
    FROM {{ ref('orgs') }} o
    WHERE NOT o.is_deleted
      AND NOT EXISTS (SELECT 1 FROM tree t WHERE t.event_id = o.event_id)
),

-- (a) structural: its child is a mapped root, it is not mapped
parents_of_roots AS (
    SELECT
        u.event_id,
        'parent_of_mapped_root' AS reason,
        string_agg(DISTINCT r.program_name, ', ' ORDER BY r.program_name) AS related_programs,
        0::numeric AS dollars_from_programs,
        0::numeric AS dollars_to_programs
    FROM unmatched u
    JOIN {{ source('hcb', 'events') }} child ON child.parent_id = u.event_id
    JOIN roots r ON r.root_event_id = child.id
    GROUP BY 1, 2
),

-- (b) financial: money crossed between a mapped tree and this org
flows AS (
    SELECT
        d.event_id AS other_event_id,
        t.program_name,
        SUM(d.amount) FILTER (WHERE TRUE) AS to_other_cents,
        0::bigint AS from_other_cents
    FROM {{ source('hcb', 'disbursements') }} d
    JOIN tree t ON t.event_id = d.source_event_id
    WHERE d.aasm_state = 'deposited'
      AND d.event_id <> d.source_event_id
      AND NOT EXISTS (SELECT 1 FROM tree t2 WHERE t2.event_id = d.event_id)
    GROUP BY 1, 2

    UNION ALL

    SELECT
        d.source_event_id AS other_event_id,
        t.program_name,
        0::bigint,
        SUM(d.amount)
    FROM {{ source('hcb', 'disbursements') }} d
    JOIN tree t ON t.event_id = d.event_id
    WHERE d.aasm_state = 'deposited'
      AND d.event_id <> d.source_event_id
      AND NOT EXISTS (SELECT 1 FROM tree t2 WHERE t2.event_id = d.source_event_id)
    GROUP BY 1, 2
),

flow_rollup AS (
    SELECT
        other_event_id AS event_id,
        string_agg(DISTINCT program_name, ', ' ORDER BY program_name) AS related_programs,
        ROUND(SUM(to_other_cents) / 100.0, 2) AS dollars_from_programs,
        ROUND(SUM(from_other_cents) / 100.0, 2) AS dollars_to_programs
    FROM flows
    GROUP BY 1
),

money_reasons AS (
    SELECT
        f.event_id,
        CASE WHEN f.dollars_from_programs >= f.dollars_to_programs
             THEN 'funded_by_mapped_program'
             ELSE 'funds_mapped_program' END AS reason,
        f.related_programs,
        f.dollars_from_programs,
        f.dollars_to_programs
    FROM flow_rollup f
    JOIN unmatched u ON u.event_id = f.event_id
    WHERE f.dollars_from_programs > 0 OR f.dollars_to_programs > 0
),

combined AS (
    SELECT * FROM parents_of_roots
    UNION ALL
    SELECT * FROM money_reasons
)

SELECT
    c.event_id,
    u.slug AS org_slug,
    u.name AS org_name,
    c.reason,
    c.related_programs,
    u.parent_slug,
    EXISTS (SELECT 1 FROM tree t WHERE t.event_id = u.parent_id) AS parent_is_mapped,
    -- Fiscal-host and HQ-operations orgs (hq, bank, usps-ops, ...) show up here
    -- because they legitimately move money in and out of programs. Flagged, not
    -- filtered: filtering them would be a guess about which orgs "count".
    u.is_hq,
    u.plan_category,
    c.dollars_from_programs,
    c.dollars_to_programs,
    ROUND(-u.total_outflow_cents / 100.0, 2) AS gross_outflow_dollars,
    u.balance_dollars,
    'https://hcb.hackclub.com/' || u.slug AS hcb_url
FROM combined c
JOIN unmatched u ON u.event_id = c.event_id
ORDER BY GREATEST(c.dollars_from_programs, c.dollars_to_programs) DESC, u.slug
