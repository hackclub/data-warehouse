{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS true spend by program — one row per program, reconciling gross outflow
    into TRUE SPEND (A + C), excluded categories (B / D / X), and netted
    intra-tree transfers (I):

        gross_outflow = true_spend + not_spend (B+D+X) + intra_tree (I)

    Grant cards: card_grants_funded_dollars is money committed to cards (already
    inside true_spend), card_grants_remaining_dollars is how much of it is still
    sitting on those cards, measured from each grant's subledger.
    card_grants_active_face_dollars is only the face value of the grants HCB
    marks active and is NOT a remaining balance -- it was previously exposed as
    card_grants_unspent_dollars, which overstated what is left by ~6x.

    stated_outflow_dollars reproduces the legacy "stated spend" methodology
    (HCB raised - balance == gross main-ledger outflow, summed over the tree)
    for comparison. balance_dollars is money still sitting in the tree — not
    yet spend, will become spend as it is used.

    weighted_projects / weighted_hours / cost_per_weighted_hour join
    approved_projects through the Airtable program RECORD IDs carried on
    ysws_spend_programs.member_ids, so the match survives program renames;
    NULL where the program has no approved projects (yet), has no Airtable
    record (manual roots), or is tracked app-natively (e.g. Stardance hours
    live in stardance.projects).
*/

WITH spend AS (
    SELECT
        root_event_id,
        COUNT(DISTINCT org_id) FILTER (WHERE NOT is_synthetic_offset) AS orgs_with_outflows,
        MIN(transaction_date) FILTER (WHERE NOT is_synthetic_offset) AS first_outflow_date,
        MAX(transaction_date) FILTER (WHERE NOT is_synthetic_offset) AS last_outflow_date,
        -- Real rows only: reconciles as A+C+B+D+X+I.
        SUM(outflow_dollars) FILTER (WHERE NOT is_synthetic_offset) AS gross_outflow_dollars,
        SUM(outflow_dollars) FILTER (WHERE spend_category = 'A') AS spent_on_event_dollars,
        SUM(outflow_dollars) FILTER (WHERE spend_category = 'C') AS internal_cost_dollars,
        SUM(outflow_dollars) FILTER (WHERE spend_category = 'B') AS author_fund_dollars,
        SUM(outflow_dollars) FILTER (WHERE spend_category = 'D') AS returned_to_hq_dollars,
        SUM(outflow_dollars) FILTER (WHERE spend_category = 'X') AS other_internal_dollars,
        SUM(outflow_dollars) FILTER (WHERE spend_category = 'I') AS intra_tree_dollars,
        -- Synthetic offsets (category M, negative) net marketing-funded budget
        -- out of the receiving program's true spend automatically.
        -SUM(outflow_dollars) FILTER (WHERE spend_category = 'M') AS funded_by_marketing_dollars,
        SUM(outflow_dollars) FILTER (WHERE is_true_spend) AS true_spend_dollars
    FROM {{ ref('ysws_spend_ledger') }}
    GROUP BY 1
),

tree_stats AS (
    SELECT
        root_event_id,
        COUNT(*) AS org_count,
        ROUND(SUM(-total_outflow_cents) / 100.0, 2) AS stated_outflow_dollars,
        ROUND(SUM(balance_cents) / 100.0, 2) AS balance_dollars,
        ROUND(SUM(card_grants_total_cents) / 100.0, 2) AS card_grants_funded_dollars,
        -- Face value of grants HCB marks status = active. NOT money left on the
        -- cards: those grants are mostly spent (see grant_cards below).
        ROUND(SUM(card_grants_active_cents) / 100.0, 2) AS card_grants_active_face_dollars
    FROM {{ ref('ysws_spend_org_tree') }}
    GROUP BY 1
),

-- What is actually LEFT on the grant cards.
--
-- Funding a grant card is a main-ledger disbursement into the grant's own
-- subledger, and that funding is already counted as spend (spend_bucket
-- 'grants', category A) the moment it happens, unspent or not. This block does
-- not change that; it reports how much of that committed money is still
-- sitting on cards.
--
-- The balance is the subledger's own arithmetic: funding credit, minus swipes,
-- minus any expiry return to the org. The synthetic CardGrant issuance row is
-- excluded because it is the main-ledger side of the same event, not subledger
-- movement. Grants with no subledger rows yet are counted as fully unspent.
--
-- Deliberately NOT card_grants.status (0 = pending invite, 1 = active,
-- 2 = completed/expired): status says where a grant is in its lifecycle, not
-- how much is left on it. The grants marked active hold almost nothing, while
-- pending-invite grants hold most of the live money, so the balance is measured
-- rather than inferred from the label.
subledger_balances AS (
    SELECT
        subledger_id,
        SUM(amount_cents) AS net_cents,
        COUNT(*) AS row_count
    FROM {{ ref('ledger') }}
    WHERE subledger_id IS NOT NULL
      AND transaction_source_type IS DISTINCT FROM 'CardGrant'
    GROUP BY 1
),

grant_cards AS (
    SELECT
        t.root_event_id,
        COUNT(*) AS grant_card_count,
        ROUND(SUM(
            CASE WHEN sb.row_count IS NULL THEN cg.amount_cents ELSE sb.net_cents END
        ) / 100.0, 2) AS card_grants_remaining_dollars
    FROM {{ ref('ysws_spend_org_tree') }} t
    JOIN {{ source('hcb', 'card_grants') }} cg ON cg.event_id = t.event_id
    -- Only grants whose funding disbursement actually moved, matching how
    -- card_grants_funded_dollars is counted upstream in orgs. A grant created
    -- but not yet deposited has neither been spent nor put money on a card, so
    -- counting its balance would make remaining exceed funded.
    JOIN {{ source('hcb', 'disbursements') }} d
        ON d.id = cg.disbursement_id AND d.aasm_state = 'deposited'
    LEFT JOIN subledger_balances sb ON sb.subledger_id = cg.subledger_id
    GROUP BY 1
),

hours AS (
    -- Approved-project attribution, joined on the Airtable RECORD ID of each
    -- root's constituent programs (an org can back several program versions,
    -- e.g. Jumpstart V1/V2/V3).
    --
    -- Record ids, not names: a name match silently drops a program the moment
    -- Airtable renames it or spells it differently in the linked-record
    -- lookup, and it double counts programs whose names differ only in
    -- punctuation. The link table approved_projects__ysws holds the record ids
    -- of the programs each project was approved under, so the match is exact
    -- and deterministic.
    SELECT
        p.root_event_id,
        SUM(ap.ysws_weighted_project_contribution::numeric) AS weighted_projects,
        -- A weighted project is defined as 10 weighted hours.
        SUM(ap.ysws_weighted_project_contribution::numeric) * 10 AS weighted_hours,
        COUNT(*) AS approved_project_count
    FROM {{ ref('ysws_spend_programs') }} p
    JOIN {{ source('unified_ysws', 'approved_projects__ysws') }} yl
        ON yl.value = ANY(p.member_ids)
    JOIN {{ source('unified_ysws', 'approved_projects') }} ap
        ON yl._dlt_parent_id = ap._dlt_id
    GROUP BY 1
)

SELECT
    p.program_name,
    p.bucket,
    p.is_ysws_program,
    p.match_source,
    p.member_ids,
    p.root_event_id,
    p.root_slug,
    ts.org_count,
    s.first_outflow_date,
    s.last_outflow_date,

    ROUND(COALESCE(s.true_spend_dollars, 0)::numeric, 2) AS true_spend_dollars,
    ROUND(COALESCE(s.spent_on_event_dollars, 0)::numeric, 2) AS spent_on_event_dollars,
    ROUND(COALESCE(s.internal_cost_dollars, 0)::numeric, 2) AS internal_cost_dollars,
    ROUND(COALESCE(s.author_fund_dollars, 0)::numeric, 2) AS author_fund_dollars,
    ROUND(COALESCE(s.returned_to_hq_dollars, 0)::numeric, 2) AS returned_to_hq_dollars,
    ROUND(COALESCE(s.other_internal_dollars, 0)::numeric, 2) AS other_internal_dollars,
    ROUND(COALESCE(s.intra_tree_dollars, 0)::numeric, 2) AS intra_tree_dollars,
    ROUND(COALESCE(s.gross_outflow_dollars, 0)::numeric, 2) AS gross_outflow_dollars,

    ts.stated_outflow_dollars,
    CASE WHEN ts.stated_outflow_dollars > 0
         THEN ROUND(100.0 * (ts.stated_outflow_dollars - COALESCE(s.true_spend_dollars, 0)::numeric)
                    / ts.stated_outflow_dollars, 1)
    END AS stated_overstatement_pct,

    ROUND(COALESCE(s.funded_by_marketing_dollars, 0)::numeric, 2) AS funded_by_marketing_dollars,

    ts.balance_dollars,
    ts.card_grants_funded_dollars,
    ts.card_grants_active_face_dollars,
    COALESCE(gc.card_grants_remaining_dollars, 0) AS card_grants_remaining_dollars,
    COALESCE(gc.grant_card_count, 0) AS grant_card_count,

    ROUND(h.weighted_projects::numeric, 2) AS weighted_projects,
    h.weighted_hours,
    h.approved_project_count,
    CASE WHEN h.weighted_hours > 0
         THEN ROUND((COALESCE(s.true_spend_dollars, 0) / h.weighted_hours)::numeric, 2)
    END AS cost_per_weighted_hour

FROM {{ ref('ysws_spend_programs') }} p
JOIN tree_stats ts ON ts.root_event_id = p.root_event_id
LEFT JOIN spend s ON s.root_event_id = p.root_event_id
LEFT JOIN grant_cards gc ON gc.root_event_id = p.root_event_id
LEFT JOIN hours h ON h.root_event_id = p.root_event_id
ORDER BY true_spend_dollars DESC
