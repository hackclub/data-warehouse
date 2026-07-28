{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS true spend by program — one row per program, reconciling gross outflow
    into TRUE SPEND (A + C), excluded categories (B / D / X), and netted
    intra-tree transfers (I):

        gross_outflow = true_spend + not_spend (B+D+X) + intra_tree (I)

    stated_outflow_dollars reproduces the legacy "stated spend" methodology
    (HCB raised - balance == gross main-ledger outflow, summed over the tree)
    for comparison. balance_dollars is money still sitting in the tree — not
    yet spend, will become spend as it is used.

    cost_per_weighted_hour joins approved_projects weighted hours by exact
    program name; NULL where the program has no approved projects (yet) or is
    tracked app-natively (e.g. Stardance hours live in stardance.projects).
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
        ROUND(SUM(card_grants_active_cents) / 100.0, 2) AS card_grants_unspent_dollars
    FROM {{ ref('ysws_spend_org_tree') }}
    GROUP BY 1
),

hours AS (
    -- Weighted hours joined through each root's constituent Airtable program
    -- names (an org can back several program versions).
    SELECT
        p.root_event_id,
        SUM(ap.ysws_weighted_project_contribution::numeric) * 10 AS weighted_hours,
        COUNT(*) AS approved_project_count
    FROM {{ ref('ysws_spend_programs') }} p
    JOIN {{ source('unified_ysws', 'approved_projects__ysws_name') }} yn
        ON yn.value = ANY(p.member_names)
    JOIN {{ source('unified_ysws', 'approved_projects') }} ap
        ON yn._dlt_parent_id = ap._dlt_id
    GROUP BY 1
)

SELECT
    p.program_name,
    p.bucket,
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
    ts.card_grants_unspent_dollars,

    h.weighted_hours,
    h.approved_project_count,
    CASE WHEN h.weighted_hours > 0
         THEN ROUND((COALESCE(s.true_spend_dollars, 0) / h.weighted_hours)::numeric, 2)
    END AS cost_per_weighted_hour

FROM {{ ref('ysws_spend_programs') }} p
JOIN tree_stats ts ON ts.root_event_id = p.root_event_id
LEFT JOIN spend s ON s.root_event_id = p.root_event_id
LEFT JOIN hours h ON h.root_event_id = p.root_event_id
ORDER BY true_spend_dollars DESC
