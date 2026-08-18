{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Individual YSWS budget pots — one row per pot in ysws_budget_orgs, with the
    person it belongs to already attached (roster link; NULL when the roster's
    "HCB Budget Fund" field was never filled in).

    personal_spend_dollars = the pot's own external spend + card-grant funding
    (drill into ysws_author_budget_ledger WHERE is_personal_spend for the
    transactions). transferred_to_orgs_dollars is money sent back to programs
    or HQ — excluded from personal spend because the receiving org's ledger
    already counts it.
*/

WITH activity AS (
    SELECT
        budget_event_id,
        MIN(transaction_date) AS first_activity_date,
        MAX(transaction_date) AS last_activity_date,
        SUM(outflow_dollars) FILTER (WHERE is_personal_spend) AS personal_spend_dollars,
        SUM(outflow_dollars) FILTER (WHERE budget_bucket = 'transfer_to_org') AS transferred_to_orgs_dollars,
        SUM(-outflow_dollars) FILTER (WHERE budget_bucket = 'funding_received') AS funding_received_dollars,
        SUM(-outflow_dollars) FILTER (WHERE budget_bucket = 'other_inflow') AS other_inflow_dollars
    FROM {{ ref('ysws_author_budget_ledger') }}
    GROUP BY 1
)

SELECT
    o.budget_event_id,
    o.budget_slug,
    o.budget_name,
    o.hcb_url,
    o.matched_by,
    o.person_record_id,
    o.person_name,
    o.airtable_record_url,
    o.has_person,
    o.is_also_program_root,
    o.also_program_name,
    o.is_public,
    a.first_activity_date,
    a.last_activity_date,
    ROUND(COALESCE(a.personal_spend_dollars, 0)::numeric, 2) AS personal_spend_dollars,
    ROUND(COALESCE(a.transferred_to_orgs_dollars, 0)::numeric, 2) AS transferred_to_orgs_dollars,
    ROUND(COALESCE(a.funding_received_dollars, 0)::numeric, 2) AS funding_received_dollars,
    ROUND(COALESCE(a.other_inflow_dollars, 0)::numeric, 2) AS other_inflow_dollars,
    ROUND(o.balance_cents / 100.0, 2) AS balance_dollars,
    ROUND(o.card_grants_total_cents / 100.0, 2) AS card_grants_funded_dollars,
    ROUND(o.card_grants_active_cents / 100.0, 2) AS card_grants_unspent_dollars
FROM {{ ref('ysws_budget_orgs') }} o
LEFT JOIN activity a ON a.budget_event_id = o.budget_event_id
ORDER BY personal_spend_dollars DESC
