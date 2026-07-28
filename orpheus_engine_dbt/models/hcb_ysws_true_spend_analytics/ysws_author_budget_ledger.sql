{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Individual YSWS budget pot ledger — every main-ledger transaction (both
    directions) of every personal `ysws-budget-*` HCB org, for per-person staff
    spend in the leadership dashboard (map slug/org_name -> person downstream).

    budget_bucket:
      external_spend     outflow to the outside world ......... personal spend
      card_grant_funding outflow funding the person's cards .... personal spend
      transfer_to_org    outflow to another HCB org ........... NOT personal
                         spend (money returned to a program counts in that
                         program's own ledger — counting here would double it)
      internal_leg       outflow flagged internal on bank rails NOT personal spend
      funding_received   inflow disbursement (source org identifies the
                         granting program; these are the same dollars program
                         ledgers classify as category B)
      other_inflow       any other inflow (refunds, donations)

    Personal spend for a pot = SUM(outflow_dollars) WHERE is_personal_spend.
*/

WITH pots AS (
    SELECT event_id, slug, name, balance_cents,
           card_grants_total_cents, card_grants_active_cents
    FROM {{ ref('orgs') }}
    WHERE slug LIKE 'ysws-budget-%'
)

SELECT
    p.event_id AS budget_event_id,
    p.slug AS budget_slug,
    p.name AS budget_name,
    l.transaction_id,
    l.hcb_code,
    l.transaction_date,
    l.transaction_type,
    l.transaction_source_type,
    l.flow_direction,
    l.amount_dollars,
    -l.amount_dollars AS outflow_dollars,
    l.display_memo,
    l.disbursement_name,
    l.source_org_slug,
    l.source_org_name,
    l.dest_org_slug,
    l.dest_org_name,
    l.counterparty_name,

    CASE
        WHEN l.flow_direction = 'inflow' THEN
            CASE WHEN l.transaction_type = 'disbursement' THEN 'funding_received'
                 ELSE 'other_inflow' END
        WHEN l.transaction_type = 'disbursement' THEN
            CASE WHEN l.dest_org_slug IS NULL OR l.dest_org_slug = p.slug
                     THEN 'card_grant_funding'
                 ELSE 'transfer_to_org' END
        WHEN l.is_internal_transfer THEN 'internal_leg'
        ELSE 'external_spend'
    END AS budget_bucket,

    l.flow_direction = 'outflow'
        AND ((l.transaction_type = 'disbursement'
                  AND (l.dest_org_slug IS NULL OR l.dest_org_slug = p.slug))
             OR (l.transaction_type <> 'disbursement'
                  AND NOT l.is_internal_transfer)) AS is_personal_spend

FROM {{ ref('ledger') }} l
JOIN pots p ON p.event_id = l.org_id
WHERE l.subledger_id IS NULL
  AND l.transaction_source_type IS DISTINCT FROM 'CardGrant'
