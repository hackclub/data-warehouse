{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Shared YSWS operating costs that belong in the leadership dashboard's
    "Other" bucket rather than on an individual staff member or program.

    Grain: one source charge or approved payout run. All amounts are positive
    expense dollars; transaction_date is the month in which the cost is
    recognized.

      security_bounty   Source-side YSWS chargebacks to the central security
                        bounty org, plus explicitly memoed security payouts.
                        These are X rows in ysws_spend_ledger, so promoting them
                        here does not double-count program true spend.
      fulfillment_bounty
                        Approved Flavortown and Stardance payout runs, accrued
                        to the run's service-period end date.
      servers           Direct HQ card charges from Hetzner and Cloudflare.
                        Raw HCB-to-HCB reimbursements are intentionally excluded.
*/

WITH security_bounties AS (
    SELECT
        'security_bounty'::text AS cost_type,
        l.transaction_date,
        'hcb_ysws_spend_ledger'::text AS source_system,
        COALESCE(l.transaction_id::text, l.hcb_code) AS source_id,
        l.hcb_code AS source_reference,
        l.program_name AS detail,
        l.outflow_dollars::numeric AS amount_dollars,
        l.initiated_by_name,
        l.receipt_count,
        l.receipt_marked_no_or_lost,
        l.tag_labels,
        l.spent_date,
        l.settled_after_days
    FROM {{ ref('ysws_spend_ledger') }} l
    WHERE l.bucket = 'program'
      AND (
          l.dest_org_slug = 'hack-club-security'
          OR COALESCE(l.disbursement_name, l.display_memo, '') ~* 'security (bounty|problem)'
      )
),

fulfillment_bounties AS (
    SELECT
        'fulfillment_bounty'::text AS cost_type,
        r.period_end::date AS transaction_date,
        'flavortown'::text AS source_system,
        r.id::text AS source_id,
        NULL::text AS source_reference,
        'Flavortown'::text AS detail,
        r.total_amount::numeric AS amount_dollars,
        NULL::text AS initiated_by_name,
        NULL::bigint AS receipt_count,
        NULL::boolean AS receipt_marked_no_or_lost,
        NULL::text[] AS tag_labels,
        NULL::date AS spent_date,
        NULL::integer AS settled_after_days
    FROM {{ source('flavortown', 'fulfillment_payout_runs') }} r
    WHERE r.aasm_state = 'approved'

    UNION ALL

    SELECT
        'fulfillment_bounty'::text AS cost_type,
        r.period_end::date AS transaction_date,
        'stardance'::text AS source_system,
        r.id::text AS source_id,
        NULL::text AS source_reference,
        'Stardance'::text AS detail,
        r.total_amount::numeric AS amount_dollars,
        NULL::text AS initiated_by_name,
        NULL::bigint AS receipt_count,
        NULL::boolean AS receipt_marked_no_or_lost,
        NULL::text[] AS tag_labels,
        NULL::date AS spent_date,
        NULL::integer AS settled_after_days
    FROM {{ source('stardance', 'fulfillment_payout_runs') }} r
    WHERE r.aasm_state = 'approved'
),

server_charges AS (
    SELECT
        'servers'::text AS cost_type,
        l.transaction_date,
        'hcb_hq'::text AS source_system,
        COALESCE(l.transaction_id::text, l.hcb_code) AS source_id,
        l.hcb_code AS source_reference,
        CASE
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'HETZNER'
                THEN 'Hetzner'
            ELSE 'Cloudflare'
        END AS detail,
        ABS(l.amount_dollars)::numeric AS amount_dollars,
        COALESCE(l.requested_by_name, l.transacting_user_name) AS initiated_by_name,
        COALESCE(e.receipt_count, 0) AS receipt_count,
        COALESCE(e.receipt_marked_no_or_lost, FALSE) AS receipt_marked_no_or_lost,
        e.tag_labels,
        e.spent_date,
        e.settled_after_days
    FROM {{ ref('ledger') }} l
    LEFT JOIN {{ ref('hcb_code_enrichment') }} e ON e.hcb_code = l.hcb_code
    WHERE l.org_slug = 'hq'
      AND l.transaction_source_type = 'RawStripeTransaction'
      AND l.flow_direction = 'outflow'
      AND NOT l.is_internal_transfer
      AND CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* '(HETZNER|CLOUDFLARE)'
)

SELECT
    cost_type,
    transaction_date,
    source_system,
    source_id,
    source_system || ':' || source_id AS source_key,
    source_reference,
    CASE WHEN source_reference LIKE 'HCB-%'
         THEN 'https://hcb.hackclub.com/hcb/' || source_reference
    END AS hcb_url,
    detail,
    amount_dollars,
    initiated_by_name,
    receipt_count,
    receipt_marked_no_or_lost,
    tag_labels,
    spent_date,
    settled_after_days
FROM security_bounties

UNION ALL

SELECT
    cost_type,
    transaction_date,
    source_system,
    source_id,
    source_system || ':' || source_id AS source_key,
    source_reference,
    CASE WHEN source_reference LIKE 'HCB-%'
         THEN 'https://hcb.hackclub.com/hcb/' || source_reference
    END AS hcb_url,
    detail,
    amount_dollars,
    initiated_by_name,
    receipt_count,
    receipt_marked_no_or_lost,
    tag_labels,
    spent_date,
    settled_after_days
FROM fulfillment_bounties

UNION ALL

SELECT
    cost_type,
    transaction_date,
    source_system,
    source_id,
    source_system || ':' || source_id AS source_key,
    source_reference,
    CASE WHEN source_reference LIKE 'HCB-%'
         THEN 'https://hcb.hackclub.com/hcb/' || source_reference
    END AS hcb_url,
    detail,
    amount_dollars,
    initiated_by_name,
    receipt_count,
    receipt_marked_no_or_lost,
    tag_labels,
    spent_date,
    settled_after_days
FROM server_charges
