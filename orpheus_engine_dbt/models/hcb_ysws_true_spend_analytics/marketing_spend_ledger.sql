{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Marketing Spend Ledger

    Complete marketing spend history in one place. Two kinds of rows, told
    apart by is_synthetic / record_kind:

    - hcb_transaction (is_synthetic = false): real transactions from the
      dedicated marketing HCB org (slug from the marketing_hcb_org_slug var),
      via the hcb_analytics ledger. This is the go-forward source of truth.

    - synthetic_backfill (is_synthetic = true): the marketing org only exists
      as of July 2026, so historical spend is backfilled from the "Payments"
      table of the "Hack Club Videos DB" Airtable base (applLfTvXaKUzBU0i).
      These rows are NOT real transactions in the marketing org — the money
      actually moved through other HCB orgs (HQ disbursements, card grants,
      salary, ...). source_url links to the Airtable record each row came
      from, and backfill_payment_url links to the underlying real-world money
      movement where one was recorded.

    A payment that already matches a real marketing-org transaction (see
    marketing_videos_db_payments.marketing_org_match_method) is skipped here
    so nothing is double-counted; the real hcb_transaction row covers it.

    Sign convention follows HCB: negative amounts are outflows (spend).
    Synthetic rows are always outflows.
*/

WITH hcb_transactions AS (
    SELECT
        'hcb:' || hcb_code || ':' || transaction_id::text AS ledger_id,
        false AS is_synthetic,
        'hcb_transaction' AS record_kind,
        'HCB "' || org_name || '" org ledger' AS data_source,
        'https://hcb.hackclub.com/hcb/' || hcb_code AS source_url,
        transaction_date,
        amount_cents,
        amount_dollars,
        flow_direction,
        display_memo AS memo,
        counterparty_name,
        NULL::text AS counterparty_slack_user_id,
        hcb_code,
        transaction_type,
        NULL::text AS backfill_payment_url
    FROM {{ ref('ledger') }}
    WHERE org_slug = '{{ var("marketing_hcb_org_slug") }}'
),

synthetic_backfill AS (
    SELECT
        'synthetic:airtable:' || airtable_record_id AS ledger_id,
        true AS is_synthetic,
        'synthetic_backfill' AS record_kind,
        'SYNTHETIC — backfilled from "Hack Club Videos DB" Airtable base, "Payments" table'
            AS data_source,
        airtable_record_url AS source_url,
        payment_date AS transaction_date,
        -ROUND(amount_dollars * 100)::bigint AS amount_cents,
        -amount_dollars AS amount_dollars,
        -- Payments are recorded as positive dollars in Airtable; a negative
        -- Amount there would mean money coming back (refund)
        CASE WHEN amount_dollars >= 0 THEN 'outflow' ELSE 'inflow' END AS flow_direction,
        description AS memo,
        person_name AS counterparty_name,
        person_slack_user_id AS counterparty_slack_user_id,
        NULL::text AS hcb_code,
        'synthetic_payment' AS transaction_type,
        canonical_url_source AS backfill_payment_url
    FROM {{ ref('marketing_videos_db_payments') }}
    WHERE NOT is_ignored
      AND marketing_org_match_method IS NULL
)

SELECT * FROM hcb_transactions
UNION ALL
SELECT * FROM synthetic_backfill
ORDER BY transaction_date DESC, ledger_id
