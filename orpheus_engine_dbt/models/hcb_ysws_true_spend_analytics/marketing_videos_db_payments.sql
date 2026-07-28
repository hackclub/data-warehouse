{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    Videos DB Payments

    One row per record in the "Payments" table of the "Hack Club Videos DB"
    Airtable base (applLfTvXaKUzBU0i), read from the airtable_raw_all_bases
    mirror. These are marketing payments to video authors that predate the
    dedicated marketing HCB org and were paid out of assorted places (HQ
    disbursements, card grants, checks, salary, ...).

    Each payment is checked against the marketing HCB org tree (the
    bucket = 'marketing' tree in ysws_spend_org_tree, rooted at the org
    registered in ysws_spend_programs). Match methods, in priority order:
      - canonical_url_hcb_code: the payment's "Canonical URL Source" links to
        hcb.hackclub.com/hcb/<code> and that code (or short code) belongs to
        the marketing tree
      - amount_and_date: the marketing tree has an outflow of the exact same
        amount within 3 days of the payment date
    Matched payments already exist as real transactions in the marketing org,
    so ysws_spend_ledger skips them when it builds synthetic backfill rows.
*/

WITH payments AS (
    SELECT
        r.record_id AS airtable_record_id,
        'https://airtable.com/' || r.base_id || '/' || r.table_id || '/' || r.record_id
            AS airtable_record_url,
        (r.fields ->> 'Date')::date AS payment_date,
        (r.fields ->> 'Amount')::numeric AS amount_dollars,
        r.fields ->> 'Description' AS description,
        r.fields ->> 'Canonical URL Source' AS canonical_url_source,
        -- Payments link to exactly one author in practice; take the first link
        r.fields -> 'Person' ->> 0 AS person_record_id,
        NULLIF(TRIM(r.fields ->> 'Ignore Me'), '') IS NOT NULL AS is_ignored,
        -- HCB code when the canonical URL is an HCB transaction page
        SUBSTRING(
            r.fields ->> 'Canonical URL Source'
            FROM '^https://(?:ui3\.)?hcb\.hackclub\.com/hcb/([^/?#]+)'
        ) AS url_hcb_code,
        r._synced_at AS airtable_synced_at
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    WHERE r.base_id = 'applLfTvXaKUzBU0i'   -- "Hack Club Videos DB"
      AND r.table_id = 'tblXRkpDEfkBFHiFm'  -- "Payments"
),

authors AS (
    SELECT
        record_id,
        fields ->> 'Name' AS name,
        fields ->> 'Slack User ID' AS slack_user_id
    FROM {{ source('airtable_raw_all_bases', 'records') }}
    WHERE base_id = 'applLfTvXaKUzBU0i'
      AND table_id = 'tblAdQcf0RJNIyPs0'    -- "Attributed Authors"
),

marketing_tree AS (
    SELECT event_id
    FROM {{ ref('ysws_spend_org_tree') }}
    WHERE bucket = 'marketing'
),

-- Every HCB code that lives in the marketing tree: regular hcb_codes (with
-- their short codes) plus the GRANT-<id> pseudo-codes the hcb_analytics
-- ledger synthesizes for card grants. MATERIALIZED so the per-payment
-- lateral lookups below scan this small set instead of re-scanning
-- hcb.hcb_codes for every payment row.
marketing_org_codes AS MATERIALIZED (
    SELECT hc.hcb_code, hc.short_code
    FROM {{ source('hcb', 'hcb_codes') }} hc
    JOIN marketing_tree mt ON mt.event_id = hc.event_id
    UNION ALL
    SELECT 'GRANT-' || cg.id::text AS hcb_code, NULL AS short_code
    FROM {{ source('hcb', 'card_grants') }} cg
    JOIN marketing_tree mt ON mt.event_id = cg.event_id
),

marketing_org_ledger AS MATERIALIZED (
    SELECT l.hcb_code, l.transaction_date, l.amount_cents
    FROM {{ ref('ledger') }} l
    JOIN marketing_tree mt ON mt.event_id = l.org_id
)

SELECT
    p.airtable_record_id,
    p.airtable_record_url,
    p.payment_date,
    p.amount_dollars,
    p.description,
    p.person_record_id,
    a.name AS person_name,
    a.slack_user_id AS person_slack_user_id,
    p.canonical_url_source,
    p.url_hcb_code,
    p.is_ignored,
    COALESCE(code_match.hcb_code, fuzzy_match.hcb_code) AS matched_marketing_org_hcb_code,
    CASE
        WHEN code_match.hcb_code IS NOT NULL THEN 'canonical_url_hcb_code'
        WHEN fuzzy_match.hcb_code IS NOT NULL THEN 'amount_and_date'
    END AS marketing_org_match_method,
    p.airtable_synced_at
FROM payments p
LEFT JOIN authors a ON a.record_id = p.person_record_id
LEFT JOIN LATERAL (
    SELECT c.hcb_code
    FROM marketing_org_codes c
    WHERE p.url_hcb_code IS NOT NULL
      AND (c.hcb_code = p.url_hcb_code OR UPPER(c.short_code) = UPPER(p.url_hcb_code))
    LIMIT 1
) code_match ON true
LEFT JOIN LATERAL (
    SELECT l.hcb_code
    FROM marketing_org_ledger l
    WHERE l.amount_cents = -ROUND(p.amount_dollars * 100)::bigint
      AND l.transaction_date BETWEEN p.payment_date - 3 AND p.payment_date + 3
    ORDER BY ABS(l.transaction_date - p.payment_date)
    LIMIT 1
) fuzzy_match ON true
