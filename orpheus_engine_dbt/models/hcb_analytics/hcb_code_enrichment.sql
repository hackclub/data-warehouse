{{ config(
    schema='hcb_analytics',
    materialized='table'
) }}

/*
    Per-hcb_code display enrichment for transaction lists: receipt status,
    tags, spent-vs-settled timing, and — for card transactions — the
    merchant, card, spender, and charge method HCB's own transaction page
    shows. One row per hcb_code (hcb_codes.hcb_code is unique), so joining
    this to any ledger never fans out.

    Columns:
      receipt_count             receipts attached to the code's HCB page
      receipt_marked_no_or_lost someone explicitly marked the receipt as
                                lost / never existed (distinct from just
                                missing)
      tag_labels                user-defined per-org tag labels, sorted
      spent_date                earliest pending-transaction date (when the
                                card was swiped / the transfer initiated)
      settled_date              earliest settled canonical-transaction date
      settled_after_days        settled_date - spent_date, NULL when unknown
                                or negative (data quirks)
      merchant_name/city/country/category
                                Stripe merchant_data (card transactions only)
      charge_method             keyed_in / swipe / chip / contactless / online
      charge_wallet             apple_pay / google_pay / samsung_pay, if any
      card_last4 / card_name / card_type_text / card_user_name / card_user_email
                                the card that made the charge and its holder

    Card fields come from raw_stripe_transactions (settled, via
    transaction_source_id) and raw_pending_stripe_transactions (auth method,
    via canonical_pending_transactions.raw_pending_stripe_transaction_id) —
    the card id inside stripe_transaction changed format over time, hence
    the COALESCE of ->'card'->>'id' and ->>'card'.

    hcb_codes rows are created lazily by HCB's UI, but coverage is total in
    practice: every real HCB-% code in the YSWS spend ledger had a row as of
    2026-07-30.

    A handful of codes have duplicate hcb_codes rows in the mirror (deleted
    and re-created upstream; the incremental mirror keeps the stale row), so
    receipts and tags are aggregated across all row ids sharing the code.
*/

WITH receipt_counts AS (
    SELECT
        receiptable_id AS hcb_code_row_id,
        COUNT(*) AS receipt_count
    FROM {{ source('hcb', 'receipts') }}
    WHERE receiptable_type = 'HcbCode'
    GROUP BY 1
),

tag_labels AS (
    SELECT
        hct.hcb_code_id AS hcb_code_row_id,
        ARRAY_AGG(DISTINCT t.label ORDER BY t.label) AS tag_labels
    FROM {{ source('hcb', 'hcb_codes_tags') }} hct
    JOIN {{ source('hcb', 'tags') }} t ON t.id = hct.tag_id
    GROUP BY 1
),

-- When did the money actually move vs when did it settle? The pending
-- transaction (card authorization, initiated transfer) carries the spend
-- date; the settled canonical transaction carries the ledger date.
pending_settle AS (
    SELECT
        ct.hcb_code,
        MIN(cpt.date) AS spent_date,
        MIN(ct.date) AS settled_date
    FROM {{ source('hcb', 'canonical_pending_settled_mappings') }} m
    JOIN {{ source('hcb', 'canonical_pending_transactions') }} cpt
        ON cpt.id = m.canonical_pending_transaction_id
    JOIN {{ source('hcb', 'canonical_transactions') }} ct
        ON ct.id = m.canonical_transaction_id
    WHERE ct.hcb_code IS NOT NULL
    GROUP BY 1
),

-- Merchant + card for settled card transactions. Refunds share the original
-- charge's hcb_code, so DISTINCT ON keeps the earliest source row.
card_settled AS (
    SELECT DISTINCT ON (ct.hcb_code)
        ct.hcb_code,
        rst.stripe_transaction -> 'merchant_data' ->> 'name' AS merchant_name,
        rst.stripe_transaction -> 'merchant_data' ->> 'city' AS merchant_city,
        rst.stripe_transaction -> 'merchant_data' ->> 'country' AS merchant_country,
        rst.stripe_transaction -> 'merchant_data' ->> 'category' AS merchant_category,
        COALESCE(rst.stripe_transaction -> 'card' ->> 'id',
                 rst.stripe_transaction ->> 'card') AS card_stripe_id
    FROM {{ source('hcb', 'canonical_transactions') }} ct
    JOIN {{ source('hcb', 'raw_stripe_transactions') }} rst
        ON ct.transaction_source_type = 'RawStripeTransaction'
       AND ct.transaction_source_id = rst.id
    WHERE ct.hcb_code IS NOT NULL
    ORDER BY ct.hcb_code, rst.id
),

-- Authorization method (and wallet) only exists on the pending side; also a
-- merchant/card fallback for authorizations that have not settled yet.
card_pending AS (
    SELECT DISTINCT ON (cpt.hcb_code)
        cpt.hcb_code,
        rpst.stripe_transaction ->> 'authorization_method' AS charge_method,
        NULLIF(rpst.stripe_transaction ->> 'wallet', '') AS charge_wallet,
        rpst.stripe_transaction -> 'merchant_data' ->> 'name' AS merchant_name,
        rpst.stripe_transaction -> 'merchant_data' ->> 'city' AS merchant_city,
        rpst.stripe_transaction -> 'merchant_data' ->> 'country' AS merchant_country,
        rpst.stripe_transaction -> 'merchant_data' ->> 'category' AS merchant_category,
        COALESCE(rpst.stripe_transaction -> 'card' ->> 'id',
                 rpst.stripe_transaction ->> 'card') AS card_stripe_id
    FROM {{ source('hcb', 'canonical_pending_transactions') }} cpt
    JOIN {{ source('hcb', 'raw_pending_stripe_transactions') }} rpst
        ON rpst.id = cpt.raw_pending_stripe_transaction_id
    WHERE cpt.hcb_code IS NOT NULL
    ORDER BY cpt.hcb_code, rpst.id
),

card_info AS (
    SELECT
        COALESCE(s.hcb_code, p.hcb_code) AS hcb_code,
        COALESCE(s.merchant_name, p.merchant_name) AS merchant_name,
        COALESCE(s.merchant_city, p.merchant_city) AS merchant_city,
        COALESCE(s.merchant_country, p.merchant_country) AS merchant_country,
        COALESCE(s.merchant_category, p.merchant_category) AS merchant_category,
        COALESCE(s.card_stripe_id, p.card_stripe_id) AS card_stripe_id,
        p.charge_method,
        p.charge_wallet
    FROM card_settled s
    FULL OUTER JOIN card_pending p ON p.hcb_code = s.hcb_code
),

cards AS (
    SELECT
        sc.stripe_id,
        sc.last4 AS card_last4,
        sc.name AS card_name,
        CASE sc.card_type WHEN 0 THEN 'virtual' WHEN 1 THEN 'physical' END AS card_type_text,
        u.full_name AS card_user_name,
        u.email AS card_user_email
    FROM {{ source('hcb', 'stripe_cards') }} sc
    LEFT JOIN {{ source('hcb', 'stripe_cardholders') }} sch
        ON sch.id = sc.stripe_cardholder_id
    LEFT JOIN {{ source('hcb', 'users') }} u
        ON u.id = sch.user_id
),

per_code AS (
    SELECT
        hc.hcb_code,
        MAX(hc.id) AS hcb_code_row_id,
        SUM(COALESCE(rc.receipt_count, 0)) AS receipt_count,
        BOOL_OR(hc.marked_no_or_lost_receipt_at IS NOT NULL) AS receipt_marked_no_or_lost
    FROM {{ source('hcb', 'hcb_codes') }} hc
    LEFT JOIN receipt_counts rc ON rc.hcb_code_row_id = hc.id
    GROUP BY 1
),

code_tags AS (
    SELECT
        hc.hcb_code,
        ARRAY_AGG(DISTINCT label ORDER BY label) AS tag_labels
    FROM {{ source('hcb', 'hcb_codes') }} hc
    JOIN tag_labels tl ON tl.hcb_code_row_id = hc.id
    CROSS JOIN LATERAL UNNEST(tl.tag_labels) AS label
    GROUP BY 1
)

SELECT
    pc.hcb_code,
    pc.hcb_code_row_id,
    pc.receipt_count,
    pc.receipt_marked_no_or_lost,
    ct.tag_labels,
    ps.spent_date,
    ps.settled_date,
    CASE
        WHEN ps.settled_date >= ps.spent_date
            THEN ps.settled_date - ps.spent_date
    END AS settled_after_days,
    ci.merchant_name,
    ci.merchant_city,
    ci.merchant_country,
    ci.merchant_category,
    ci.charge_method,
    ci.charge_wallet,
    cd.card_last4,
    cd.card_name,
    cd.card_type_text,
    cd.card_user_name,
    cd.card_user_email
FROM per_code pc
LEFT JOIN code_tags ct ON ct.hcb_code = pc.hcb_code
LEFT JOIN pending_settle ps ON ps.hcb_code = pc.hcb_code
LEFT JOIN card_info ci ON ci.hcb_code = pc.hcb_code
LEFT JOIN cards cd ON cd.stripe_id = ci.card_stripe_id
