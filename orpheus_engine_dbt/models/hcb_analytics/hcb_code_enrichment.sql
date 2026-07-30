{{ config(
    schema='hcb_analytics',
    materialized='table'
) }}

/*
    Per-hcb_code display enrichment for transaction lists: receipt status,
    tags, and spent-vs-settled timing — the extra context HCB's own
    transaction page shows. One row per hcb_code (hcb_codes.hcb_code is
    unique), so joining this to any ledger never fans out.

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

    hcb_codes rows are created lazily by HCB's UI, but coverage is total in
    practice: every real HCB-% code in the YSWS spend ledger had a row as of
    2026-07-30. Card / cardholder / merchant detail is NOT here — that lives
    in raw_stripe_transactions, which the warehouse deliberately does not
    mirror (the replication user has no grant on it).

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
    END AS settled_after_days
FROM per_code pc
LEFT JOIN code_tags ct ON ct.hcb_code = pc.hcb_code
LEFT JOIN pending_settle ps ON ps.hcb_code = pc.hcb_code
