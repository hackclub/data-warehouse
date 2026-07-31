{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS grant spend — the vendor-level decomposition of every card-grant
    funding row in ysws_spend_ledger (spend_bucket = 'grants').

    True spend counts card-grant money at FUNDING time (the disbursement-to-
    self that loads the grant card), so the spend ledger shows "Grant to X"
    rows, not what the money actually bought. This model decomposes each
    grant's funding into:

      allocation_kind = 'spend'    one row per (grant subledger transaction ×
                                   funding month) — the card swipes / refunds
                                   the grantee made, with merchant + card
                                   enrichment, allocated back onto the month
                                   the grant was funded
      allocation_kind = 'unspent'  one row per funding ledger row — the share
                                   of that funding not (yet) spent on the
                                   card, incl. money later returned/expired

    Attribution: a grant funded across several months (top-ups) splits every
    subledger transaction across those months proportionally to each month's
    share of total funding (is_partial_allocation marks split rows). This
    makes the decomposition EXACT: for every program × month,

      SUM(allocated_dollars)  =  ysws_spend_ledger grants-bucket outflow

    (verified 0.00 diff across all 738 program-months of history when built),
    so a dashboard can replace the "Grants to makers" line with per-vendor
    rows + an "unspent" remainder and every column still ties out. Note that
    accrual_month is the FUNDING month — a swipe's own date is in
    transaction_date — and 'unspent' can go negative for a grant only in the
    rare case the subledger overspends its funding.

    Subledger transactions are identified by canonical_event_mappings
    .subledger_id → card_grants.subledger_id (every such row in the program
    trees resolves to a card grant; verified 2026-07-31). Disbursement legs
    (funding in, clawbacks out) are excluded — the funding side is already
    the anchor, and clawed-back money stays in 'unspent' by design, matching
    the ledger's gross-outflow accounting.
*/

WITH grants AS (
    -- One card grant per subledger. A handful of duplicate mirror rows exist
    -- upstream; keep the lowest id.
    SELECT DISTINCT ON (cg.subledger_id)
        cg.id AS grant_id,
        cg.subledger_id,
        COALESCE(u.full_name, SPLIT_PART(cg.email, '@', 1)) AS grant_recipient_name,
        cg.email AS grant_recipient_email,
        cg.purpose AS grant_purpose
    FROM {{ source('hcb', 'card_grants') }} cg
    LEFT JOIN {{ source('hcb', 'users') }} u ON u.id = cg.user_id
    WHERE cg.subledger_id IS NOT NULL
    ORDER BY cg.subledger_id, cg.id
),

-- Every grants-bucket funding row, linked to its card grant through the
-- disbursement's destination subledger.
funding AS (
    SELECT
        s.program_name,
        s.bucket,
        s.root_event_id,
        s.root_slug,
        s.org_id,
        s.org_slug,
        s.org_name,
        s.hcb_code,
        s.transaction_date,
        s.outflow_dollars,
        s.initiated_by_name,
        date_trunc('month', s.transaction_date)::date AS accrual_month,
        g.grant_id,
        g.grant_recipient_name,
        g.grant_recipient_email,
        g.grant_purpose
    FROM {{ ref('ysws_spend_ledger') }} s
    JOIN {{ source('hcb', 'disbursements') }} d
        ON s.hcb_code = 'HCB-500-' || d.id::text
    JOIN grants g ON g.subledger_id = d.destination_subledger_id
    WHERE s.spend_bucket = 'grants'
),

-- Month-by-month funding per grant, with each month's share of the grant's
-- total funding (the allocation weight for that month).
funding_shares AS (
    SELECT
        grant_id,
        accrual_month,
        SUM(outflow_dollars) AS funded_dollars,
        SUM(outflow_dollars)
            / NULLIF(SUM(SUM(outflow_dollars)) OVER (PARTITION BY grant_id), 0) AS share
    FROM funding
    GROUP BY 1, 2
),

funding_totals AS (
    SELECT grant_id, SUM(outflow_dollars) AS funded_total_dollars
    FROM funding
    GROUP BY 1
),

-- What the grantee actually did with the card: subledger card transactions
-- (and refunds), excluding the internal disbursement legs.
subledger_txns AS (
    SELECT
        g.grant_id,
        l.hcb_code,
        l.transaction_date,
        l.transaction_type,
        -l.amount_dollars AS outflow_dollars,
        l.display_memo
    FROM {{ ref('ledger') }} l
    JOIN grants g ON g.subledger_id = l.subledger_id
    WHERE l.subledger_id IS NOT NULL
      AND l.transaction_source_type IS DISTINCT FROM 'CardGrant'
      AND l.transaction_type NOT IN ('disbursement', 'incoming_disbursement')
),

spent_totals AS (
    SELECT grant_id, SUM(outflow_dollars) AS spent_total_dollars
    FROM subledger_txns
    GROUP BY 1
),

-- Program attribution rides on the funding side (grant and swipe share the
-- same org, but the funding rows are what the spend ledger counted).
grant_programs AS (
    SELECT DISTINCT ON (grant_id)
        grant_id, program_name, bucket, root_event_id, root_slug,
        org_id, org_slug, org_name
    FROM funding
    ORDER BY grant_id, transaction_date
),

allocations AS (
    -- Spend: each subledger transaction × each funding month, weighted.
    SELECT
        gp.program_name,
        gp.bucket,
        gp.root_event_id,
        gp.root_slug,
        gp.org_id,
        gp.org_slug,
        gp.org_name,
        t.grant_id,
        'spend' AS allocation_kind,
        fs.accrual_month,
        t.outflow_dollars * fs.share AS allocated_dollars,
        t.hcb_code,
        t.transaction_date,
        t.transaction_type,
        t.outflow_dollars AS txn_dollars,
        t.display_memo,
        fs.share < 1 AS is_partial_allocation,
        NULL::numeric AS funded_dollars,
        NULL::text AS funding_initiated_by_name
    FROM subledger_txns t
    JOIN funding_shares fs ON fs.grant_id = t.grant_id
    JOIN grant_programs gp ON gp.grant_id = t.grant_id

    UNION ALL

    -- Unspent: each funding row's share of the grant's unspent remainder.
    SELECT
        f.program_name,
        f.bucket,
        f.root_event_id,
        f.root_slug,
        f.org_id,
        f.org_slug,
        f.org_name,
        f.grant_id,
        'unspent' AS allocation_kind,
        f.accrual_month,
        f.outflow_dollars
            * (1 - COALESCE(st.spent_total_dollars, 0) / NULLIF(ft.funded_total_dollars, 0))
            AS allocated_dollars,
        f.hcb_code,
        f.transaction_date,
        'card_grant' AS transaction_type,
        f.outflow_dollars AS txn_dollars,
        'Card grant to ' || f.grant_recipient_name AS display_memo,
        FALSE AS is_partial_allocation,
        f.outflow_dollars AS funded_dollars,
        f.initiated_by_name AS funding_initiated_by_name
    FROM funding f
    JOIN funding_totals ft ON ft.grant_id = f.grant_id
    LEFT JOIN spent_totals st ON st.grant_id = f.grant_id
)

SELECT
    a.*,
    g.grant_recipient_name,
    g.grant_recipient_email,
    g.grant_purpose,
    ft.funded_total_dollars AS grant_funded_total_dollars,
    COALESCE(st.spent_total_dollars, 0) AS grant_spent_total_dollars,
    -- Display enrichment for spend rows: merchant, card, spender, receipts —
    -- same source the spend ledger uses. Funding rows keep their requester.
    CASE WHEN a.allocation_kind = 'spend'
         THEN COALESCE(e.card_user_name, g.grant_recipient_name)
         ELSE a.funding_initiated_by_name
    END AS initiated_by_name,
    e.merchant_name,
    e.merchant_country,
    e.merchant_category,
    e.card_last4,
    e.charge_method,
    e.charge_wallet,
    CASE WHEN a.allocation_kind = 'spend' THEN COALESCE(e.receipt_count, 0) END AS receipt_count,
    CASE WHEN a.allocation_kind = 'spend' THEN COALESCE(e.receipt_marked_no_or_lost, FALSE) END AS receipt_marked_no_or_lost,
    e.tag_labels,
    e.spent_date,
    e.settled_after_days,
    'https://hcb.hackclub.com/hcb/' || a.hcb_code AS hcb_url
FROM allocations a
JOIN grants g ON g.grant_id = a.grant_id
JOIN funding_totals ft ON ft.grant_id = a.grant_id
LEFT JOIN spent_totals st ON st.grant_id = a.grant_id
LEFT JOIN {{ ref('hcb_code_enrichment') }} e
    ON a.allocation_kind = 'spend' AND e.hcb_code = a.hcb_code
