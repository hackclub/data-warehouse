{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS true-spend ledger — every MAIN-LEDGER OUTFLOW row of every org in every
    program tree, classified so that each dollar lands in exactly one category
    and the categories reconcile to the org tree's gross outflow.

    Categories (spend_category):
      A  spent_on_event        EXPENSE   grants to makers (card-grant funding,
                                         incl. still-unspent cards), external
                                         card/ACH/wire/check spend, chapters
      C  internal_cost         EXPENSE   payments to HQ service orgs (postage,
                                         printing, fulfillment, hosting) and
                                         fines swept out — real costs the
                                         program bears
      B  author_fund           not exp.  transfers into personal author/reviewer
                                         pots (future events, not this program)
      D  returned_to_hq        not exp.  overfunding sent back to the fiscal host
      X  other_internal        not exp.  round-trip washes and transfers to
                                         OTHER programs (counted in the
                                         receiving program's own ledger)
      I  intra_tree            netted    transfers to the program's own sub-orgs
                                         (the sub-org's spend is counted
                                         directly, so the transfer must not be)
      M  marketing_offset      SYNTHETIC negative row on the RECEIVING program
                                         for each marketing->program budget
                                         transfer. The transfer itself is
                                         marketing spend (program_funding, A);
                                         this offset nets the same dollars out
                                         of the program's true spend so program
                                         + marketing sums don't double count.
                                         is_synthetic_offset = true and
                                         linked_transaction_id / linked_hcb_code
                                         / linked_org_slug point back at the
                                         marketing-side transaction.

    TRUE SPEND for a program = SUM(outflow_dollars) WHERE is_true_spend
    (synthetic offsets are negative and net automatically).

    MARKETING BACKFILL: the ysws-marketing org only exists as of July 2026, so
    historical marketing spend is backfilled as SYNTHETIC rows (spend_bucket =
    'marketing_backfill', transaction_type = 'synthetic_backfill', hcb_code =
    'BACKFILL-<airtable record id>') from the "Payments" table of the Hack
    Club Videos DB Airtable base — see marketing_videos_db_payments, which
    also skips payments that already match a real marketing-tree transaction.
    These are NOT real HCB transactions in the marketing org; the money moved
    through other orgs. When a backfilled payment's canonical HCB link
    resolves to a true-spend row inside a tracked program tree (i.e. those
    dollars are already counted as that program's spend), a marketing_offset
    row nets the payment amount back out of that program, mirroring the M
    mechanism above, so marketing + program sums don't double count.

    Excluded rows: card-grant subledger activity (transaction_source_type =
    'CardGrant' and subledger rows) — grant funding already appears as a
    main-ledger disbursement-to-self, and counting card swipes too would
    double-count. Unspent grant-card money IS treated as spend (committed the
    moment the card is funded).
*/

{% set internal_payment_keywords = [
    'postage', 'warehouse', 'shipping', 'fedex', 'freight', 'ups ', 'dhl',
    'import', 'fulfil', 'invoice', 'contractor', 'reimburs', 'sticker',
    'print', 'label', 'envelope', 'server', 'hosting', 'stamp', 'package',
] %}

WITH tree AS (
    SELECT * FROM {{ ref('ysws_spend_org_tree') }}
),

-- Tree-wide disbursement flows vs each EXTERNAL org, for round-trip wash
-- detection (org A sends $X to org B and gets ~$X back -> not spend).
external_net AS (
    SELECT root_event_id, other_event_id, SUM(out_cents) AS out_cents, SUM(in_cents) AS in_cents
    FROM (
        SELECT ts.root_event_id, d.event_id AS other_event_id, d.amount AS out_cents, 0 AS in_cents
        FROM {{ source('hcb', 'disbursements') }} d
        JOIN tree ts ON ts.event_id = d.source_event_id
        LEFT JOIN tree td ON td.event_id = d.event_id AND td.root_event_id = ts.root_event_id
        WHERE d.aasm_state = 'deposited' AND d.source_event_id <> d.event_id
          AND td.event_id IS NULL

        UNION ALL

        SELECT td.root_event_id, d.source_event_id, 0, d.amount
        FROM {{ source('hcb', 'disbursements') }} d
        JOIN tree td ON td.event_id = d.event_id
        LEFT JOIN tree ts ON ts.event_id = d.source_event_id AND ts.root_event_id = td.root_event_id
        WHERE d.aasm_state = 'deposited' AND d.source_event_id <> d.event_id
          AND ts.event_id IS NULL
    ) flows
    GROUP BY 1, 2
),

wash_partners AS (
    -- External orgs where money round-trips (>= 99% comes back to the tree)
    SELECT n.root_event_id, e.slug AS other_slug
    FROM external_net n
    JOIN {{ source('hcb', 'events') }} e ON e.id = n.other_event_id
    WHERE n.out_cents > 0 AND n.in_cents >= 0.99 * n.out_cents
),

classified AS (
    SELECT
        t.program_name,
        t.bucket,
        t.root_event_id,
        t.root_slug,
        l.org_id,
        l.org_slug,
        l.org_name,
        l.transaction_id,
        l.hcb_code,
        l.transaction_date,
        l.transaction_type,
        l.transaction_source_type,
        l.amount_cents,
        l.amount_dollars,
        -l.amount_dollars AS outflow_dollars,
        l.display_memo,
        l.disbursement_name,
        l.dest_org_slug,
        l.dest_org_name,
        l.counterparty_name,
        l.is_internal_transfer,
        -- Who initiated it, where HCB records one: the requesting user for
        -- disbursements, the creating user otherwise. Card swipes have no
        -- user here (that would need raw_stripe_transactions, not mirrored).
        COALESCE(l.requested_by_name, l.transacting_user_name) AS initiated_by_name,
        l.ach_payment_for,

        CASE
            -- Non-disbursement rows ride external bank rails. Safety net: if HCB
            -- flags the hcb_code in >1 event it is an internal leg, not spend.
            WHEN l.transaction_type <> 'disbursement' THEN
                CASE
                    WHEN l.is_internal_transfer THEN 'inter_org'
                    WHEN l.transaction_type IN ('card_transaction', 'ach_transfer', 'wire', 'check',
                                                'paypal_transfer', 'wise_transfer', 'expense_payout')
                        THEN 'external_spend'
                    WHEN l.transaction_source_type IN ('Wire', 'WiseTransfer')
                        THEN 'external_spend'
                    ELSE 'other_external'
                END

            -- Disbursement to self (or with no dest recorded) = card-grant funding.
            WHEN l.dest_org_slug IS NULL OR l.dest_org_slug = l.org_slug THEN 'grants'

            -- Transfer to another org in the SAME program tree: netted out.
            WHEN dt.event_id IS NOT NULL THEN 'intra_tree'

            -- Marketing paying into a program's budget (e.g. $2/watch-hour
            -- incentives): MARKETING expense at transfer time, by design.
            -- A synthetic negative offset row is generated on the receiving
            -- program (see offsets CTE) so bucket sums don't double count.
            WHEN t.bucket = 'marketing' AND pt.event_id IS NOT NULL THEN 'program_funding'

            -- Personal author/reviewer pots.
            WHEN l.dest_org_slug LIKE 'ysws-budget-%'
              OR l.dest_org_slug LIKE 'ysws-resolution-%'
              OR l.dest_org_slug LIKE '%-fund'
              OR l.dest_org_slug LIKE '%-earnings'
              OR l.dest_org_slug LIKE '%-jemoney'
              OR COALESCE(l.dest_org_name, '') ILIKE '%budget%'
              OR COALESCE(l.dest_org_name, '') ILIKE '%earnings%'
              OR LOWER(COALESCE(l.disbursement_name, '')) LIKE '%personal budget transfer%'
                THEN 'author_fund'

            -- Payment for a real cost: memo names a service, or dest is a service org.
            WHEN {% for kw in internal_payment_keywords -%}
                 LOWER(COALESCE(l.disbursement_name, '')) LIKE '%{{ kw }}%' OR
                 {% endfor -%}
                 l.dest_org_slug IN ('hq-usps-ops', 'printing-legion', 'sprig', 'nest', 'hackpad')
                THEN 'internal_payment'

            -- Program local chapters are part of the event footprint.
            WHEN l.dest_org_slug LIKE 'build-guild-%' THEN 'program_chapters'

            -- Fiscal host without a payment keyword: overfunding coming back.
            WHEN l.dest_org_slug IN ('hq', 'bank', 'hcb') THEN 'host_return'

            -- Fines swept to the central fines org: a real cost the event bears.
            WHEN l.dest_org_slug = 'fines' THEN 'fines'

            -- Round-trip wash with an external org.
            WHEN w.other_slug IS NOT NULL THEN 'wash_roundtrip'

            ELSE 'inter_org'
        END AS spend_bucket

    FROM {{ ref('ledger') }} l
    JOIN tree t ON t.event_id = l.org_id
    LEFT JOIN tree dt
        ON dt.org_slug = l.dest_org_slug AND dt.root_event_id = t.root_event_id
    LEFT JOIN tree pt
        ON pt.org_slug = l.dest_org_slug AND pt.bucket = 'program'
    LEFT JOIN wash_partners w
        ON w.root_event_id = t.root_event_id AND w.other_slug = l.dest_org_slug
    WHERE l.flow_direction = 'outflow'
      AND l.subledger_id IS NULL
      AND l.transaction_source_type IS DISTINCT FROM 'CardGrant'
),

-- Synthetic negative rows on the RECEIVING program for every marketing ->
-- program budget transfer, so the same dollars are not counted twice across
-- buckets. Clearly linked back to the marketing-side transaction.
offsets AS (
    SELECT
        pt.program_name,
        pt.bucket,
        pt.root_event_id,
        pt.root_slug,
        pt.event_id AS org_id,
        pt.org_slug,
        pt.org_name,
        NULL::bigint AS transaction_id,
        'OFFSET-' || c.hcb_code AS hcb_code,
        c.transaction_date,
        'synthetic_offset' AS transaction_type,
        'SyntheticOffset' AS transaction_source_type,
        -c.amount_cents AS amount_cents,
        -c.amount_dollars AS amount_dollars,
        -c.outflow_dollars AS outflow_dollars,
        'Marketing-funded budget (offset): '
            || COALESCE(c.disbursement_name, c.display_memo, '') AS display_memo,
        c.disbursement_name,
        NULL::text AS dest_org_slug,
        NULL::text AS dest_org_name,
        'Marketing (' || c.org_slug || ')' AS counterparty_name,
        FALSE AS is_internal_transfer,
        NULL::text AS initiated_by_name,
        NULL::text AS ach_payment_for,
        'marketing_offset' AS spend_bucket,
        TRUE AS is_synthetic_offset,
        c.transaction_id AS linked_transaction_id,
        c.hcb_code AS linked_hcb_code,
        c.org_slug AS linked_org_slug
    FROM classified c
    JOIN tree pt
        ON pt.org_slug = c.dest_org_slug AND pt.bucket = 'program'
    WHERE c.spend_bucket = 'program_funding'
),

-- Historical marketing payments (Videos DB Airtable) that predate the
-- marketing org, as SYNTHETIC spend rows on the marketing tree root. Payments
-- that already match a real marketing-tree transaction are excluded upstream.
backfill_payments AS (
    SELECT *
    FROM {{ ref('marketing_videos_db_payments') }}
    WHERE NOT is_ignored
      AND marketing_org_match_method IS NULL
),

backfill_rows AS (
    SELECT
        m.program_name,
        m.bucket,
        m.root_event_id,
        m.root_slug,
        m.event_id AS org_id,
        m.org_slug,
        m.org_name,
        NULL::bigint AS transaction_id,
        'BACKFILL-' || bp.airtable_record_id AS hcb_code,
        bp.payment_date AS transaction_date,
        'synthetic_backfill' AS transaction_type,
        'AirtableVideosDbPayment' AS transaction_source_type,
        -ROUND(bp.amount_dollars * 100)::bigint AS amount_cents,
        -bp.amount_dollars AS amount_dollars,
        bp.amount_dollars AS outflow_dollars,
        'SYNTHETIC backfill from Videos DB Airtable: '
            || COALESCE(bp.description, '') AS display_memo,
        NULL::text AS disbursement_name,
        NULL::text AS dest_org_slug,
        NULL::text AS dest_org_name,
        bp.person_name AS counterparty_name,
        FALSE AS is_internal_transfer,
        NULL::text AS initiated_by_name,
        NULL::text AS ach_payment_for,
        'marketing_backfill' AS spend_bucket,
        FALSE AS is_synthetic_offset,
        NULL::bigint AS linked_transaction_id,
        -- The underlying real-world transaction, where one was recorded.
        bp.url_hcb_code AS linked_hcb_code,
        NULL::text AS linked_org_slug
    FROM backfill_payments bp
    JOIN tree m ON m.bucket = 'marketing' AND m.event_id = m.root_event_id
),

-- When a backfilled payment's canonical HCB link resolves to spend that is
-- already counted inside a tracked PROGRAM tree, those dollars would be
-- double counted (program + marketing). Net the payment amount (the
-- marketing-attributable share) back out of the program with a
-- marketing_offset row, mirroring the program_funding offsets above.
-- Two ways a payment can point into a program tree:
--   (1) its HCB code is a true-spend ledger row of a tree org
--   (2) it is a GRANT-<id> card grant issued from a tree org (grant funding
--       is counted as that program's 'grants' spend under a different code)
backfill_offset_candidates AS (
    SELECT
        bp.airtable_record_id,
        c.transaction_id AS dedupe_order,
        c.program_name,
        c.bucket,
        c.root_event_id,
        c.root_slug,
        c.org_id,
        c.org_slug,
        c.org_name,
        c.transaction_date,
        bp.amount_dollars,
        bp.description,
        c.disbursement_name,
        c.transaction_id AS linked_transaction_id,
        c.hcb_code AS linked_hcb_code,
        c.org_slug AS linked_org_slug
    FROM backfill_payments bp
    JOIN classified c
        ON c.hcb_code = bp.url_hcb_code
       AND c.bucket = 'program'
       AND c.spend_bucket IN ('grants', 'external_spend', 'other_external',
                              'program_chapters', 'internal_payment', 'fines')

    UNION ALL

    SELECT
        bp.airtable_record_id,
        cg.id AS dedupe_order,
        t.program_name,
        t.bucket,
        t.root_event_id,
        t.root_slug,
        t.event_id AS org_id,
        t.org_slug,
        t.org_name,
        cg.created_at::date AS transaction_date,
        bp.amount_dollars,
        bp.description,
        NULL::text AS disbursement_name,
        NULL::bigint AS linked_transaction_id,
        'GRANT-' || cg.id::text AS linked_hcb_code,
        t.org_slug AS linked_org_slug
    FROM backfill_payments bp
    JOIN {{ source('hcb', 'card_grants') }} cg
        ON 'GRANT-' || cg.id::text = bp.url_hcb_code
    JOIN {{ source('hcb', 'disbursements') }} d
        ON d.id = cg.disbursement_id AND d.aasm_state = 'deposited'
    JOIN tree t
        ON t.event_id = cg.event_id AND t.bucket = 'program'
),

backfill_offsets AS (
    SELECT DISTINCT ON (cand.airtable_record_id)
        cand.program_name,
        cand.bucket,
        cand.root_event_id,
        cand.root_slug,
        cand.org_id,
        cand.org_slug,
        cand.org_name,
        NULL::bigint AS transaction_id,
        'BACKFILL-OFFSET-' || cand.airtable_record_id AS hcb_code,
        cand.transaction_date,
        'synthetic_offset' AS transaction_type,
        'SyntheticOffset' AS transaction_source_type,
        ROUND(cand.amount_dollars * 100)::bigint AS amount_cents,
        cand.amount_dollars AS amount_dollars,
        -cand.amount_dollars AS outflow_dollars,
        'Marketing-paid backfill (offset): ' || COALESCE(cand.description, '') AS display_memo,
        cand.disbursement_name,
        NULL::text AS dest_org_slug,
        NULL::text AS dest_org_name,
        'Marketing (Videos DB backfill)' AS counterparty_name,
        FALSE AS is_internal_transfer,
        NULL::text AS initiated_by_name,
        NULL::text AS ach_payment_for,
        'marketing_offset' AS spend_bucket,
        TRUE AS is_synthetic_offset,
        cand.linked_transaction_id,
        cand.linked_hcb_code,
        cand.linked_org_slug
    FROM backfill_offset_candidates cand
    ORDER BY cand.airtable_record_id, cand.dedupe_order
),

unioned AS (
    SELECT
        c.*,
        FALSE AS is_synthetic_offset,
        NULL::bigint AS linked_transaction_id,
        NULL::text AS linked_hcb_code,
        NULL::text AS linked_org_slug
    FROM classified c
    UNION ALL
    SELECT * FROM offsets
    UNION ALL
    SELECT * FROM backfill_rows
    UNION ALL
    SELECT * FROM backfill_offsets
)

SELECT
    c.*,
    -- Display enrichment: receipt status, tags, and spent-vs-settled timing
    -- from the code's HCB page. NULL / 0-receipt for synthetic rows (their
    -- pseudo-codes have no HCB page).
    COALESCE(e.receipt_count, 0) AS receipt_count,
    COALESCE(e.receipt_marked_no_or_lost, FALSE) AS receipt_marked_no_or_lost,
    e.tag_labels,
    e.spent_date,
    e.settled_after_days,
    -- Clickable HCB transaction page. Real rows link to their own transaction
    -- (/hcb/<code> resolves full HCB-xxx-xxx codes). Synthetic rows (OFFSET-,
    -- BACKFILL-) have no HCB page of their own, so they link to the linked
    -- real transaction that generated them, when there is one. GRANT-<id>
    -- pseudo-codes are warehouse-synthesized and have no HCB page.
    CASE
        WHEN c.hcb_code LIKE 'HCB-%'
            THEN 'https://hcb.hackclub.com/hcb/' || c.hcb_code
        WHEN c.linked_hcb_code IS NOT NULL AND c.linked_hcb_code NOT LIKE 'GRANT-%'
            THEN 'https://hcb.hackclub.com/hcb/' || c.linked_hcb_code
    END AS hcb_url,
    CASE c.spend_bucket
        WHEN 'grants'           THEN 'A'
        WHEN 'external_spend'   THEN 'A'
        WHEN 'other_external'   THEN 'A'
        WHEN 'program_chapters' THEN 'A'
        WHEN 'program_funding'  THEN 'A'
        WHEN 'marketing_backfill' THEN 'A'
        WHEN 'internal_payment' THEN 'C'
        WHEN 'fines'            THEN 'C'
        WHEN 'author_fund'      THEN 'B'
        WHEN 'host_return'      THEN 'D'
        WHEN 'wash_roundtrip'   THEN 'X'
        WHEN 'inter_org'        THEN 'X'
        WHEN 'intra_tree'       THEN 'I'
        WHEN 'marketing_offset' THEN 'M'
    END AS spend_category,
    CASE c.spend_bucket
        WHEN 'grants'           THEN 'A - Grants to makers (card-grant funding, incl. unspent cards)'
        WHEN 'external_spend'   THEN 'A - Direct external spend (card / ACH / wire / check)'
        WHEN 'other_external'   THEN 'A - Other external payments (Column ACH / bank / Wise)'
        WHEN 'program_chapters' THEN 'A - Program local chapters'
        WHEN 'program_funding'  THEN 'A - Program budget funding from marketing (incentives)'
        WHEN 'marketing_backfill' THEN 'A - SYNTHETIC backfill of pre-org marketing payments (Videos DB Airtable)'
        WHEN 'internal_payment' THEN 'C - Internal payment for a real cost (postage / fulfillment / services)'
        WHEN 'fines'            THEN 'C - Fines swept to central account'
        WHEN 'author_fund'      THEN 'B - Into personal author/reviewer funds (not this program''s spend)'
        WHEN 'host_return'      THEN 'D - Returned to HQ (overfunding)'
        WHEN 'wash_roundtrip'   THEN 'X - Round-trip wash with another org'
        WHEN 'inter_org'        THEN 'X - Transfer to another program / org'
        WHEN 'intra_tree'       THEN 'I - Intra-tree transfer (netted; sub-org spend counted directly)'
        WHEN 'marketing_offset' THEN 'M - Offset: budget funded by marketing (nets the linked marketing transaction)'
    END AS spend_bucket_label,
    -- Offsets are negative and true-spend, so program totals net automatically.
    c.spend_bucket IN ('grants', 'external_spend', 'other_external', 'program_chapters',
                       'program_funding', 'marketing_backfill', 'internal_payment', 'fines',
                       'marketing_offset') AS is_true_spend
FROM unioned c
LEFT JOIN {{ ref('hcb_code_enrichment') }} e ON e.hcb_code = c.hcb_code
