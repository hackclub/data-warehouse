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
      servers           HQ server-infrastructure costs, from two sources:
                        (1) direct HQ card charges from hosting / CDN / DNS /
                        dev-infra vendors (Hetzner, Cloudflare, AWS,
                        DigitalOcean, Vercel, Linode, Heroku, Railway, GitHub,
                        Twilio, ...), and
                        (2) HQ-funded reimbursements of server bills paid out
                        through the HCB reimbursement clearinghouse — since
                        April 2026 the large monthly Hetzner invoice is paid
                        personally and reimbursed, so it never appears as an
                        HQ card charge. See infra_reimbursements below for
                        how those are tied back to HQ without the (unmirrored)
                        reimbursement-report tables.
                        Twilio for the HCB platform is excluded (HCB pays its
                        own Twilio from the bank org, which this never scans;
                        the rare HQ-card charge memoed for HCB is filtered).
      ai                OpenAI / Anthropic spend paid from HQ or from central
                        YSWS orgs that sit outside the program trees. AI spend
                        from program orgs stays in program true spend and AI
                        spend from individual author budgets stays in the
                        staff budget lines — counting either here would
                        double-count. Includes HQ-funded reimbursements of AI
                        credits via the clearinghouse.
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
        l.settled_after_days,
        l.card_last4,
        l.charge_method,
        l.charge_wallet
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
        NULL::integer AS settled_after_days,
        NULL::text AS card_last4,
        NULL::text AS charge_method,
        NULL::text AS charge_wallet
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
        NULL::integer AS settled_after_days,
        NULL::text AS card_last4,
        NULL::text AS charge_method,
        NULL::text AS charge_wallet
    FROM {{ source('stardance', 'fulfillment_payout_runs') }} r
    WHERE r.aasm_state = 'approved'
),

server_charges AS (
    -- Direct HQ card charges from hosting / CDN / DNS / dev-infra vendors.
    -- The vendor list is matched against card memos; keep the WHERE
    -- alternation and the detail CASE in sync when adding a vendor.
    -- Deliberately NOT matched: bare "AWS" (retail Amazon and e.g. "SHAWS"
    -- collide — the card memo is always "Amazon web services"). Twilio
    -- charges memoed for the HCB platform are excluded (HCB pays its own
    -- Twilio account from the bank org; this model never scans that org, but
    -- the odd HCB top-up has landed on the HQ card).
    SELECT
        'servers'::text AS cost_type,
        l.transaction_date,
        'hcb_hq'::text AS source_system,
        COALESCE(l.transaction_id::text, l.hcb_code) AS source_id,
        l.hcb_code AS source_reference,
        CASE
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'HETZNER' THEN 'Hetzner'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'CLOUDFLARE' THEN 'Cloudflare'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'AMAZON WEB' THEN 'AWS'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'DIGITALOCEAN' THEN 'DigitalOcean'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'GOOGLE CLOUD' THEN 'Google Cloud'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'VERCEL-NEON' THEN 'Neon'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'VERCEL' THEN 'Vercel'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'FLY\.IO' THEN 'Fly.io'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'RAILWAY' THEN 'Railway'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'HEROKU' THEN 'Heroku'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'NETLIFY' THEN 'Netlify'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'LINODE|AKAMAI' THEN 'Linode'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'SCALEWAY' THEN 'Scaleway'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'BACKBLAZE' THEN 'Backblaze'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'BUNNY\.NET|BUNNYCDN' THEN 'Bunny.net'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'FASTLY' THEN 'Fastly'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'DNSIMPLE' THEN 'DNSimple'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'PORKBUN' THEN 'Porkbun'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'TWILIO' THEN 'Twilio'
            -- Tight on purpose: bare GITHUB matches sticker printing runs
            -- ("GitHub will reimburse"), GitHub Universe travel, etc.
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'GITHUB,? INC|GITHUB GITHUB|^\s*GITHUB\s*$' THEN 'GitHub'
            ELSE 'Other hosting'
        END AS detail,
        ABS(l.amount_dollars)::numeric AS amount_dollars,
        COALESCE(l.requested_by_name, l.transacting_user_name, e.card_user_name) AS initiated_by_name,
        COALESCE(e.receipt_count, 0) AS receipt_count,
        COALESCE(e.receipt_marked_no_or_lost, FALSE) AS receipt_marked_no_or_lost,
        e.tag_labels,
        e.spent_date,
        e.settled_after_days,
        e.card_last4,
        e.charge_method,
        e.charge_wallet
    FROM {{ ref('ledger') }} l
    LEFT JOIN {{ ref('hcb_code_enrichment') }} e ON e.hcb_code = l.hcb_code
    WHERE l.org_slug = 'hq'
      AND l.transaction_source_type = 'RawStripeTransaction'
      AND l.flow_direction = 'outflow'
      AND NOT l.is_internal_transfer
      AND CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo)
          ~* '(HETZNER|CLOUDFLARE|AMAZON WEB|DIGITALOCEAN|GOOGLE CLOUD|VERCEL|FLY\.IO|RAILWAY|HEROKU|NETLIFY|LINODE|AKAMAI|SCALEWAY|BACKBLAZE|BUNNY\.NET|BUNNYCDN|FASTLY|DNSIMPLE|PORKBUN|TWILIO|GITHUB,? INC|GITHUB GITHUB|^\s*GITHUB\s*$)'
      -- HCB's own Twilio account is paid from the bank org; drop the rare
      -- HCB-memoed Twilio top-up that landed on the HQ card.
      AND NOT (
          CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo, l.custom_memo) ~* 'TWILIO'
          AND CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo, l.custom_memo) ~* 'HCB'
      )
),

-- OpenAI / Anthropic paid from HQ or from central YSWS orgs that sit outside
-- the YSWS spend trees (tree orgs are already counted as program/marketing
-- true spend; ysws-budget-* orgs are individual author funds counted in the
-- staff budget lines — both would double-count here). "CLAUDE" alone is not
-- matched: card memos are "ANTHROPIC* CLAUDE SUB" / "CLAUDE.AI", and bare
-- CLAUDE could be a person's name in a human memo.
ai_charges AS (
    SELECT
        'ai'::text AS cost_type,
        l.transaction_date,
        -- Not 'hcb_hq': these card rows also cover central YSWS orgs, and a
        -- distinct source_system keeps source_key collision-proof against
        -- server_charges (same transaction_id space).
        'hcb_ai'::text AS source_system,
        COALESCE(l.transaction_id::text, l.hcb_code) AS source_id,
        l.hcb_code AS source_reference,
        CASE
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'CHATGPT' THEN 'OpenAI · ChatGPT subs'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'OPENAI' THEN 'OpenAI · API & credits'
            WHEN CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo) ~* 'CLAUDE( SUB|\.AI)' THEN 'Anthropic · Claude subs'
            ELSE 'Anthropic · API'
        END AS detail,
        ABS(l.amount_dollars)::numeric AS amount_dollars,
        COALESCE(l.requested_by_name, l.transacting_user_name, e.card_user_name) AS initiated_by_name,
        COALESCE(e.receipt_count, 0) AS receipt_count,
        COALESCE(e.receipt_marked_no_or_lost, FALSE) AS receipt_marked_no_or_lost,
        e.tag_labels,
        e.spent_date,
        e.settled_after_days,
        e.card_last4,
        e.charge_method,
        e.charge_wallet
    FROM {{ ref('ledger') }} l
    LEFT JOIN {{ ref('hcb_code_enrichment') }} e ON e.hcb_code = l.hcb_code
    WHERE (
            l.org_slug = 'hq'
            OR (
                l.org_slug LIKE 'ysws-%'
                AND l.org_slug NOT LIKE 'ysws-budget-%'
                AND NOT EXISTS (
                    SELECT 1 FROM {{ ref('ysws_spend_org_tree') }} t
                    WHERE t.org_slug = l.org_slug
                )
            )
          )
      AND l.transaction_source_type = 'RawStripeTransaction'
      AND l.flow_direction = 'outflow'
      AND NOT l.is_internal_transfer
      AND CONCAT_WS(' ', l.display_memo, l.raw_memo, l.friendly_memo)
          ~* '(OPENAI|CHATGPT|ANTHROPIC|CLAUDE( SUB|\.AI))'
),

-- Since April 2026 the big monthly Hetzner invoice is paid personally and
-- reimbursed through the HCB reimbursement clearinghouse, so it never hits
-- the HQ card (the Servers line collapsed to a small fraction of its prior
-- level when that switch happened). The clearinghouse payout (HCB-300) carries the
-- report title in ach_payment_for; the per-expense charge lands on HQ as an
-- HCB-710 expense payout whose mirrored memo is only an opaque short code —
-- the reimbursement-report tables are not replicated into the warehouse.
-- So an infra-keyword payout (servers or AI credits) is tied back to HQ by
-- amount:
--   * exact:    one HQ HCB-710 row within 3 days equals the payout, or
--   * dominant: HQ's HCB-710 rows that day sum to the payout and this row is
--     >= 90% of it (mixed reports like "postage & servers!" carry a few
--     dollars of ride-along spend, which this leaves out).
-- A 50/50 mixed report would be dropped by both paths (undercount, never
-- overcount); expense-level memos verified against the HCB API 2026-07-30.
hq_expense_payouts AS (
    SELECT
        l.transaction_id,
        l.hcb_code,
        l.transaction_date,
        ABS(l.amount_dollars)::numeric AS amount_dollars,
        SUM(ABS(l.amount_dollars)::numeric)
            OVER (PARTITION BY l.transaction_date) AS same_day_total
    FROM {{ ref('ledger') }} l
    WHERE l.org_slug = 'hq'
      AND l.hcb_code LIKE 'HCB-710-%'
      AND l.flow_direction = 'outflow'
),

infra_reimbursement_payouts AS (
    SELECT
        l.transaction_date,
        ABS(l.amount_dollars)::numeric AS amount_dollars,
        l.ach_recipient_name,
        l.ach_payment_for
    FROM {{ ref('ledger') }} l
    WHERE l.org_slug = 'reimbursement-clearinghouse'
      AND l.hcb_code LIKE 'HCB-300-%'
      AND l.flow_direction = 'outflow'
      -- Bare "cloudflare" is not matched: it catches conference/travel
      -- reimbursements ("Cloudflare Connect speaking").
      AND l.ach_payment_for ~* '(hetzner|cloudflare (bill|invoice|credits?)|server|hosting|vps|openai|anthropic|chatgpt|claude)'
),

infra_reimbursements AS (
    SELECT DISTINCT ON (e.hcb_code)
        CASE
            WHEN p.ach_payment_for ~* '(openai|anthropic|chatgpt|claude)' THEN 'ai'
            ELSE 'servers'
        END::text AS cost_type,
        e.transaction_date,
        'hcb_reimbursement'::text AS source_system,
        COALESCE(e.transaction_id::text, e.hcb_code) AS source_id,
        e.hcb_code AS source_reference,
        CASE
            WHEN p.ach_payment_for ~* 'HETZNER' THEN 'Hetzner'
            WHEN p.ach_payment_for ~* 'CLOUDFLARE' THEN 'Cloudflare'
            WHEN p.ach_payment_for ~* '(OPENAI|CHATGPT)' THEN 'OpenAI · API & credits'
            WHEN p.ach_payment_for ~* '(ANTHROPIC|CLAUDE)' THEN 'Anthropic · API'
            ELSE 'Servers (reimbursed)'
        END AS detail,
        e.amount_dollars,
        p.ach_recipient_name AS initiated_by_name,
        NULL::bigint AS receipt_count,
        NULL::boolean AS receipt_marked_no_or_lost,
        NULL::text[] AS tag_labels,
        NULL::date AS spent_date,
        NULL::integer AS settled_after_days,
        NULL::text AS card_last4,
        NULL::text AS charge_method,
        NULL::text AS charge_wallet
    FROM hq_expense_payouts e
    JOIN infra_reimbursement_payouts p
      ON ABS(p.transaction_date - e.transaction_date) <= 3
     AND (
          p.amount_dollars = e.amount_dollars
          OR (p.amount_dollars = e.same_day_total AND e.amount_dollars >= 0.9 * p.amount_dollars)
     )
    ORDER BY e.hcb_code, e.transaction_date
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
    settled_after_days,
    card_last4,
    charge_method,
    charge_wallet
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
    settled_after_days,
    card_last4,
    charge_method,
    charge_wallet
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
    settled_after_days,
    card_last4,
    charge_method,
    charge_wallet
FROM server_charges

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
    settled_after_days,
    card_last4,
    charge_method,
    charge_wallet
FROM infra_reimbursements

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
    settled_after_days,
    card_last4,
    charge_method,
    charge_wallet
FROM ai_charges
