{{ config(
    schema='finance_2026_analytics',
    materialized='table'
) }}

-- One row per month for a trailing 24-month window (the current month plus
-- the 23 before it), so the dashboard can show history rather than only the
-- current fiscal year. Revenue components come from the HCB ledger (fees,
-- interest, grants) and the finance team's sheet (major gifts, other revenue,
-- HQ interest). `has_sheet_month` says whether the sheet's monthly_finances
-- tab has a row for the month: expenses and "other" revenue only exist for
-- those months, so a month without one is revenue-only, not zero spend.

WITH bounds AS (
  SELECT
    (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '23 months')::date AS window_start,
    (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::date AS window_end
),

months AS (
  SELECT generate_series(window_start, window_end - INTERVAL '1 month', INTERVAL '1 month')::date AS month
  FROM bounds
),

fee_revenue AS (
  SELECT
    DATE_TRUNC('month', date)::date AS month,
    SUM(amount_cents) / 100.0 AS hcb_fee_revenue
  FROM {{ source('hcb', 'canonical_transactions') }}, bounds
  WHERE hcb_code ILIKE 'HCB-702%'
    AND (amount_cents > 0 OR date > '2024-02-26')
    AND date >= bounds.window_start
    AND date < bounds.window_end
  GROUP BY DATE_TRUNC('month', date)::date
),

-- Gift dates are parsed once, in the major_gifts model (it handles both
-- 2- and 4-digit years); rolling up from there keeps the two in agreement.
major_gifts_received AS (
  SELECT
    DATE_TRUNC('month', date)::date AS month,
    SUM(amount) AS major_gift_total
  FROM {{ ref('major_gifts') }}
  WHERE status = 'Received'
    AND date IS NOT NULL
  GROUP BY DATE_TRUNC('month', date)::date
),

revenue_awaiting_receipt AS (
  SELECT
    DATE_TRUNC('month', date)::date AS month,
    SUM(amount) AS awaiting_total
  FROM {{ ref('major_gifts') }}
  WHERE status = 'Awaiting Receipt'
    AND date IS NOT NULL
  GROUP BY DATE_TRUNC('month', date)::date
),

monthly AS (
  SELECT
    TO_DATE(month, 'Mon YYYY') AS month,
    expenses AS total_expenses,
    revenue_other,
    revenue_hq_interest
  FROM {{ source('finance_2026', 'monthly_finances') }}
  WHERE NULLIF(BTRIM(month), '') IS NOT NULL
),

hcb_exp AS (
  SELECT
    TO_DATE(month, 'Mon YYYY') AS month,
    hcb_expenses,
    hcb_revenue_from_bank_interest AS hcb_interest,
    hcb_revenue_from_grants AS hcb_grants
  FROM {{ source('finance_2026', 'hcb_expense_reporting') }}
  WHERE NULLIF(BTRIM(month), '') IS NOT NULL
)

SELECT
  TO_CHAR(mo.month, 'Mon YYYY') AS month,
  mo.month AS month_start,
  (m.month IS NOT NULL) AS has_sheet_month,

  COALESCE(f.hcb_fee_revenue, 0)
    + COALESCE(g.major_gift_total, 0)
    + COALESCE(m.revenue_other, 0)
    + COALESCE(m.revenue_hq_interest, 0)
    + COALESCE(h.hcb_interest, 0)
    + COALESCE(h.hcb_grants, 0)
    AS revenue_total,

  COALESCE(f.hcb_fee_revenue, 0)
    + COALESCE(h.hcb_interest, 0)
    + COALESCE(h.hcb_grants, 0)
    AS revenue_hcb,

  COALESCE(g.major_gift_total, 0) AS revenue_major_gifts,
  COALESCE(m.revenue_other, 0) AS revenue_other,
  COALESCE(m.revenue_hq_interest, 0) + COALESCE(h.hcb_interest, 0) AS revenue_interest,
  COALESCE(a.awaiting_total, 0) AS revenue_awaiting_receipt,

  COALESCE(h.hcb_expenses, 0) AS expenses_hcb,
  COALESCE(m.total_expenses, 0) - COALESCE(h.hcb_expenses, 0) AS expenses_everything_else,
  COALESCE(m.total_expenses, 0) AS expenses_total,

  COALESCE(f.hcb_fee_revenue, 0)
    + COALESCE(g.major_gift_total, 0)
    + COALESCE(m.revenue_other, 0)
    + COALESCE(m.revenue_hq_interest, 0)
    + COALESCE(h.hcb_interest, 0)
    + COALESCE(h.hcb_grants, 0)
    - COALESCE(m.total_expenses, 0)
    AS net_revenue

FROM months mo
LEFT JOIN monthly m ON m.month = mo.month
LEFT JOIN fee_revenue f ON f.month = mo.month
LEFT JOIN major_gifts_received g ON g.month = mo.month
LEFT JOIN hcb_exp h ON h.month = mo.month
LEFT JOIN revenue_awaiting_receipt a ON a.month = mo.month
ORDER BY mo.month
