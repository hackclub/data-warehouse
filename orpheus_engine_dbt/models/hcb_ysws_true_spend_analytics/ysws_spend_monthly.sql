{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS true spend by program and month — spend_category grain for time-series
    dashboards and date-windowed reporting (e.g. fiscal-year true spend:
    SUM(outflow_dollars) WHERE is_true_spend AND month BETWEEN ...).
*/

SELECT
    program_name,
    bucket,
    root_event_id,
    root_slug,
    date_trunc('month', transaction_date)::date AS month,
    spend_category,
    ROUND(SUM(outflow_dollars)::numeric, 2) AS outflow_dollars,
    ROUND(SUM(outflow_dollars) FILTER (WHERE is_true_spend)::numeric, 2) AS true_spend_dollars,
    COUNT(*) AS transaction_count
FROM {{ ref('ysws_spend_ledger') }}
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY month, program_name, spend_category
