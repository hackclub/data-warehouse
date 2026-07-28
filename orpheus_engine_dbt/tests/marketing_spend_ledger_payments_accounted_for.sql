-- Every non-ignored Videos DB payment must be accounted for in the spend
-- ledger exactly one way: either it matched a real marketing-org transaction
-- (and was skipped), or it appears as a synthetic backfill row. A failure
-- means payments are being silently dropped or double-entered.

SELECT
    p.airtable_record_id,
    p.marketing_org_match_method,
    COUNT(sl.ledger_id) AS synthetic_rows
FROM {{ ref('marketing_videos_db_payments') }} p
LEFT JOIN {{ ref('marketing_spend_ledger') }} sl
    ON sl.ledger_id = 'synthetic:airtable:' || p.airtable_record_id
WHERE NOT p.is_ignored
GROUP BY p.airtable_record_id, p.marketing_org_match_method
HAVING COUNT(sl.ledger_id) <> CASE WHEN p.marketing_org_match_method IS NULL THEN 1 ELSE 0 END
