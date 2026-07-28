-- Every non-ignored Videos DB payment must be accounted for in the spend
-- ledger exactly one way: either it matched a real marketing-tree transaction
-- (and was skipped), or it appears as exactly one synthetic BACKFILL- row.
-- A failure means payments are being silently dropped or double-entered.

SELECT
    p.airtable_record_id,
    p.marketing_org_match_method,
    COUNT(sl.hcb_code) AS backfill_rows
FROM {{ ref('marketing_videos_db_payments') }} p
LEFT JOIN {{ ref('ysws_spend_ledger') }} sl
    ON sl.hcb_code = 'BACKFILL-' || p.airtable_record_id
   AND sl.spend_bucket = 'marketing_backfill'
WHERE NOT p.is_ignored
GROUP BY p.airtable_record_id, p.marketing_org_match_method
HAVING COUNT(sl.hcb_code) <> CASE WHEN p.marketing_org_match_method IS NULL THEN 1 ELSE 0 END
