-- Each backfilled payment may produce at most one program-side offset, and
-- the offsets must net to no more than the total backfilled marketing spend.
-- Duplicated offsets would understate program spend.

WITH backfill AS (
    SELECT SUM(outflow_dollars) AS backfill_total
    FROM {{ ref('ysws_spend_ledger') }}
    WHERE spend_bucket = 'marketing_backfill'
),

offsets AS (
    SELECT hcb_code, COUNT(*) AS n, SUM(-outflow_dollars) AS offset_dollars
    FROM {{ ref('ysws_spend_ledger') }}
    WHERE hcb_code LIKE 'BACKFILL-OFFSET-%'
    GROUP BY hcb_code
)

SELECT o.hcb_code, o.n, o.offset_dollars, b.backfill_total
FROM offsets o
CROSS JOIN backfill b
WHERE o.n > 1
   OR (SELECT SUM(offset_dollars) FROM offsets) > b.backfill_total
