-- Every row in the marketing spend ledger must have a unique ledger_id;
-- duplicates mean the same spend is being counted twice.

SELECT ledger_id, COUNT(*) AS n
FROM {{ ref('spend_ledger') }}
GROUP BY ledger_id
HAVING COUNT(*) > 1
