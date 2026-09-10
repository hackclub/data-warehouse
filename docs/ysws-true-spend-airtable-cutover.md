# YSWS true-spend Airtable cutover

The website and YSWS cost sync both read
`public_hcb_ysws_true_spend_analytics.ysws_spend_by_program`.
Do not calculate cost from HCB raised minus balance, add estimated postage, or
redivide by an individual Airtable row's hours. Several program versions may
share one HCB root; the website reports their combined spend/hours/rate.
The sync uses Airtable record IDs in `member_ids`, not program names, and checks
that the current HCB link still matches the snapshot's root.

## One-time Airtable schema change

Replace the existing columns in place. **Do not create additional fields.**
The previous two-field approval request is superseded and should not be approved.

Keep `Total Spent From HCB Fund` as the writable spend column; it now receives
canonical **true spend**, not gross HCB outflow.

Convert the existing **Cost Per Hour** field from formula to currency (2 decimal
places), preserving its name and field ID. The pipeline writes the mart's
`cost_per_weighted_hour` directly into this field. This keeps shared-root rates
identical to the website without changing existing project-hour rollups.
Before conversion, save the old formula for rollback:

```text
IF(AND({Total Spend}, {Weighted–Total} > 0), {Total Spend} / {Weighted–Total} / 10)
```

Change the existing **Total Spend** formula (keep its field ID):

```text
IF(({Total Spent From HCB Fund} & "") != "", {Total Spent From HCB Fund}, BLANK())
```

The string check preserves a real zero without turning missing mappings into
zero spend. Keep the old postage inputs for reference; the formula must no
longer add the estimate on top of the reconciled ledger.

## Deployment / verification

1. Confirm browser access to edit the existing Airtable fields.
2. Deploy the code: the legacy gross-spend writer is removed. The new writer
   refuses writes until Cost Per Hour is writable and Total Spend no longer adds postage.
3. Convert Cost Per Hour and update Total Spend in Airtable's UI (the API cannot change field types or edit formulas).
4. Materialize `ysws_programs_hcb_stats` and
   `ysws_programs_true_spend_update_status`. No unrelated signup or project
   processing needs to run for this cutover.
5. Reconcile every matched record's spend and displayed cost/hour against
   its canonical mart row. Check zero spend, zero/missing hours, removed HCB
   links, and shared roots. Unknown mappings clear old derived numbers to blank;
   they never fall back to gross spend.

The new sync is selected in the existing 15-minute Unified YSWS job. It reads
the latest successful true-spend snapshot, requiring a materialization within
36 hours. It does not trigger a heavy HCB mirror/dbt rebuild every 15 minutes.
It is deliberately outside `ysws_programs_prepared_for_update` and
`unified_ysws_db_processing_done`: those lead to the source warehouse refresh
that dbt reads, so making them depend on the true-spend output creates a cycle.

Do not claim live reconciliation is complete until schema, formulas, production
materialization, and read-back are all verified.
