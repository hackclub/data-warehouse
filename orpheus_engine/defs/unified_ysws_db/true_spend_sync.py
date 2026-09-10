"""Sync the website's canonical HCB-root costs, without redoing its accounting.

Several Airtable program versions can share a root. Each gets the same root
spend, hours and rate shown on the website, NOT an invented per-version split.
Keep this writer downstream of dbt and outside the pre-warehouse processing
marker: feeding it back into that marker would create a Dagster asset cycle.
"""
from contextlib import closing
from datetime import datetime, timezone
import os
from urllib.parse import urlparse

import polars as pl
import psycopg2
from dagster import AssetKey, AssetExecutionContext, Output, asset

from ..airtable.generated_ids import AirtableIDs

PROGRAMS = AirtableIDs.unified_ysws_projects_db.ysws_programs
SPEND_FIELD = PROGRAMS.total_spent_from_hcb_fund
# Airtable accepts field names in updates. Resolve these approved schema fields
# at execution time so no fabricated field IDs enter the generated IDs file.
HOURS_FIELD = "True Spend — Weighted Hours"
RATE_FIELD = "True Spend — Cost Per Weighted Hour"
SYNC_FIELDS = (SPEND_FIELD, HOURS_FIELD, RATE_FIELD)
SOURCE_ASSET = AssetKey(["hcb_ysws_true_spend_analytics", "ysws_spend_by_program"])
SOURCE_SQL = """
SELECT root_slug, member_ids, true_spend_dollars, weighted_hours,
       cost_per_weighted_hour
FROM public_hcb_ysws_true_spend_analytics.ysws_spend_by_program
WHERE is_ysws_program
"""


def _root_slug(url):
    if not isinstance(url, str):
        return None
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "hcb.hackclub.com":
        return None
    return parsed.path.strip("/").split("/")[0] or None


def build_true_spend_updates(programs: pl.DataFrame, costs: pl.DataFrame) -> pl.DataFrame:
    """Exact record-ID + current HCB-link match; unknowns clear stale values.

    Zero spend is a real zero. Missing/zero hours keep the mart's NULL rate.
    No gross-outflow fallback, postage estimate, or local rate calculation.
    """
    if costs.is_empty():
        raise ValueError("True-spend source is empty; refusing to clear all Airtable costs")
    if programs["id"].n_unique() != programs.height:
        raise ValueError("Duplicate Airtable program IDs")
    by_id = {}
    for cost in costs.iter_rows(named=True):
        for member in cost["member_ids"] or []:
            if member in by_id:
                raise ValueError("Program belongs to multiple canonical HCB roots")
            by_id[member] = cost
    rows = []
    for program in programs.iter_rows(named=True):
        cost = by_id.get(program["id"])
        if cost is not None and _root_slug(program.get(PROGRAMS.hcb)) != cost["root_slug"]:
            cost = None  # Link changed since the warehouse snapshot: never use the old root.
        rows.append({
            "id": program["id"],
            SPEND_FIELD: float(cost["true_spend_dollars"]) if cost else None,
            HOURS_FIELD: float(cost["weighted_hours"]) if cost and cost["weighted_hours"] is not None else None,
            RATE_FIELD: float(cost["cost_per_weighted_hour"]) if cost and cost["cost_per_weighted_hour"] is not None else None,
        })
    return pl.DataFrame(rows, schema={"id": pl.String, **{f: pl.Float64 for f in SYNC_FIELDS}})


@asset(
    group_name="ysws_true_spend_sync",
    description="Read canonical true spend and weighted-hour cost from the same mart as the website.",
    compute_kind="warehouse_query",
    deps=[SOURCE_ASSET],
    required_resource_keys={"airtable"},
)
def ysws_programs_hcb_stats(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    event = context.instance.get_latest_materialization_event(SOURCE_ASSET)
    if event is None or datetime.now(timezone.utc).timestamp() - event.timestamp > 36 * 3600:
        raise ValueError("True-spend mart has no successful materialization within 36 hours")
    programs = context.resources.airtable.get_all_records_as_polars(
        context=context, base_key="unified_ysws_projects_db", table_key="ysws_programs",
    )
    with closing(psycopg2.connect(
        os.environ["WAREHOUSE_COOLIFY_URL"], connect_timeout=15,
        options="-c statement_timeout=120000 -c lock_timeout=10000",
    )) as conn:
        costs = pl.read_database(SOURCE_SQL, conn)
    updates = build_true_spend_updates(programs, costs)
    return Output(updates, metadata={
        "programs": updates.height,
        "matched": updates[SPEND_FIELD].is_not_null().sum(),
        "missing_mapping": updates[SPEND_FIELD].is_null().sum(),
        "source_materialized_at": datetime.fromtimestamp(event.timestamp, timezone.utc).isoformat(),
        "source": "public_hcb_ysws_true_spend_analytics.ysws_spend_by_program",
    })


def write_true_spend_updates(table, updates: pl.DataFrame) -> int:
    schema = table.schema(force=True)
    fields = {f.name: f.id for f in schema.fields}
    for name in (HOURS_FIELD, RATE_FIELD):
        if name not in fields:
            raise ValueError(f"Create the approved Airtable field first: {name}")
    by_id = {f.id: f for f in schema.fields}
    expected_references = {
        PROGRAMS.cost_per_hour: {fields[HOURS_FIELD], fields[RATE_FIELD]},
        PROGRAMS.total_spend: {SPEND_FIELD},
    }
    for field_id, expected in expected_references.items():
        field = by_id.get(field_id)
        actual = set(getattr(getattr(field, "options", None), "referenced_field_ids", None) or [])
        if actual != expected:
            raise ValueError("Update Airtable Cost Per Hour and Total Spend formulas to canonical true spend before syncing")
    # Explicit nulls are intentional clears. The generic resource helper drops
    # None, which would leave old rates after hours or HCB mappings disappear.
    records = [{"id": row["id"], "fields": {
        fields.get(k, k): row[k] for k in SYNC_FIELDS
    }} for row in updates.iter_rows(named=True)]
    if not records:
        return 0
    result = table.batch_update(records)
    if len(result) != len(records):
        raise RuntimeError("Incomplete true-spend Airtable update")
    return len(result)


@asset(
    group_name="ysws_true_spend_sync",
    description="Update Airtable costs after true-spend dbt, separate from pre-dbt YSWS processing.",
    compute_kind="airtable_update",
    required_resource_keys={"airtable"},
)
def ysws_programs_true_spend_update_status(
    context: AssetExecutionContext, ysws_programs_hcb_stats: pl.DataFrame,
) -> Output[None]:
    table = context.resources.airtable.get_table("unified_ysws_projects_db", "ysws_programs")
    count = write_true_spend_updates(table, ysws_programs_hcb_stats)
    return Output(None, metadata={"updates_successful": count})
