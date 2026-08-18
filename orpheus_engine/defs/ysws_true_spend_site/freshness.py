"""
How stale the numbers on the site are.

Two different clocks, both worth showing, because either can be the reason a
number looks wrong:

  hcb_pulled_at / hcb_data_through
      When the HCB mirror last ran, and how recent the HCB rows in the
      warehouse actually are. If the mirror has been failing, every number on
      the site is as old as this, no matter how often the models rebuild.

  recalculated_at
      When the true-spend dbt models last rebuilt from that mirror.

The pull and rebuild times are Dagster materialization facts, so they come from
the Dagster instance in production and from the prod Dagster database when
rendering a preview locally. The data watermark comes from the warehouse itself.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

# Sling asset that mirrors HCB's database into the warehouse.
HCB_MIRROR_ASSET_KEY: List[str] = ["hcb_warehouse_mirror"]
# The dbt model whose rebuild IS the recalculation behind this site.
RECALC_ASSET_KEY: List[str] = ["hcb_ysws_true_spend_analytics", "ysws_spend_ledger"]

# Newest source row we hold, across the HCB tables the true-spend models read.
# Sling replicates these incrementally on updated_at, so the max IS the sync
# watermark: anything changed in HCB after this is not here yet.
HCB_WATERMARK_SQL = """
SELECT MAX(updated_at) AS updated_at FROM (
    SELECT MAX(updated_at) AS updated_at FROM hcb.canonical_transactions
    UNION ALL SELECT MAX(updated_at) FROM hcb.events
    UNION ALL SELECT MAX(updated_at) FROM hcb.disbursements
    UNION ALL SELECT MAX(updated_at) FROM hcb.card_grants
) w
"""

# Only real materializations count. ASSET_MATERIALIZATION_PLANNED rows also
# touch asset_keys.last_materialization_timestamp, which is why that column
# reads "now" for assets that have not actually succeeded in days.
DAGSTER_MATERIALIZATION_SQL = """
SELECT asset_key, MAX(timestamp) AS last_materialized
FROM event_logs
WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
  AND asset_key = ANY(%s)
GROUP BY asset_key
"""


@dataclass
class Freshness:
    """Every timestamp the site shows about itself."""

    hcb_pulled_at: Optional[datetime] = None
    hcb_data_through: Optional[datetime] = None
    recalculated_at: Optional[datetime] = None


def _utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return None


def hcb_data_through(conn) -> Optional[datetime]:
    """Newest HCB row in the warehouse mirror."""
    with conn.cursor() as cur:
        cur.execute(HCB_WATERMARK_SQL)
        row = cur.fetchone()
    return _utc(row[0] if row else None)


def _asset_key_json(key: List[str]) -> str:
    import json

    return json.dumps(key)


def dagster_times_from_db(dagster_db_url: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """(hcb_pulled_at, recalculated_at) from a Dagster instance's Postgres."""
    import psycopg2

    keys = [_asset_key_json(HCB_MIRROR_ASSET_KEY), _asset_key_json(RECALC_ASSET_KEY)]
    conn = psycopg2.connect(dagster_db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(DAGSTER_MATERIALIZATION_SQL, (keys,))
            found = {row[0]: _utc(row[1]) for row in cur.fetchall()}
    finally:
        conn.close()
    return found.get(keys[0]), found.get(keys[1])


def dagster_times_from_instance(instance) -> Tuple[Optional[datetime], Optional[datetime]]:
    """(hcb_pulled_at, recalculated_at) from the running Dagster instance."""
    from dagster import AssetKey

    def latest(key: List[str]) -> Optional[datetime]:
        event = instance.get_latest_materialization_event(AssetKey(key))
        if event is None:
            return None
        return datetime.fromtimestamp(event.timestamp, tz=timezone.utc)

    return latest(HCB_MIRROR_ASSET_KEY), latest(RECALC_ASSET_KEY)
