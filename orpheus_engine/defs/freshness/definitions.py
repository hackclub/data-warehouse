"""Per-mirror warehouse freshness metadata table and live SQL view.

Materializes ``warehouse_metadata.mirror_sync_status`` from Dagster's latest
successful ``ASSET_MATERIALIZATION`` timestamps and exposes a live SQL view
``warehouse_metadata.mirror_freshness`` that evaluates ``age_hours``,
``is_stale``, and ``status`` against ``NOW()`` at query time.

Because ``is_stale`` is computed in the view rather than frozen at write time,
every mirror still flips to stale if Dagster stops running. Both
``hc_warehouse_agent`` and ``hc_warehouse_mcp`` hold ``pg_read_all_data`` and
can query ``warehouse_metadata.mirror_freshness`` directly or via the
``check_freshness`` MCP tool.
"""

import importlib
import os
import pkgutil
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import dagster as dg
from dagster import AssetExecutionContext, Nothing
import psycopg2

from orpheus_engine.defs.sling import programs as sling_programs_pkg

FRESHNESS_SCHEMA = "warehouse_metadata"
FRESHNESS_TABLE = f"{FRESHNESS_SCHEMA}.mirror_sync_status"
FRESHNESS_VIEW = f"{FRESHNESS_SCHEMA}.mirror_freshness"


@dataclass(frozen=True)
class MirrorFreshnessSpec:
    schema_name: str
    asset_key: str
    sync_type: str
    schedule_name: str
    expected_cadence_minutes: int
    stale_after_minutes: int


# Mirrors synced on a faster cadence than the 6-hourly materialize_all_assets job.
_SLING_CADENCE_OVERRIDES: dict[str, tuple[str, int, int]] = {
    "stardance_warehouse_mirror": ("frequent_15min_schedule", 15, 60),
    "stardance_ahoy_warehouse_mirror": ("frequent_15min_schedule", 15, 60),
    "hackatime_warehouse_mirror": ("hackatime_dau_hourly_schedule", 120, 360),
}

_CORE_SLING_MIRRORS: tuple[tuple[str, str], ...] = (
    ("hackatime", "hackatime_warehouse_mirror"),
    ("hcer_public_github_data", "hcer_public_github_data_warehouse_mirror"),
    ("hcb", "hcb_warehouse_mirror"),
    ("auth", "auth_warehouse_mirror"),
)

# Non-Sling mirrors (Airtable DLT, API syncs, Loops, Fillout, Zoom, QuickBooks, Zenventory).
_NON_SLING_MIRRORS: tuple[MirrorFreshnessSpec, ...] = (
    MirrorFreshnessSpec(
        schema_name="airtable_unified_ysws_projects_db",
        asset_key="unified_ysws_approved_projects_warehouse",
        sync_type="dlt",
        schedule_name="unified_ysws_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_raw_all_bases",
        asset_key="airtable_raw_all_bases_sync",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_campfire",
        asset_key="campfire_event_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_campfire_flagship",
        asset_key="campfire_flagship_rsvps_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_daydream",
        asset_key="daydream_projects_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_daydream_ops",
        asset_key="daydream_ops_all_attendees_ever_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_hack_club_videos_db",
        asset_key="hack_club_videos_db_videos_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_slack_nps",
        asset_key="slack_nps_nps_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_sleepover",
        asset_key="sleepover_projects_warehouse",
        sync_type="dlt",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_snowglobe",
        asset_key="snowglobe_projects_warehouse",
        sync_type="dlt",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_shredder",
        asset_key="shredder_projects_warehouse",
        sync_type="dlt",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_mipmap",
        asset_key="mipmap_project_warehouse",
        sync_type="dlt",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_audit_logs",
        asset_key="airtable_audit_log_events",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="airtable_users",
        asset_key="airtable_enterprise_users",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="agh_fulfillment_zenventory",
        asset_key="agh_fulfillment_zenventory_inventory",
        sync_type="api",
        schedule_name="frequent_15min_schedule",
        expected_cadence_minutes=15,
        stale_after_minutes=60,
    ),
    MirrorFreshnessSpec(
        schema_name="loops",
        asset_key="loops_audience",
        sync_type="dlt",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="fillout",
        asset_key="fillout_sync",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
    MirrorFreshnessSpec(
        schema_name="zoom",
        asset_key="zoom_meetings",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=1440,
    ),
    MirrorFreshnessSpec(
        schema_name="quickbooks",
        asset_key="quickbooks_online_sync",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=1440,
    ),
    MirrorFreshnessSpec(
        schema_name="slack",
        asset_key="slack_member_metadata",
        sync_type="api",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    ),
)


def _extract_target_schema(replication_config: Mapping[str, Any] | None, fallback: str) -> str:
    """Derive the target warehouse schema from a Sling replication_config."""
    if not replication_config:
        return fallback
    default_obj = (replication_config.get("defaults") or {}).get("object", "")
    if isinstance(default_obj, str) and "." in default_obj:
        return default_obj.split(".", 1)[0]
    for stream_cfg in (replication_config.get("streams") or {}).values():
        if isinstance(stream_cfg, Mapping):
            obj = stream_cfg.get("object", "")
            if isinstance(obj, str) and "." in obj:
                return obj.split(".", 1)[0]
    return fallback


def build_mirror_specs() -> list[MirrorFreshnessSpec]:
    """Return every active warehouse mirror spec, sorted by schema_name."""
    by_schema: dict[str, MirrorFreshnessSpec] = {}

    def _add_sling(schema_name: str, asset_key: str) -> None:
        schedule_name, cadence, stale_after = _SLING_CADENCE_OVERRIDES.get(
            asset_key, ("materialize_all_assets", 360, 720)
        )
        by_schema[schema_name] = MirrorFreshnessSpec(
            schema_name=schema_name,
            asset_key=asset_key,
            sync_type="sling",
            schedule_name=schedule_name,
            expected_cadence_minutes=cadence,
            stale_after_minutes=stale_after,
        )

    for schema_name, asset_key in _CORE_SLING_MIRRORS:
        _add_sling(schema_name, asset_key)

    for _, mod_name, _ in pkgutil.iter_modules(sling_programs_pkg.__path__):
        mod = importlib.import_module(f"orpheus_engine.defs.sling.programs.{mod_name}")
        if getattr(mod, "__disabled__", False):
            continue
        asset_fn = getattr(mod, f"{mod_name}_warehouse_mirror", None)
        if asset_fn is None:
            continue
        repl_cfg = getattr(mod, f"{mod_name}_replication_config", None)
        schema_name = _extract_target_schema(repl_cfg, fallback=mod_name)
        _add_sling(schema_name, f"{mod_name}_warehouse_mirror")

    for spec in _NON_SLING_MIRRORS:
        by_schema.setdefault(spec.schema_name, spec)

    return [by_schema[name] for name in sorted(by_schema)]


FRESHNESS_DDL_STATEMENTS: tuple[str, ...] = (
    f"CREATE SCHEMA IF NOT EXISTS {FRESHNESS_SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {FRESHNESS_TABLE} (
        schema_name TEXT PRIMARY KEY,
        asset_key TEXT NOT NULL,
        sync_type TEXT NOT NULL,
        schedule_name TEXT NOT NULL,
        expected_cadence_minutes INTEGER NOT NULL,
        stale_after_minutes INTEGER NOT NULL,
        last_synced_at TIMESTAMPTZ,
        recorded_at TIMESTAMPTZ NOT NULL
    )
    """,
    f"""
    CREATE OR REPLACE VIEW {FRESHNESS_VIEW} AS
    SELECT
        schema_name,
        asset_key,
        sync_type,
        schedule_name,
        last_synced_at,
        ROUND((EXTRACT(EPOCH FROM (NOW() - last_synced_at)) / 3600.0)::numeric, 2) AS age_hours,
        expected_cadence_minutes,
        stale_after_minutes,
        (
            last_synced_at IS NULL
            OR NOW() - last_synced_at > make_interval(mins => stale_after_minutes)
        ) AS is_stale,
        CASE
            WHEN last_synced_at IS NULL THEN 'never_synced'
            WHEN NOW() - last_synced_at > make_interval(mins => stale_after_minutes) THEN 'stale'
            ELSE 'fresh'
        END AS status,
        recorded_at
    FROM {FRESHNESS_TABLE}
    ORDER BY is_stale DESC, age_hours DESC NULLS FIRST, schema_name
    """,
)


def fetch_latest_materializations(
    instance: dg.DagsterInstance,
    specs: Sequence[MirrorFreshnessSpec],
) -> dict[str, datetime | None]:
    """Query Dagster for the latest successful materialization UTC time per asset_key."""
    asset_keys = [dg.AssetKey(spec.asset_key) for spec in specs]
    latest = instance.get_latest_materialization_events(asset_keys)
    result: dict[str, datetime | None] = {}
    for spec in specs:
        event = latest.get(dg.AssetKey(spec.asset_key))
        if event is None or event.timestamp is None:
            result[spec.asset_key] = None
        else:
            result[spec.asset_key] = datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
    return result


def write_mirror_freshness(
    conn: Any,
    specs: Sequence[MirrorFreshnessSpec],
    materializations: Mapping[str, datetime | None],
    recorded_at: datetime | None = None,
) -> int:
    """Create/update the freshness table and view atomically in one transaction."""
    now = recorded_at or datetime.now(timezone.utc)
    rows = [
        (
            spec.schema_name,
            spec.asset_key,
            spec.sync_type,
            spec.schedule_name,
            spec.expected_cadence_minutes,
            spec.stale_after_minutes,
            materializations.get(spec.asset_key),
            now,
        )
        for spec in specs
    ]

    with conn.cursor() as cur:
        for ddl in FRESHNESS_DDL_STATEMENTS:
            cur.execute(ddl)
        cur.execute(f"DELETE FROM {FRESHNESS_TABLE}")
        cur.executemany(
            f"""
            INSERT INTO {FRESHNESS_TABLE} (
                schema_name,
                asset_key,
                sync_type,
                schedule_name,
                expected_cadence_minutes,
                stale_after_minutes,
                last_synced_at,
                recorded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    return len(rows)


@dg.asset(
    name="warehouse_mirror_freshness",
    group_name="freshness",
    compute_kind="postgres",
)
def warehouse_mirror_freshness(context: AssetExecutionContext) -> Nothing:
    """Refresh warehouse_metadata.mirror_sync_status and warehouse_metadata.mirror_freshness."""
    warehouse_url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL is required to update mirror freshness")

    specs = build_mirror_specs()
    materializations = fetch_latest_materializations(context.instance, specs)
    recorded_at = datetime.now(timezone.utc)

    conn = psycopg2.connect(warehouse_url)
    try:
        count = write_mirror_freshness(conn, specs, materializations, recorded_at=recorded_at)
    finally:
        conn.close()

    stale_count = sum(
        1
        for spec in specs
        if materializations.get(spec.asset_key) is None
        or (recorded_at - materializations[spec.asset_key]).total_seconds()
        > spec.stale_after_minutes * 60
    )
    context.log.info(
        "Updated %s with %d mirrors (%d currently stale)",
        FRESHNESS_VIEW,
        count,
        stale_count,
    )
    context.add_output_metadata(
        {
            "mirror_count": count,
            "stale_count": stale_count,
            "view": FRESHNESS_VIEW,
        }
    )
    return None


defs = dg.Definitions(assets=[warehouse_mirror_freshness])
