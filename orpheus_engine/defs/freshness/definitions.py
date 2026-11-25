"""Per-asset and per-schema warehouse freshness tables and live SQL views.

Materializes ``warehouse_metadata.asset_sync_status`` from Dagster's latest
successful ``ASSET_MATERIALIZATION`` timestamps (plus Fivetran's
``_fivetran_log`` metadata and explicit untracked entries for unmanaged
warehouse schemas) and exposes two live SQL views:

- ``warehouse_metadata.asset_freshness``: one row per tracked asset (or
  untracked schema), evaluating ``age_hours``, ``is_stale``, and ``status``
  against ``NOW()`` at query time.
- ``warehouse_metadata.mirror_freshness``: one row per warehouse schema using
  a worst-of rollup across all assets writing to that schema, so a schema with
  even one stale or never-synced asset is flagged ``is_stale = TRUE``.

Every Dagster asset's expected cadence is derived directly from the job
selections in ``orpheus_engine.schedules`` rather than hand-maintained.
"""

import hashlib
import importlib
import os
import pkgutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import dagster as dg
from dagster import AssetExecutionContext, Nothing
import psycopg2

from orpheus_engine.defs.sling import programs as sling_programs_pkg

FRESHNESS_SCHEMA = "warehouse_metadata"
FRESHNESS_TABLE = f"{FRESHNESS_SCHEMA}.asset_sync_status"
ASSET_FRESHNESS_VIEW = f"{FRESHNESS_SCHEMA}.asset_freshness"
FRESHNESS_VIEW = f"{FRESHNESS_SCHEMA}.mirror_freshness"

# Schedules in orpheus_engine.schedules, fastest first. An asset in several
# selections takes the first (fastest) one. The cadence must equal the schedule's
# cron interval; test_scheduled_cadences_match_cron enforces that.
# (schedule attribute name, expected_cadence_minutes, default_stale_after_minutes)
_SCHEDULED_JOB_CADENCES: tuple[tuple[str, int, int], ...] = (
    ("frequent_15min_schedule", 15, 60),
    ("unified_ysws_15min_schedule", 15, 60),
    ("hackatime_dau_hourly_schedule", 120, 360),
    ("project_search_2h_schedule", 120, 360),
    ("materialize_all_assets_schedule", 360, 720),
)

# Keep the freshness write from queueing agents' reads behind it, or hanging.
_WAREHOUSE_CONNECT_OPTIONS = "-c lock_timeout=10000 -c statement_timeout=120000"

# Assets that hit external daily API quotas or have daily-tolerant stale windows
# while still running inside their derived schedule.
_DAILY_TOLERANCE_STALE_MINUTES = 1440
_DAILY_TOLERANCE_PREFIXES: tuple[str, ...] = ("zoom_", "quickbooks_")

_CORE_SLING_MIRRORS: tuple[tuple[str, str], ...] = (
    ("hackatime", "hackatime_warehouse_mirror"),
    ("hcer_public_github_data", "hcer_public_github_data_warehouse_mirror"),
    ("hcb", "hcb_warehouse_mirror"),
    ("auth", "auth_warehouse_mirror"),
)

# Map Dagster asset group names to target warehouse schemas for multi-asset groups.
_GROUP_TO_SCHEMA: dict[str, tuple[str, str]] = {
    "dlt_airtable_unified_ysws": ("airtable_unified_ysws_projects_db", "dlt"),
    "agh_fulfillment_zenventory": ("agh_fulfillment_zenventory", "api"),
    "airtable_raw_all_bases": ("airtable_raw_all_bases", "api"),
    "fillout": ("fillout", "api"),
    "quickbooks": ("quickbooks", "api"),
    "zoom": ("zoom", "api"),
}

# Individual non-Sling, non-group-mapped warehouse-writing assets -> (schema, sync_type).
_EXPLICIT_ASSET_TO_SCHEMA: dict[str, tuple[str, str]] = {
    "airtable_audit_log_events": ("airtable_audit_logs", "api"),
    "airtable_enterprise_users": ("airtable_users", "api"),
    "loops_audience": ("loops", "dlt"),
    "loops_audience_mailing_lists_to_warehouse": ("loops", "api"),
    "loops_campaigns_to_warehouse": ("loops", "api"),
    "loops_campaign_contents_to_warehouse": ("loops", "api"),
    "loops_campaign_email_contents": ("loops", "api"),
    "loops_campaign_metrics_to_warehouse": ("loops", "api"),
    "loops_campaign_publishable_content_to_warehouse": ("loops", "api"),
    "loops_mailing_lists_to_warehouse": ("loops", "api"),
    "slack_audit_logs": ("slack", "api"),
    "slack_member_metadata": ("slack", "api"),
    "slack_public_channel_analytics": ("slack", "api"),
    "slack_public_channel_metadata": ("slack", "api"),
}

# Explicit reasons for warehouse schemas not managed by a scheduled Dagster sync.
_KNOWN_UNTRACKED_REASONS: dict[str, str] = {
    "highway_github": "Manual-only one-time backfill (Highway ended Oct 2025; excluded from scheduled jobs)",
    "metabase_user_uploads": "Metabase user-uploaded tables (not synced by Dagster)",
    "moonshot": "Historical/legacy schema (not synced by Dagster)",
    "public": "Warehouse top-level views and Postgres extension views (not a mirror)",
    "review": "Retired mirror (source DB decommissioned; disabled in Dagster)",
    "warehouse_metadata": "Internal warehouse freshness metadata schema",
}

_FIVETRAN_CADENCE_MINUTES = 360
_FIVETRAN_STALE_MINUTES = 1440


@dataclass(frozen=True)
class AssetFreshnessSpec:
    schema_name: str
    asset_key: str  # slash-joined for multi-part keys (e.g. "unified_analytics/events")
    sync_type: str  # sling | dlt | api | dbt | fivetran | untracked
    schedule_name: str
    expected_cadence_minutes: int
    stale_after_minutes: int
    tracking_status: str = "tracked"  # tracked | untracked
    untracked_reason: str | None = None

    def dagster_asset_key(self) -> dg.AssetKey:
        return dg.AssetKey(self.asset_key.split("/"))


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


def _discover_dbt_model_assets() -> list[tuple[str, str]]:
    """Return (public_<dir>, '<dir>/<model>') pairs from orpheus_engine_dbt/models/."""
    models_root = Path(__file__).resolve().parents[3] / "orpheus_engine_dbt" / "models"
    if not models_root.is_dir():
        return []
    pairs: list[tuple[str, str]] = []
    for subdir in sorted(models_root.iterdir()):
        if not subdir.is_dir() or subdir.name in ("sources", "__pycache__"):
            continue
        schema_name = f"public_{subdir.name}"
        for sql_file in sorted(subdir.glob("*.sql")):
            pairs.append((schema_name, f"{subdir.name}/{sql_file.stem}"))
    return pairs


def resolve_asset_schedule_map(
    defs: dg.Definitions | None = None,
) -> dict[str, tuple[str, int, int]]:
    """Derive (schedule_name, expected_cadence_minutes, stale_after_minutes) per asset key
    directly from the job selections in orpheus_engine.schedules."""
    from orpheus_engine import schedules

    if defs is None:
        import orpheus_engine.definitions as defs_mod

        defs = defs_mod.defs

    asset_graph = defs.resolve_asset_graph()

    # dbt assets are not loaded locally without WAREHOUSE_COOLIFY_URL, so the
    # selections above miss them; name the scheduled ones explicitly.
    frequent_dbt_keys = {"/".join(k.path) for k in schedules.FREQUENT_DBT_ASSETS}
    dau_dbt_keys = {"/".join(k.path) for k in schedules.HACKATIME_DAU_DBT_ASSETS}

    schedule_map: dict[str, tuple[str, int, int]] = {}
    for sched_attr, cadence, stale_after in _SCHEDULED_JOB_CADENCES:
        schedule_def = getattr(schedules, sched_attr)
        sched_name = schedule_def.name
        resolved = schedule_def.job.selection.resolve(asset_graph, allow_missing=True)
        for key in resolved:
            key_str = "/".join(key.path)
            if key_str in schedule_map:
                continue
            effective_stale = (
                _DAILY_TOLERANCE_STALE_MINUTES
                if key_str.startswith(_DAILY_TOLERANCE_PREFIXES)
                else stale_after
            )
            schedule_map[key_str] = (sched_name, cadence, effective_stale)

    cadence_by_attr = {attr: (cad, stale) for attr, cad, stale in _SCHEDULED_JOB_CADENCES}
    for keys, sched_attr in (
        (frequent_dbt_keys, "frequent_15min_schedule"),
        (dau_dbt_keys, "hackatime_dau_hourly_schedule"),
        ({key for _, key in _discover_dbt_model_assets()}, "materialize_all_assets_schedule"),
    ):
        sched_name = getattr(schedules, sched_attr).name
        cadence, stale_after = cadence_by_attr[sched_attr]
        for key_str in sorted(keys):
            schedule_map.setdefault(key_str, (sched_name, cadence, stale_after))

    return schedule_map


def build_asset_specs(defs: dg.Definitions | None = None) -> list[AssetFreshnessSpec]:
    """Return every tracked warehouse-writing asset spec, with cadences derived from
    the actual Dagster job selections."""
    from orpheus_engine.schedules import EXCLUDED_FROM_MAIN_JOB

    if defs is None:
        import orpheus_engine.definitions as defs_mod

        defs = defs_mod.defs

    schedule_map = resolve_asset_schedule_map(defs)
    excluded_keys = {"/".join(k.path) for k in EXCLUDED_FROM_MAIN_JOB}

    # Collect (schema_name, asset_key, sync_type) tuples.
    entries: dict[tuple[str, str], str] = {}

    # 1. Core + program Sling mirrors.
    for schema_name, asset_key in _CORE_SLING_MIRRORS:
        entries[(schema_name, asset_key)] = "sling"

    for _, mod_name, _ in pkgutil.iter_modules(sling_programs_pkg.__path__):
        mod = importlib.import_module(f"orpheus_engine.defs.sling.programs.{mod_name}")
        if getattr(mod, "__disabled__", False):
            continue
        if getattr(mod, f"{mod_name}_warehouse_mirror", None) is None:
            continue
        repl_cfg = getattr(mod, f"{mod_name}_replication_config", None)
        schema_name = _extract_target_schema(repl_cfg, fallback=mod_name)
        entries[(schema_name, f"{mod_name}_warehouse_mirror")] = "sling"

    # 2. All other warehouse-writing Dagster assets (dlt Airtable, Zenventory, Loops,
    #    Fillout, Zoom, QuickBooks, Slack, Airtable raw/audit/users).
    asset_graph = defs.resolve_asset_graph()
    for asset in defs.assets:
        for key in asset.keys if hasattr(asset, "keys") else [asset.key]:
            key_str = "/".join(key.path)
            if key_str in excluded_keys:
                continue
            group = asset_graph.get(key).group_name or ""

            if key_str in _EXPLICIT_ASSET_TO_SCHEMA:
                schema_name, sync_type = _EXPLICIT_ASSET_TO_SCHEMA[key_str]
                entries[(schema_name, key_str)] = sync_type
            elif group in _GROUP_TO_SCHEMA:
                schema_name, sync_type = _GROUP_TO_SCHEMA[group]
                entries[(schema_name, key_str)] = sync_type
            elif group.startswith("dlt_airtable_"):
                schema_name = group[len("dlt_"):]  # e.g. dlt_airtable_campfire -> airtable_campfire
                entries[(schema_name, key_str)] = "dlt"

    # 3. dbt mart models (public_<dir> schemas).
    for schema_name, key_str in _discover_dbt_model_assets():
        entries[(schema_name, key_str)] = "dbt"

    specs: list[AssetFreshnessSpec] = []
    for (schema_name, asset_key), sync_type in sorted(entries.items()):
        sched_name, cadence, stale_after = schedule_map.get(
            asset_key, ("materialize_all_assets", 360, 720)
        )
        specs.append(
            AssetFreshnessSpec(
                schema_name=schema_name,
                asset_key=asset_key,
                sync_type=sync_type,
                schedule_name=sched_name,
                expected_cadence_minutes=cadence,
                stale_after_minutes=stale_after,
            )
        )
    return specs


FRESHNESS_DDL_STATEMENTS: tuple[str, ...] = (
    f"CREATE SCHEMA IF NOT EXISTS {FRESHNESS_SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {FRESHNESS_TABLE} (
        schema_name TEXT NOT NULL,
        asset_key TEXT NOT NULL,
        sync_type TEXT NOT NULL,
        schedule_name TEXT NOT NULL,
        expected_cadence_minutes INTEGER NOT NULL,
        stale_after_minutes INTEGER NOT NULL,
        last_synced_at TIMESTAMPTZ,
        tracking_status TEXT NOT NULL DEFAULT 'tracked',
        untracked_reason TEXT,
        recorded_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (schema_name, asset_key)
    )
    """,
)

# The views are only rebuilt when this text changes (see _ensure_freshness_views),
# not on every 15-minute run: each rebuild takes an ACCESS EXCLUSIVE lock.
FRESHNESS_VIEW_DDL_STATEMENTS: tuple[str, ...] = (
    f"""
    CREATE VIEW {ASSET_FRESHNESS_VIEW} AS
    SELECT
        schema_name,
        asset_key,
        sync_type,
        schedule_name,
        tracking_status,
        untracked_reason,
        last_synced_at,
        CASE
            WHEN tracking_status = 'untracked' OR last_synced_at IS NULL THEN NULL
            ELSE ROUND((EXTRACT(EPOCH FROM (NOW() - last_synced_at)) / 3600.0)::numeric, 2)
        END AS age_hours,
        expected_cadence_minutes,
        stale_after_minutes,
        CASE
            WHEN tracking_status = 'untracked' THEN NULL
            WHEN last_synced_at IS NULL THEN TRUE
            ELSE (NOW() - last_synced_at > make_interval(mins => stale_after_minutes))
        END AS is_stale,
        CASE
            WHEN tracking_status = 'untracked' THEN 'untracked'
            WHEN last_synced_at IS NULL THEN 'never_synced'
            WHEN NOW() - last_synced_at > make_interval(mins => stale_after_minutes) THEN 'stale'
            ELSE 'fresh'
        END AS status,
        recorded_at
    FROM {FRESHNESS_TABLE}
    ORDER BY
        CASE
            WHEN tracking_status = 'untracked' THEN 2
            WHEN last_synced_at IS NULL OR NOW() - last_synced_at > make_interval(mins => stale_after_minutes) THEN 0
            ELSE 1
        END,
        age_hours DESC NULLS FIRST,
        schema_name,
        asset_key
    """,
    f"""
    CREATE VIEW {FRESHNESS_VIEW} AS
    WITH ranked AS (
        SELECT
            af.*,
            ROW_NUMBER() OVER (
                PARTITION BY af.schema_name
                ORDER BY
                    CASE af.status
                        WHEN 'never_synced' THEN 0
                        WHEN 'stale' THEN 1
                        WHEN 'fresh' THEN 2
                        ELSE 3
                    END,
                    af.age_hours DESC NULLS FIRST,
                    af.asset_key
            ) AS worst_rank
        FROM {ASSET_FRESHNESS_VIEW} af
    ),
    rolled AS (
        SELECT
            schema_name,
            COUNT(*) FILTER (WHERE tracking_status = 'tracked')::integer AS asset_count,
            COUNT(*) FILTER (WHERE tracking_status = 'tracked' AND is_stale IS TRUE)::integer AS stale_asset_count,
            MIN(last_synced_at) FILTER (WHERE tracking_status = 'tracked') AS oldest_synced_at,
            MAX(last_synced_at) FILTER (WHERE tracking_status = 'tracked') AS newest_synced_at,
            BOOL_OR(is_stale) FILTER (WHERE tracking_status = 'tracked') AS any_stale,
            BOOL_OR(status = 'never_synced') FILTER (WHERE tracking_status = 'tracked') AS any_never_synced,
            BOOL_OR(status = 'stale') FILTER (WHERE tracking_status = 'tracked') AS any_synced_stale,
            BOOL_AND(tracking_status = 'untracked') AS all_untracked,
            MAX(untracked_reason) AS untracked_reason,
            MAX(recorded_at) AS recorded_at
        FROM {ASSET_FRESHNESS_VIEW}
        GROUP BY schema_name
    )
    SELECT
        r.schema_name,
        w.asset_key AS worst_asset_key,
        w.sync_type,
        w.schedule_name,
        r.asset_count,
        r.stale_asset_count,
        CASE
            WHEN r.all_untracked THEN NULL
            WHEN r.any_never_synced THEN NULL
            ELSE r.oldest_synced_at
        END AS last_synced_at,
        r.newest_synced_at,
        w.age_hours,
        w.expected_cadence_minutes,
        w.stale_after_minutes,
        CASE
            WHEN r.all_untracked THEN NULL
            ELSE COALESCE(r.any_stale, TRUE)
        END AS is_stale,
        CASE
            WHEN r.all_untracked THEN 'untracked'
            WHEN r.any_never_synced AND NOT COALESCE(r.any_synced_stale, FALSE) THEN 'never_synced'
            WHEN COALESCE(r.any_stale, TRUE) THEN 'stale'
            ELSE 'fresh'
        END AS status,
        r.untracked_reason,
        r.recorded_at
    FROM rolled r
    JOIN ranked w ON w.schema_name = r.schema_name AND w.worst_rank = 1
    ORDER BY
        CASE
            WHEN r.all_untracked THEN 2
            WHEN COALESCE(r.any_stale, TRUE) THEN 0
            ELSE 1
        END,
        w.age_hours DESC NULLS FIRST,
        r.schema_name
    """,
)


def fetch_latest_materializations(
    instance: dg.DagsterInstance,
    specs: Sequence[AssetFreshnessSpec],
) -> dict[str, datetime | None]:
    """Query Dagster for the latest successful materialization UTC time per asset_key."""
    dagster_specs = [s for s in specs if s.tracking_status == "tracked" and s.sync_type != "fivetran"]
    asset_keys = [spec.dagster_asset_key() for spec in dagster_specs]
    if not asset_keys:
        return {}
    latest = instance.get_latest_materialization_events(asset_keys)
    result: dict[str, datetime | None] = {}
    for spec in dagster_specs:
        event = latest.get(spec.dagster_asset_key())
        if event is None or event.timestamp is None:
            result[spec.asset_key] = None
        else:
            result[spec.asset_key] = datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
    return result


def discover_warehouse_extra_specs(
    conn: Any,
    tracked_schemas: set[str],
) -> tuple[list[AssetFreshnessSpec], dict[str, datetime | None]]:
    """Inspect the warehouse Postgres read-only for:
    1. Fivetran-managed schemas (reading _fivetran_log.connection / log when present), and
    2. Any remaining non-system schemas with tables, recorded explicitly as untracked.
    """
    extra_specs: list[AssetFreshnessSpec] = []
    extra_times: dict[str, datetime | None] = {}
    seen = set(tracked_schemas)

    with conn.cursor() as cur:
        # 1. Discover all non-system schemas that hold at least one table or view.
        cur.execute(
            """
            SELECT DISTINCT table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND table_schema NOT LIKE 'pg_temp_%'
              AND table_schema NOT LIKE 'pg_toast_temp_%'
            ORDER BY table_schema
            """
        )
        all_schemas = [row[0] for row in cur.fetchall()]

        # 2. Check Fivetran sync metadata if _fivetran_log.connection + log exist.
        cur.execute(
            """
            SELECT to_regclass('_fivetran_log.connection') IS NOT NULL
               AND to_regclass('_fivetran_log.log') IS NOT NULL
            """
        )
        has_fivetran_log = bool(cur.fetchone()[0])

        fivetran_schemas: dict[str, tuple[bool, datetime | None]] = {}
        if has_fivetran_log:
            cur.execute(
                """
                SELECT
                    split_part(c.connection_name, '.', 1) AS schema_name,
                    BOOL_AND(c.paused) AS all_paused,
                    MAX(l.time_stamp) FILTER (WHERE l.message_event = 'sync_end') AS last_sync_end
                FROM _fivetran_log.connection c
                LEFT JOIN _fivetran_log.log l ON l.connection_id = c.connection_id
                GROUP BY 1
                """
            )
            for sch, all_paused, last_sync in cur.fetchall():
                if sch:
                    fivetran_schemas[sch] = (bool(all_paused), last_sync)

        # Also identify legacy Fivetran-created schemas via _fivetran_synced column.
        cur.execute(
            """
            SELECT DISTINCT table_schema
            FROM information_schema.columns
            WHERE column_name = '_fivetran_synced'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            """
        )
        fivetran_synced_schemas = {row[0] for row in cur.fetchall()}

    for schema_name in all_schemas:
        if schema_name in seen:
            continue
        if schema_name in fivetran_schemas:
            all_paused, last_sync = fivetran_schemas[schema_name]
            asset_key = f"fivetran/{schema_name}"
            if all_paused:
                extra_specs.append(
                    AssetFreshnessSpec(
                        schema_name=schema_name,
                        asset_key=asset_key,
                        sync_type="fivetran",
                        schedule_name="fivetran_paused",
                        expected_cadence_minutes=0,
                        stale_after_minutes=0,
                        tracking_status="untracked",
                        untracked_reason="Paused Fivetran connector (historical snapshot)",
                    )
                )
            else:
                extra_specs.append(
                    AssetFreshnessSpec(
                        schema_name=schema_name,
                        asset_key=asset_key,
                        sync_type="fivetran",
                        schedule_name="fivetran",
                        expected_cadence_minutes=_FIVETRAN_CADENCE_MINUTES,
                        stale_after_minutes=_FIVETRAN_STALE_MINUTES,
                        tracking_status="tracked",
                    )
                )
                extra_times[asset_key] = last_sync
            seen.add(schema_name)
        elif schema_name in fivetran_synced_schemas:
            extra_specs.append(
                AssetFreshnessSpec(
                    schema_name=schema_name,
                    asset_key=f"untracked/{schema_name}",
                    sync_type="untracked",
                    schedule_name="untracked",
                    expected_cadence_minutes=0,
                    stale_after_minutes=0,
                    tracking_status="untracked",
                    untracked_reason="Legacy Fivetran snapshot schema (connector paused or removed)",
                )
            )
            seen.add(schema_name)
        else:
            reason = _KNOWN_UNTRACKED_REASONS.get(
                schema_name,
                "Not tracked by a scheduled Dagster mirror or Fivetran connector (freshness unknown)",
            )
            extra_specs.append(
                AssetFreshnessSpec(
                    schema_name=schema_name,
                    asset_key=f"untracked/{schema_name}",
                    sync_type="untracked",
                    schedule_name="untracked",
                    expected_cadence_minutes=0,
                    stale_after_minutes=0,
                    tracking_status="untracked",
                    untracked_reason=reason,
                )
            )
            seen.add(schema_name)

    return extra_specs, extra_times


_DELETED_ROWS_CAVEAT = (
    "Freshness is the time of the last successful sync only. It does not show "
    "that the data is complete: incremental mirrors may keep rows that were "
    "deleted at the source."
)

# Agents find these through SQL alone (obj_description / col_description or
# describe_schema), so they explain how to read the views. They are part of the
# DDL hash, so changing them rebuilds the views.
FRESHNESS_VIEW_DESCRIPTIONS: dict[str, str] = {
    ASSET_FRESHNESS_VIEW: (
        "Sync freshness, one row per Dagster asset (and one per untracked schema). "
        "Query this before trusting recent data. " + _DELETED_ROWS_CAVEAT
    ),
    FRESHNESS_VIEW: (
        "Sync freshness, one row per warehouse schema. The row shows the worst "
        "asset in the schema (worst_asset_key): one stale asset makes the schema "
        "stale. Query this before trusting recent data; use "
        f"{ASSET_FRESHNESS_VIEW} for each asset. " + _DELETED_ROWS_CAVEAT
    ),
}

_SHARED_COLUMN_DESCRIPTIONS: dict[str, str] = {
    "schema_name": "Warehouse schema (Postgres namespace) the data lands in.",
    "sync_type": "How the data arrives: sling, dlt, api, dbt, fivetran or untracked.",
    "schedule_name": (
        "Dagster schedule that runs the asset (materialize_all_assets, "
        "frequent_15min_schedule, ...), or untracked."
    ),
    "last_synced_at": (
        "Time of the last successful sync (Dagster materialization or Fivetran "
        "sync). NULL if never synced or untracked."
    ),
    "age_hours": "Hours since last_synced_at, computed at query time. NULL if never synced or untracked.",
    "expected_cadence_minutes": "How often the schedule runs, in minutes (taken from its cron).",
    "stale_after_minutes": "Age in minutes after which the row is stale. Larger than the cadence, to allow for slow runs.",
    "is_stale": "TRUE if older than stale_after_minutes or never synced. NULL for untracked.",
    "status": (
        "fresh = synced within stale_after_minutes; stale = older than that; "
        "never_synced = tracked but no successful sync recorded; untracked = no "
        "Dagster asset or live connector syncs it, so freshness is unknown "
        "(see untracked_reason)."
    ),
    "untracked_reason": "Why the schema is untracked (for example a paused Fivetran connector). NULL if tracked.",
    "recorded_at": "When the warehouse_mirror_freshness asset last wrote this row (it runs every 15 minutes).",
}

FRESHNESS_COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    ASSET_FRESHNESS_VIEW: {
        **_SHARED_COLUMN_DESCRIPTIONS,
        "asset_key": "Dagster asset key (path joined with /), or untracked/<schema> for an untracked schema.",
        "tracking_status": "tracked or untracked.",
    },
    FRESHNESS_VIEW: {
        **_SHARED_COLUMN_DESCRIPTIONS,
        "worst_asset_key": "The least fresh asset in the schema; its cadence and age fill this row.",
        "asset_count": "Number of tracked assets that write to the schema.",
        "stale_asset_count": "Number of those assets that are stale or never synced.",
        "last_synced_at": (
            "Oldest last successful sync across the schema's assets. NULL if any "
            "asset never synced, or the schema is untracked."
        ),
        "newest_synced_at": "Newest last successful sync across the schema's assets.",
        "age_hours": "Hours since the worst asset's last sync, computed at query time.",
        "is_stale": "TRUE if any tracked asset in the schema is stale or never synced. NULL for untracked.",
    },
}


def _view_ddl_comment(view: str) -> str:
    payload = "\n".join(FRESHNESS_VIEW_DDL_STATEMENTS) + "\n" + repr(
        (sorted(FRESHNESS_VIEW_DESCRIPTIONS.items()),
         sorted((v, sorted(c.items())) for v, c in FRESHNESS_COLUMN_DESCRIPTIONS.items()))
    )
    digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return (
        f"{FRESHNESS_VIEW_DESCRIPTIONS[view]} "
        f"Managed by Dagster asset warehouse_mirror_freshness (ddl {digest})."
    )


def _ensure_freshness_views(cur: Any) -> bool:
    """Rebuild the views only when their DDL changed. Returns True if rebuilt.

    The comment on each view records a hash of the DDL that made it. Dropping and
    recreating (rather than CREATE OR REPLACE) lets a later change remove or
    reorder columns.
    """
    views = (ASSET_FRESHNESS_VIEW, FRESHNESS_VIEW)
    cur.execute(
        "SELECT obj_description(to_regclass(%s), 'pg_class'), "
        "obj_description(to_regclass(%s), 'pg_class')",
        views,
    )
    existing = cur.fetchone()
    if all(found == _view_ddl_comment(view) for found, view in zip(existing, views)):
        return False
    cur.execute(f"DROP VIEW IF EXISTS {FRESHNESS_VIEW}")
    cur.execute(f"DROP VIEW IF EXISTS {ASSET_FRESHNESS_VIEW}")
    for ddl in FRESHNESS_VIEW_DDL_STATEMENTS:
        cur.execute(ddl)
    for view in views:
        for column, text in FRESHNESS_COLUMN_DESCRIPTIONS[view].items():
            cur.execute(f"COMMENT ON COLUMN {view}.{column} IS %s", (text,))
        # Written last: the hash comment marks a complete rebuild.
        cur.execute(f"COMMENT ON VIEW {view} IS %s", (_view_ddl_comment(view),))
    return True


def write_mirror_freshness(
    conn: Any,
    specs: Sequence[AssetFreshnessSpec],
    materializations: Mapping[str, datetime | None],
    recorded_at: datetime | None = None,
) -> int:
    """Create/update the freshness table and views atomically in one transaction."""
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
            spec.tracking_status,
            spec.untracked_reason,
            now,
        )
        for spec in specs
    ]

    with conn.cursor() as cur:
        for ddl in FRESHNESS_DDL_STATEMENTS:
            cur.execute(ddl)
        _ensure_freshness_views(cur)
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
                tracking_status,
                untracked_reason,
                recorded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    """Refresh warehouse_metadata.asset_sync_status, asset_freshness, and mirror_freshness."""
    warehouse_url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL is required to update mirror freshness")

    specs = build_asset_specs()
    materializations = fetch_latest_materializations(context.instance, specs)
    recorded_at = datetime.now(timezone.utc)

    conn = psycopg2.connect(warehouse_url, options=_WAREHOUSE_CONNECT_OPTIONS)
    try:
        tracked_schemas = {s.schema_name for s in specs}
        extra_specs, extra_times = discover_warehouse_extra_specs(conn, tracked_schemas)
        all_specs = list(specs) + extra_specs
        all_times = {**materializations, **extra_times}
        count = write_mirror_freshness(conn, all_specs, all_times, recorded_at=recorded_at)
    finally:
        conn.close()

    tracked_only = [s for s in all_specs if s.tracking_status == "tracked"]
    stale_count = sum(
        1
        for spec in tracked_only
        if all_times.get(spec.asset_key) is None
        or (recorded_at - all_times[spec.asset_key]).total_seconds()
        > spec.stale_after_minutes * 60
    )
    untracked_count = sum(1 for s in all_specs if s.tracking_status == "untracked")
    context.log.info(
        "Updated %s and %s with %d rows (%d tracked assets, %d stale, %d untracked schemas)",
        ASSET_FRESHNESS_VIEW,
        FRESHNESS_VIEW,
        count,
        len(tracked_only),
        stale_count,
        untracked_count,
    )
    context.add_output_metadata(
        {
            "row_count": count,
            "tracked_asset_count": len(tracked_only),
            "stale_asset_count": stale_count,
            "untracked_schema_count": untracked_count,
            "view": FRESHNESS_VIEW,
            "asset_view": ASSET_FRESHNESS_VIEW,
        }
    )
    return None


defs = dg.Definitions(assets=[warehouse_mirror_freshness])
