"""Tests for per-asset and per-schema warehouse freshness views and cadence derivation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import dagster as dg
import psycopg2

from orpheus_engine.defs.freshness.definitions import (
    ASSET_FRESHNESS_VIEW,
    FRESHNESS_VIEW,
    AssetFreshnessSpec,
    build_asset_specs,
    discover_warehouse_extra_specs,
    fetch_latest_materializations,
    resolve_asset_schedule_map,
    write_mirror_freshness,
)
from orpheus_engine_tests.test_warehouse_agent_login import server  # noqa: F401


def test_all_non_dbt_asset_specs_exist_in_dagster_definitions():
    import orpheus_engine.definitions as defs_mod

    all_keys = {
        "/".join(key.path)
        for asset in defs_mod.defs.assets
        for key in (asset.keys if hasattr(asset, "keys") else [asset.key])
    }
    specs = build_asset_specs(defs_mod.defs)
    non_dbt = [s for s in specs if s.sync_type != "dbt"]
    assert len(non_dbt) >= 150

    missing = [spec for spec in non_dbt if spec.asset_key not in all_keys]
    assert not missing, f"Freshness specs reference unknown Dagster assets: {missing}"


def test_every_active_sling_mirror_and_multi_asset_schemas_are_tracked():
    import orpheus_engine.definitions as defs_mod
    import orpheus_engine.defs.sling.definitions as sling_defs_mod

    specs = build_asset_specs(defs_mod.defs)
    tracked_keys = {spec.asset_key for spec in specs}
    by_schema: dict[str, set[str]] = {}
    for spec in specs:
        by_schema.setdefault(spec.schema_name, set()).add(spec.asset_key)

    sling_mirror_keys = {
        asset.key.path[0]
        for asset in sling_defs_mod.defs.assets
        if asset.key.path[0].endswith("_warehouse_mirror")
    }
    assert not (sling_mirror_keys - tracked_keys)

    # Multi-asset schemas must include both fast and slow assets.
    assert "campfire_event_warehouse" in by_schema["airtable_campfire"]
    assert "campfire_participants_warehouse" in by_schema["airtable_campfire"]
    assert "loops_audience" in by_schema["loops"]
    assert "loops_campaign_email_contents" in by_schema["loops"]
    assert "loops_campaign_contents_to_warehouse" in by_schema["loops"]

    # dbt mart schemas are covered too.
    assert "public_unified_analytics" in by_schema
    assert "unified_analytics/events" in by_schema["public_unified_analytics"]
    # joe_warehouse_mirror writes to schema fraud_joe
    assert "joe_warehouse_mirror" in by_schema["fraud_joe"]


def test_derived_cadence_matches_actual_dagster_job_selection():
    """Every tracked Dagster asset's derived schedule must match the job that actually
    materializes it (would have caught hackatime_warehouse_mirror 2h vs 6h)."""
    import orpheus_engine.definitions as defs_mod
    from orpheus_engine import schedules

    asset_graph = defs_mod.defs.resolve_asset_graph()
    selection_by_schedule = {
        "frequent_15min_schedule": schedules.FREQUENT_SELECTION.resolve(
            asset_graph, allow_missing=True
        ),
        "unified_ysws_15min_schedule": schedules.UNIFIED_YSWS_SELECTION.resolve(
            asset_graph, allow_missing=True
        ),
        "hackatime_dau_hourly_schedule": schedules.HACKATIME_DAU_SELECTION.resolve(
            asset_graph, allow_missing=True
        ),
        "project_search_2h_schedule": schedules.PROJECT_SEARCH_SELECTION.resolve(
            asset_graph, allow_missing=True
        ),
        "materialize_all_assets": schedules.materialize_all_assets_job.selection.resolve(
            asset_graph, allow_missing=True
        ),
        "zoom_daily_schedule": schedules.ZOOM_SELECTION.resolve(
            asset_graph, allow_missing=True
        ),
    }

    specs = build_asset_specs(defs_mod.defs)
    for spec in specs:
        if spec.sync_type == "dbt":
            continue
        assert spec.schedule_name in selection_by_schedule
        assert spec.dagster_asset_key() in selection_by_schedule[spec.schedule_name], (
            f"{spec.asset_key} claims schedule {spec.schedule_name} but is not in that job's selection"
        )

    schedule_map = resolve_asset_schedule_map(defs_mod.defs)
    # Hackatime mirror resolves into materialize_all_assets (6h/12h), NOT 2h DAU job.
    assert schedule_map["hackatime_warehouse_mirror"] == ("materialize_all_assets", 360, 720)
    # Stardance mirror resolves into frequent 15m job.
    assert schedule_map["stardance_warehouse_mirror"] == ("frequent_15min_schedule", 15, 60)
    # Campfire has mixed-cadence assets: event is 15m, participants is 6h.
    assert schedule_map["campfire_event_warehouse"] == ("frequent_15min_schedule", 15, 60)
    assert schedule_map["campfire_participants_warehouse"] == ("materialize_all_assets", 360, 720)
    # Zoom assets run on the dedicated daily 00:10 UTC schedule (24h cadence, 36h stale window).
    assert schedule_map["zoom_meetings"] == ("zoom_daily_schedule", 1440, 2160)
    assert schedule_map["zoom_daily_usage"] == ("zoom_daily_schedule", 1440, 2160)


def test_freshness_asset_is_in_frequent_schedule_and_not_all_assets():
    import orpheus_engine.definitions as defs_mod
    from orpheus_engine.schedules import FREQUENT_SELECTION, materialize_all_assets_job

    asset_graph = defs_mod.defs.resolve_asset_graph()
    frequent_keys = FREQUENT_SELECTION.resolve(asset_graph, allow_missing=True)
    all_assets_keys = materialize_all_assets_job.selection.resolve(
        asset_graph, allow_missing=True
    )

    key = dg.AssetKey("warehouse_mirror_freshness")
    assert key in frequent_keys
    assert key not in all_assets_keys


def test_fetch_latest_materializations_returns_none_for_unsynced_assets():
    spec = AssetFreshnessSpec(
        schema_name="example",
        asset_key="example_warehouse_mirror",
        sync_type="sling",
        schedule_name="materialize_all_assets",
        expected_cadence_minutes=360,
        stale_after_minutes=720,
    )
    with dg.instance_for_test() as instance:
        result = fetch_latest_materializations(instance, [spec])
    assert result == {"example_warehouse_mirror": None}


def test_worst_of_rollup_untracked_discovery_and_reader_access(server):
    now = datetime.now(timezone.utc)
    specs = [
        # Mixed-cadence schema: fast asset fresh, slow asset stale -> schema MUST be stale
        AssetFreshnessSpec("airtable_campfire", "campfire_event_warehouse", "dlt", "frequent_15min_schedule", 15, 60),
        AssetFreshnessSpec("airtable_campfire", "campfire_participants_warehouse", "dlt", "materialize_all_assets", 360, 720),
        # Loops: audience fresh, campaign_email_contents stale -> schema MUST be stale
        AssetFreshnessSpec("loops", "loops_audience", "dlt", "materialize_all_assets", 360, 720),
        AssetFreshnessSpec("loops", "loops_campaign_email_contents", "api", "materialize_all_assets", 360, 720),
        # Healthy schema
        AssetFreshnessSpec("stardance", "stardance_warehouse_mirror", "sling", "frequent_15min_schedule", 15, 60),
        # Never synced schema
        AssetFreshnessSpec("canvas", "canvas_warehouse_mirror", "sling", "materialize_all_assets", 360, 720),
    ]
    materializations = {
        "campfire_event_warehouse": now - timedelta(minutes=5),
        "campfire_participants_warehouse": now - timedelta(hours=15),
        "loops_audience": now - timedelta(hours=2),
        "loops_campaign_email_contents": now - timedelta(days=3),
        "stardance_warehouse_mirror": now - timedelta(minutes=10),
        "canvas_warehouse_mirror": None,
    }

    with server.admin() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS legacy_unmanaged")
            cur.execute("CREATE TABLE IF NOT EXISTS legacy_unmanaged.items (id int)")
        extra_specs, extra_times = discover_warehouse_extra_specs(
            conn, {s.schema_name for s in specs}
        )
        all_specs = specs + extra_specs
        all_times = {**materializations, **extra_times}
        count = write_mirror_freshness(conn, all_specs, all_times, recorded_at=now)
    assert count >= 7

    for role_url in (server.agent_url(), server.mcp_url()):
        conn = psycopg2.connect(role_url)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT schema_name, status, is_stale, asset_count, stale_asset_count, worst_asset_key, untracked_reason "
                    f"FROM {FRESHNESS_VIEW}"
                )
                rollup = {row[0]: row[1:] for row in cur.fetchall()}

                cur.execute(f"SELECT count(*) FROM {ASSET_FRESHNESS_VIEW} WHERE schema_name = 'airtable_campfire'")
                campfire_asset_rows = cur.fetchone()[0]
        finally:
            conn.close()

        assert campfire_asset_rows == 2
        assert rollup["airtable_campfire"][:5] == ("stale", True, 2, 1, "campfire_participants_warehouse")
        assert rollup["loops"][:5] == ("stale", True, 2, 1, "loops_campaign_email_contents")
        assert rollup["stardance"][:4] == ("fresh", False, 1, 0)
        assert rollup["canvas"][:4] == ("never_synced", True, 1, 1)
        assert rollup["legacy_unmanaged"][0] == "untracked"
        assert rollup["legacy_unmanaged"][1] is None
        assert rollup["legacy_unmanaged"][5] is not None


def test_flavortown_post_ship_events_uses_full_refresh():
    """Keep the small post_ship_events table on full-refresh so hard deletes propagate."""
    from orpheus_engine.defs.sling.programs.flavortown import flavortown_replication_config

    assert flavortown_replication_config["defaults"]["mode"] == "full-refresh"
    stream_cfg = flavortown_replication_config["streams"].get("public.post_ship_events")
    assert stream_cfg is None or stream_cfg.get("mode") == "full-refresh"


def test_scheduled_cadences_match_cron():
    """The hand-written cadence for each schedule must equal its cron interval."""
    from dagster._utils.schedules import get_smallest_cron_interval

    from orpheus_engine import schedules
    from orpheus_engine.defs.freshness.definitions import _SCHEDULED_JOB_CADENCES

    for sched_attr, cadence_minutes, stale_after_minutes in _SCHEDULED_JOB_CADENCES:
        schedule_def = getattr(schedules, sched_attr)
        interval = get_smallest_cron_interval(
            schedule_def.cron_schedule, schedule_def.execution_timezone
        )
        assert interval == timedelta(minutes=cadence_minutes), sched_attr
        assert stale_after_minutes > cadence_minutes, sched_attr


def test_views_are_rebuilt_only_when_their_ddl_changes(server):
    """A routine run must not take an ACCESS EXCLUSIVE lock on the views."""
    from orpheus_engine.defs.freshness.definitions import _ensure_freshness_views

    spec = AssetFreshnessSpec("stardance", "stardance_warehouse_mirror", "sling", "frequent_15min_schedule", 15, 60)
    now = datetime.now(timezone.utc)
    with server.admin() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS warehouse_metadata CASCADE")
        write_mirror_freshness(conn, [spec], {spec.asset_key: now}, recorded_at=now)
        with conn.cursor() as cur:
            assert _ensure_freshness_views(cur) is False
            # An older definition (different comment) is replaced.
            cur.execute(f"COMMENT ON VIEW {FRESHNESS_VIEW} IS 'old'")
            assert _ensure_freshness_views(cur) is True
            assert _ensure_freshness_views(cur) is False
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'warehouse_metadata' AND table_name = 'mirror_freshness'"
            )
            columns = {row[0] for row in cur.fetchall()}
            cur.execute(f"SELECT status FROM {FRESHNESS_VIEW} WHERE schema_name = 'stardance'")
            assert cur.fetchone()[0] == "fresh"
    assert "worst_asset_key" in columns
    assert "asset_key" not in columns


def test_views_explain_themselves_to_sql_readers(server):
    """Agents read freshness through SQL only, so the views carry their own docs."""
    from orpheus_engine.defs.freshness import definitions as fresh

    spec = AssetFreshnessSpec("stardance", "stardance_warehouse_mirror", "sling", "frequent_15min_schedule", 15, 60)
    now = datetime.now(timezone.utc)
    with server.admin() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS warehouse_metadata CASCADE")
        write_mirror_freshness(conn, [spec], {spec.asset_key: now}, recorded_at=now)

    for role_url in (server.agent_url(), server.mcp_url()):
        conn = psycopg2.connect(role_url)
        try:
            with conn.cursor() as cur:
                for view in (ASSET_FRESHNESS_VIEW, FRESHNESS_VIEW):
                    cur.execute("SELECT obj_description(to_regclass(%s), 'pg_class')", (view,))
                    view_comment = cur.fetchone()[0]
                    assert "before trusting recent data" in view_comment
                    assert "deleted at the source" in view_comment
                    assert "(ddl " in view_comment

                    schema, name = view.split(".")
                    cur.execute(
                        "SELECT c.column_name, col_description(to_regclass(%s), c.ordinal_position) "
                        "FROM information_schema.columns c "
                        "WHERE c.table_schema = %s AND c.table_name = %s",
                        (view, schema, name),
                    )
                    column_comments = dict(cur.fetchall())
                    assert column_comments, view
                    missing = [col for col, text in column_comments.items() if not text]
                    assert missing == [], (view, missing)
                    for value in ("fresh", "stale", "never_synced", "untracked"):
                        assert value in column_comments["status"]
                    assert "cron" in column_comments["expected_cadence_minutes"]
        finally:
            conn.close()

    # Every documented column exists, so a renamed column cannot leave a stale doc.
    with server.admin() as conn, conn.cursor() as cur:
        for view, docs in fresh.FRESHNESS_COLUMN_DESCRIPTIONS.items():
            schema, name = view.split(".")
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, name),
            )
            assert set(docs) == {row[0] for row in cur.fetchall()}, view


def test_changing_a_description_changes_the_ddl_hash(monkeypatch):
    from orpheus_engine.defs.freshness import definitions as fresh

    before = fresh._view_ddl_comment(FRESHNESS_VIEW)
    changed = {view: dict(cols) for view, cols in fresh.FRESHNESS_COLUMN_DESCRIPTIONS.items()}
    changed[FRESHNESS_VIEW]["status"] = "reworded"
    monkeypatch.setattr(fresh, "FRESHNESS_COLUMN_DESCRIPTIONS", changed)
    assert fresh._view_ddl_comment(FRESHNESS_VIEW) != before
