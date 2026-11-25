"""Tests for per-mirror warehouse freshness metadata and live SQL view."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

from orpheus_engine.defs.freshness.definitions import (
    FRESHNESS_VIEW,
    MirrorFreshnessSpec,
    build_mirror_specs,
    fetch_latest_materializations,
    write_mirror_freshness,
)
from orpheus_engine_tests.test_warehouse_agent_login import server  # noqa: F401


def test_all_mirror_specs_exist_in_dagster_definitions():
    import orpheus_engine.definitions as defs_mod

    all_keys = {
        "/".join(key.path)
        for asset in defs_mod.defs.assets
        for key in (asset.keys if hasattr(asset, "keys") else [asset.key])
    }
    specs = build_mirror_specs()
    assert len(specs) >= 75

    missing = [spec for spec in specs if spec.asset_key not in all_keys]
    assert not missing, f"Freshness specs reference unknown Dagster assets: {missing}"


def test_every_active_sling_mirror_is_tracked():
    import orpheus_engine.defs.sling.definitions as sling_defs_mod

    sling_mirror_keys = {
        asset.key.path[0]
        for asset in sling_defs_mod.defs.assets
        if asset.key.path[0].endswith("_warehouse_mirror")
    }
    tracked_keys = {spec.asset_key for spec in build_mirror_specs()}
    missing = sling_mirror_keys - tracked_keys
    assert not missing, f"Sling mirrors missing from freshness registry: {sorted(missing)}"


def test_schema_mapping_and_cadence_overrides():
    by_schema = {spec.schema_name: spec for spec in build_mirror_specs()}

    # joe_warehouse_mirror writes to schema fraud_joe
    assert "fraud_joe" in by_schema
    assert by_schema["fraud_joe"].asset_key == "joe_warehouse_mirror"

    # Fast-cadence mirrors keep their tight thresholds
    assert by_schema["stardance"].expected_cadence_minutes == 15
    assert by_schema["stardance"].stale_after_minutes == 60
    assert by_schema["hackatime"].expected_cadence_minutes == 120
    assert by_schema["hackatime"].stale_after_minutes == 360
    assert by_schema["forge"].expected_cadence_minutes == 360
    assert by_schema["forge"].stale_after_minutes == 720


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
    spec = MirrorFreshnessSpec(
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


def test_mirror_freshness_view_computes_live_staleness_and_is_readable_by_agent_and_mcp(server):
    now = datetime.now(timezone.utc)
    specs = [
        MirrorFreshnessSpec(
            schema_name="stardance",
            asset_key="stardance_warehouse_mirror",
            sync_type="sling",
            schedule_name="frequent_15min_schedule",
            expected_cadence_minutes=15,
            stale_after_minutes=60,
        ),
        MirrorFreshnessSpec(
            schema_name="hcb",
            asset_key="hcb_warehouse_mirror",
            sync_type="sling",
            schedule_name="materialize_all_assets",
            expected_cadence_minutes=360,
            stale_after_minutes=720,
        ),
        MirrorFreshnessSpec(
            schema_name="canvas",
            asset_key="canvas_warehouse_mirror",
            sync_type="sling",
            schedule_name="materialize_all_assets",
            expected_cadence_minutes=360,
            stale_after_minutes=720,
        ),
    ]
    materializations = {
        "stardance_warehouse_mirror": now - timedelta(minutes=10),
        "hcb_warehouse_mirror": now - timedelta(hours=36),
        "canvas_warehouse_mirror": None,
    }

    with server.admin() as conn:
        count = write_mirror_freshness(conn, specs, materializations, recorded_at=now)
    assert count == 3

    for role_url in (server.agent_url(), server.mcp_url()):
        import psycopg2

        conn = psycopg2.connect(role_url)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT schema_name, status, is_stale, expected_cadence_minutes, stale_after_minutes "
                    f"FROM {FRESHNESS_VIEW}"
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        by_schema = {row[0]: row[1:] for row in rows}
        assert by_schema["stardance"] == ("fresh", False, 15, 60)
        assert by_schema["hcb"] == ("stale", True, 360, 720)
        assert by_schema["canvas"] == ("never_synced", True, 360, 720)
        # Stale/never_synced rows sort ahead of fresh rows
        assert rows[-1][0] == "stardance"
