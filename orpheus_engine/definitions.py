import dagster as dg

# Import definition modules that export defs
import orpheus_engine.defs.airtable.definitions as airtable_defs
import orpheus_engine.defs.dbt.definitions as dbt_defs
import orpheus_engine.defs.dlt.definitions as dlt_defs
import orpheus_engine.defs.loops.definitions as loops_defs
import orpheus_engine.defs.loops_campaign_and_metrics_export.definitions as loops_campaign_defs
import orpheus_engine.defs.slack.definitions as slack_defs
import orpheus_engine.defs.sling.definitions as sling_defs
import orpheus_engine.defs.unified_ysws_db.definitions as ysws_defs
import orpheus_engine.defs.agh_fulfillment_zenventory.definitions as zenventory_defs
import orpheus_engine.defs.slack_users_sync.definitions as slack_users_sync_defs
import orpheus_engine.defs.airtable_raw_all_bases.definitions as airtable_raw_all_bases_defs
import orpheus_engine.defs.zenventory_inventory_airtable_sync.definitions as zenventory_airtable_sync_defs
import orpheus_engine.defs.ysws_programs_sync.definitions as ysws_programs_sync_defs
import orpheus_engine.defs.airtable_audit_logs.definitions as airtable_audit_logs_defs
import orpheus_engine.defs.airtable_users.definitions as airtable_users_defs
import orpheus_engine.defs.zoom.definitions as zoom_defs
import orpheus_engine.defs.highway_github.definitions as highway_github_defs
import orpheus_engine.defs.fillout.definitions as fillout_defs
import orpheus_engine.schedules as schedules

from orpheus_engine.defs.shared.airtable_enterprise import AirtableEnterpriseResource
from orpheus_engine.defs.shared.zoom import ZoomResource

# Import analytics asset separately (it doesn't export defs)
from orpheus_engine.defs.analytics.definitions import analytics_hack_clubbers

# Import the DuckLake row hash asset factory and sync asset
from orpheus_engine.defs.ducklake.definitions import create_warehouse_row_hashes_asset, ducklake_sync

# Import shared exclusion list (single source of truth)
from orpheus_engine.schedules import EXCLUDED_FROM_MAIN_JOB


def _build_definitions() -> dg.Definitions:
    """
    Build the final Definitions object with dynamic DuckLake dependencies.
    Using a function to avoid having multiple Definitions at module scope.
    """
    # First, merge all definitions EXCEPT ducklake
    base_defs = dg.Definitions.merge(
        airtable_defs.defs,
        dbt_defs.defs,
        dlt_defs.defs,
        loops_defs.defs,
        loops_campaign_defs.defs,
        slack_defs.defs,
        slack_users_sync_defs.defs,
        sling_defs.defs,
        ysws_defs.defs,
        zenventory_defs.defs,
        airtable_raw_all_bases_defs.defs,
        zenventory_airtable_sync_defs.defs,
        ysws_programs_sync_defs.defs,
        airtable_audit_logs_defs.defs,
        airtable_users_defs.defs,
        zoom_defs.defs,
        highway_github_defs.defs,
        fillout_defs.defs,
        schedules.defs,
        dg.Definitions(assets=[analytics_hack_clubbers]),
        dg.Definitions(resources={
            "airtable_enterprise": AirtableEnterpriseResource(
                api_key=dg.EnvVar("AIRTABLE_ENTERPRISE_PAT"),
                enterprise_account_id=dg.EnvVar("AIRTABLE_ENTERPRISE_ACCOUNT_ID"),
            ),
            "zoom": ZoomResource(
                account_id=dg.EnvVar("ZOOM_ACCOUNT_ID"),
                client_id=dg.EnvVar("ZOOM_CLIENT_ID"),
                client_secret=dg.EnvVar("ZOOM_CLIENT_SECRET"),
            ),
        })
    )

    # Dynamically discover all asset keys from the merged definitions
    # Handle both single assets (.key) and multi-assets (.keys)
    # Exclude the same assets that are excluded from materialize_all_assets_job
    all_asset_keys = []
    for asset in base_defs.assets:
        # Multi-assets have multiple keys, single assets have one
        if hasattr(asset, 'keys'):
            for key in asset.keys:
                if key not in EXCLUDED_FROM_MAIN_JOB:
                    all_asset_keys.append(key)
        else:
            if asset.key not in EXCLUDED_FROM_MAIN_JOB:
                all_asset_keys.append(asset.key)

    # Create the DuckLake row hash asset with dependencies matching materialize_all_assets_job
    warehouse_row_hashes = create_warehouse_row_hashes_asset(dep_asset_keys=all_asset_keys)

    # Dedicated DuckLake job + schedule.
    #
    # The DuckLake assets are deliberately decoupled from materialize_all_assets_job
    # (they're in EXCLUDED_FROM_MAIN_JOB). warehouse_row_hashes depends on ALL other
    # assets, so when it lived in the all-assets job a single upstream failure (a dead
    # mirror source, a dbt data-test, a flaky Zoom call) skipped the entire lakehouse
    # sync — which is exactly why it silently stopped running on 2026-01-29. Running it
    # as its own job decouples the lakehouse from unrelated asset health: it hashes and
    # syncs whatever is currently committed in the warehouse, on its own cadence.
    #
    # Schedule ships STOPPED by default (Dagster's default). The first prod run rehashes
    # ~5 months of drift across the large hackatime/airtable tables, so trigger it once
    # manually and watch WAL/archiving, then enable the schedule for steady-state daily
    # refresh.
    materialize_ducklake_job = dg.define_asset_job(
        name="materialize_ducklake_job",
        selection=dg.AssetSelection.assets(
            dg.AssetKey("warehouse_row_hashes"),
            dg.AssetKey("ducklake_sync"),
        ),
    )
    ducklake_daily_schedule = dg.ScheduleDefinition(
        name="ducklake_daily_schedule",
        job=materialize_ducklake_job,
        cron_schedule="0 9 * * *",              # daily 09:00, after overnight loads
        execution_timezone="America/New_York",
    )

    # Final merged definitions including the DuckLake assets
    # - warehouse_row_hashes: computes row hashes (depends on all other assets)
    # - ducklake_sync: syncs to DuckLake (depends on warehouse_row_hashes)
    return dg.Definitions.merge(
        base_defs,
        dg.Definitions(
            assets=[warehouse_row_hashes, ducklake_sync],
            jobs=[materialize_ducklake_job],
            schedules=[ducklake_daily_schedule],
        )
    )


# Only one Definitions object at module scope
defs = _build_definitions()