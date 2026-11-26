# Fallout program ended 2026-07-07 and its Orchard public endpoint is off.
# Disabled so materialize_all_assets_job does not fail on every run.
__disabled__ = True

import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


fallout_db_connection = SlingConnectionResource(
    name="FALLOUT_DB",
    type="postgres",
    connection_string=EnvVar("FALLOUT_COOLIFY_URL"),
)


fallout_replication_config = {
    "source": "FALLOUT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "fallout.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updated_at ---
        "public.airtable_syncs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.journal_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.onboarding_responses": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.recordings": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "avatar", "created_at", "discarded_at", "display_name",
                "email", "hca_id", "is_adult", "is_banned", "onboarded",
                "roles", "slack_id", "timezone", "type", "updated_at",
                "verification_status",
            ],  # Excludes device_token, hca_token, lapse_token
        },

        # --- Incremental: id + special timestamp ---
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },
        "public.active_storage_attachments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.active_storage_blobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.versions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.lapse_timelapses": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.you_tube_videos": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # --- Incremental: remaining data tables ---
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.mail_interactions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.mail_messages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ships": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # --- Disabled: Rails infrastructure tables ---
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_pauses": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
        "public.solid_queue_scheduled_executions": {"disabled": True},
        "public.solid_queue_semaphores": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # active_storage_variant_records (no timestamp)
    }
}



@dg.asset(
    name="fallout_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def fallout_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Fallout DB → warehouse in a single shot."""
    context.log.info("Starting Fallout → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, fallout_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=fallout_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
