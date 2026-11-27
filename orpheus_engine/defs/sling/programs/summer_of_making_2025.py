import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


summer_of_making_2025_db_connection = SlingConnectionResource(
    name="SUMMER_OF_MAKING_2025_DB",
    type="postgres",
    connection_string=EnvVar("SUMMER_OF_MAKING_2025_COOLIFY_URL"),
)


summer_of_making_2025_replication_config = {
    "source": "SUMMER_OF_MAKING_2025_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "summer_of_making_2025.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # Disabled: Rails infrastructure tables
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_cache_entries": {"disabled": True},
        "public.magic_links": {"select": ["-token"]},  # sign-in link token
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
        # Disabled: ActiveInsights APM telemetry (request timings / job-queue
        # bookkeeping) from an event that ended in 2025, with no downstream
        # consumers. At 87M/13M rows they dominated sync cost — the 2026-06-11
        # warehouse OOM happened mid-COPY of active_insights_jobs. Existing
        # warehouse data is kept; re-enable if anyone actually needs them.
        "public.active_insights_requests": {"disabled": True},
        "public.active_insights_jobs": {"disabled": True},
        # Large tables configured for incremental sync
        "public.vote_changes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 290K rows
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",  # 274K rows
        },
        "public.hackatime_projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 192K rows
        },
        "public.view_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 161K rows
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 149K rows
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",  # 46K rows
        },
        "public.devlogs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 48K rows
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 39K rows
            "select": ["-identity_vault_access_token", "-wrapped_share_token"],
        },
        "public.activities": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 30K rows
        },
    }
}



@dg.asset(
    name="summer_of_making_2025_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def summer_of_making_2025_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Summer of Making 2025 DB → warehouse in a single shot."""
    context.log.info("Starting Summer of Making 2025 → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, summer_of_making_2025_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=summer_of_making_2025_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
