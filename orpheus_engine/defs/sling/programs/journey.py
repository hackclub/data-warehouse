import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


journey_db_connection = SlingConnectionResource(
    name="JOURNEY_DB",
    type="postgres",
    connection_string=EnvVar("JOURNEY_COOLIFY_URL"),
)


journey_replication_config = {
    "source": "JOURNEY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "journey.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.users": {"select": ["-identity_vault_access_token"]},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
    }
}



@dg.asset(
    name="journey_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def journey_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Journey DB → warehouse in a single shot."""
    context.log.info("Starting Journey → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=journey_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
