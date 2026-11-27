import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


hackatime_legacy_db_connection = SlingConnectionResource(
    name="HACKATIME_LEGACY_DB",
    type="postgres",
    connection_string=EnvVar("HACKATIME_LEGACY_COOLIFY_URL"),
)


hackatime_legacy_replication_config = {
    "source": "HACKATIME_LEGACY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hackatime_legacy.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
        # API keys, password hash and reset token (secret_columns/inventory.py)
        "public.users": {
            "select": ["-api_key", "-password", "-reset_token", "-wakatime_api_key"],
        },
        # Large tables configured for incremental sync
        "public.heartbeats": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",  # 52M rows - heartbeat timestamp
        },
    }
}



@dg.asset(
    name="hackatime_legacy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_legacy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime Legacy DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime Legacy → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, hackatime_legacy_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_legacy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
