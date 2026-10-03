import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _sling_connection_url


terra_db_connection = SlingConnectionResource(
    name="TERRA_DB",
    type="postgres",
    connection_string=_sling_connection_url("TERRA_DATABASE_URL"),
)


terra_replication_config = {
    "source": "TERRA_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "terra.{stream_table}",
    },

    "streams": {
        "public.users": {
            "select": ["id", "deleted_at"],
        },
        "public.hackatime_accounts": {
            "select": ["id", "user_id", "hackatime_user_id"],
        },
        "public.ysws_projects": {
            "select": [
                "id", "user_id", "title", "code_url", "hackatime_project_name",
                "week_number", "created_at", "deleted_at",
            ],
        },
    },
}



@dg.asset(
    name="terra_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def terra_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Terra DB tables used for DAU → warehouse in a single shot."""
    context.log.info("Starting Terra → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=terra_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
