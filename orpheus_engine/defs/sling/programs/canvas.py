import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


canvas_db_connection = SlingConnectionResource(
    name="CANVAS_DB",
    type="postgres",
    connection_string=EnvVar("CANVAS_DATABASE_URL"),
)


canvas_replication_config = {
    "source": "CANVAS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "canvas.{stream_table}",
    },

    "streams": {
        "public.daily_active_users": None,
        "public.projects": None,
        "public.ships": None,
        "public.user_activity": None,
        "public.user_coding_days": None,
    },
}


@dg.asset(
    name="canvas_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def canvas_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Canvas DB → warehouse in a single shot."""
    context.log.info("Starting Canvas → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=canvas_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
