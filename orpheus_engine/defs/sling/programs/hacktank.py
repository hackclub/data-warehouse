import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


hacktank_db_connection = SlingConnectionResource(
    name="HACKTANK_DB",
    type="postgres",
    connection_string=EnvVar("HACKTANK_DATABASE_URL"),
)


hacktank_replication_config = {
    "source": "HACKTANK_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hacktank.{stream_table}",
    },

    "streams": {
        "drizzle.__drizzle_migrations": None,
        "public.devlog_screenshots": None,
        "public.devlogs": None,
        "public.hackatime_snapshots": None,
        "public.projects": None,
        "public.screenshots": None,
        "public.seed_ledger": None,
        "public.shop_items": None,
        "public.shop_orders": None,
        "public.users": {
            "select": [
                "-hackclub_access_token", "-hackatime_access_token",
            ],
        },
        "public.video_metric_snapshots": None,
        "public.videos": None,
    },
}


@dg.asset(
    name="hacktank_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hacktank_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hacktank DB → warehouse in a single shot."""
    context.log.info("Starting Hacktank → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hacktank_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
