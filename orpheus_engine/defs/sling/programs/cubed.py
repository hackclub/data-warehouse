import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Cubed (Orchard: cubed-db, namespace ysws-cubed).
cubed_db_connection = SlingConnectionResource(
    name="CUBED_DB",
    type="postgres",
    connection_string=_sling_connection_url("CUBED_K8S_URL"),
)


# Not mirrored: sessions (its id is the login cookie),
# drizzle.__drizzle_migrations.
cubed_replication_config = {
    "source": "CUBED_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "cubed.{stream_table}",
    },

    "streams": {
        "public.admin_actions": None,
        "public.airtable_cleanup": None,
        "public.announcement_reads": None,
        "public.announcements": None,
        "public.hackatime_activity": None,
        "public.orders": None,
        "public.project_upvotes": None,
        "public.projects": None,
        "public.shop_items": None,
        "public.submissions": None,
        "public.transactions": None,
        "public.user_activity": None,
        "public.users": None,
    },
}


@dg.asset(
    name="cubed_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def cubed_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Cubed DB → warehouse in a single shot."""
    context.log.info("Starting Cubed → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=cubed_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
