import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Haven (Orchard: prod-db, namespace ysws-haven).
haven_db_connection = SlingConnectionResource(
    name="HAVEN_DB",
    type="postgres",
    connection_string=_sling_connection_url("HAVEN_K8S_URL"),
)


# Not mirrored: drizzle.__drizzle_migrations. Haven's hcb-db holds only HCB
# OAuth tokens and is not mirrored at all.
haven_replication_config = {
    "source": "HAVEN_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "haven.{stream_table}",
    },

    "streams": {
        "public.events": {
            # Venue location; some venues are private homes
            "select": [
                "-venueAddress", "-latitude", "-longitude",
            ],
        },
        "public.signup_notifications": None,
        "public.signup_snapshots": None,
        "public.ticket_summaries": None,
        "public.tickets": None,
        "public.users": {
            # address JSON holds street, postcode and phone
            "select": [
                "-token", "-hcaToken", "-phoneNumber", "-address",
            ],
        },
    },
}


@dg.asset(
    name="haven_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def haven_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Haven DB → warehouse in a single shot."""
    context.log.info("Starting Haven → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=haven_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
