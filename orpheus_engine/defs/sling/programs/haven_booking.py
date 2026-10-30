import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Haven booking (Orchard: booking-db, namespace ysws-haven).
haven_booking_db_connection = SlingConnectionResource(
    name="HAVEN_BOOKING_DB",
    type="postgres",
    connection_string=_sling_connection_url("HAVEN_BOOKING_K8S_URL"),
)


# Not mirrored: _migrations, rate_limits.
haven_booking_replication_config = {
    "source": "HAVEN_BOOKING_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "haven_booking.{stream_table}",
    },

    "streams": {
        "public.bookings": {
            "select": ["-meet_url"],  # Meeting join link
        },
        "public.hosts": {
            "select": ["-refresh_token_enc"],  # Encrypted Google refresh token
        },
    },
}


@dg.asset(
    name="haven_booking_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def haven_booking_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Haven booking DB → warehouse in a single shot."""
    context.log.info("Starting Haven booking → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=haven_booking_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
