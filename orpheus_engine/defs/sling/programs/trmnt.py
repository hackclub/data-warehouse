import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# TRMNT (Orchard: trmnt, namespace ysws-trmnt).
trmnt_db_connection = SlingConnectionResource(
    name="TRMNT_DB",
    type="postgres",
    connection_string=_sling_connection_url("TRMNT_K8S_URL"),
)


# Not mirrored: sessions, sign_in_codes, screenshots (raw image bytes) and
# drizzle.__drizzle_migrations. hackatime_project_links is a view.
trmnt_replication_config = {
    "source": "TRMNT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "trmnt.{stream_table}",
    },

    "streams": {
        "public.daily_activity": None,
        "public.hackatime_project_links": None,
        "public.ledger": None,
        "public.orders": {
            "select": ["-tracking_number"],
        },
        "public.projects": None,
        "public.submissions": None,
        "public.users": {
            "select": [
                "-hack_club_access_token", "-hack_club_refresh_token",
                "-hackatime_access_token",
            ],
        },
    },
}


@dg.asset(
    name="trmnt_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def trmnt_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the TRMNT DB → warehouse in a single shot."""
    context.log.info("Starting TRMNT → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=trmnt_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
