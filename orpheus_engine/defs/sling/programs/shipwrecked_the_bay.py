import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


shipwrecked_the_bay_db_connection = SlingConnectionResource(
    name="SHIPWRECKED_THE_BAY_DB",
    type="postgres",
    connection_string=EnvVar("SHIPWRECKED_THE_BAY_COOLIFY_URL"),
)


shipwrecked_the_bay_replication_config = {
    "source": "SHIPWRECKED_THE_BAY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "shipwrecked_the_bay.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        # OAuth, session and sign-in tokens (secret_columns/inventory.py)
        "public.Account": {"select": ["-access_token", "-id_token", "-refresh_token"]},
        "public.Session": {"select": ["-sessionToken"]},
        "public.User": {"select": ["-identityToken"]},
        "public.VerificationToken": {"select": ["-token"]},
    }
}



@dg.asset(
    name="shipwrecked_the_bay_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def shipwrecked_the_bay_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Shipwrecked The Bay DB → warehouse in a single shot."""
    context.log.info("Starting Shipwrecked The Bay → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=shipwrecked_the_bay_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
