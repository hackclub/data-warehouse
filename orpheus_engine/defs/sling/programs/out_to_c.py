import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Out to C (Orchard: out-to-c-production, namespace ysws-out-to-c).
out_to_c_db_connection = SlingConnectionResource(
    name="OUT_TO_C_DB",
    type="postgres",
    connection_string=_sling_connection_url("OUT_TO_C_K8S_URL"),
)


# Not mirrored: schema_migrations and ar_internal_metadata.
out_to_c_replication_config = {
    "source": "OUT_TO_C_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "out_to_c.{stream_table}",
    },

    "streams": {
        "public.users": {
            "select": ["-token", "-hca_token"],
        },
        "public.voyages": None,
    },
}


@dg.asset(
    name="out_to_c_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def out_to_c_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Out to C DB → warehouse in a single shot."""
    context.log.info("Starting Out to C → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=out_to_c_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
