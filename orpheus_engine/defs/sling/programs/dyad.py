import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# dyad (Orchard).
dyad_db_connection = SlingConnectionResource(
    name="DYAD_DB",
    type="postgres",
    connection_string=_sling_connection_url("DYAD_K8S_URL"),
)


# dyad source catalog and exclusions checked live 2026-10-31.
# Omitted: _sqlx_migrations, wrong_friend_code.
# wrong_friend_code is an authentication rate-limit table; no settings tables.
dyad_replication_config = {
    "source": "DYAD_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "dyad.{stream_table}",
    },

    "streams": {
        "public.dm_log": None,
        "public.hackatime_account": None,
        "public.hacker": {"select": ["-hackatime_token", "-friend_code", "-invite_token"]},
        "public.join_request": None,
        "public.project": None,
        "public.user_project": None,
    },
}


@dg.asset(
    name="dyad_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def dyad_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the dyad DB → warehouse in a single shot."""
    context.log.info("Starting dyad → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=dyad_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
