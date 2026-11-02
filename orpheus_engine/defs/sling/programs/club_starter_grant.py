import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# sessions.id is the session cookie. Exclude sessions and Knex bookkeeping.
# submitted_projects holds project/title/record_id references, not Airtable payloads.
# No key/value settings tables.
# Schema and kept-column reads checked 2026-10-31.
club_starter_grant_db_connection = SlingConnectionResource(
    name="CLUB_STARTER_GRANT_DB",
    type="postgres",
    connection_string=_sling_connection_url("CLUB_STARTER_GRANT_K8S_URL"),
)


club_starter_grant_replication_config = {
    "source": "CLUB_STARTER_GRANT_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "club_starter_grant.{stream_table}"
    },
    "streams": {
        "public.admin_invites": None,
        "public.users": {
            "select": [
                "-access_token",
                "-refresh_token",
                "-address_line_1",
                "-address_line_2",
                "-postal_code"
            ]
        }
    }
}


@dg.asset(
    name="club_starter_grant_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def club_starter_grant_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the club_starter_grant DB → warehouse in a single shot."""
    context.log.info("Starting club_starter_grant → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=club_starter_grant_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
