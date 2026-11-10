import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# scare (Orchard source; schema checked 2026-10-31).
scare_db_connection = SlingConnectionResource(
    name="SCARE_DB",
    type="postgres",
    connection_string=_sling_connection_url("SCARE_K8S_URL"),
)


# Database sessions and verification tokens stay out. OAuth tokens are encrypted secrets.
# Order details accept free text/URLs; audit/review JSON and integration errors are unstructured.
# Screenshot bytes are omitted from reporting. No key/value settings tables exist
# (catalog and app auth/order code checked 2026-10-31).
scare_replication_config = {
    "source": "SCARE_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "scare.{stream_table}"
    },
    "streams": {
        "public.Account": {
            "select": [
                "-refresh_token",
                "-access_token",
                "-id_token",
                "-session_state"
            ]
        },
        "public.AdminAudit": {
            "select": [
                "-detail"
            ]
        },
        "public.AriDelivery": None,
        "public.Game": {
            "select": [
                "-airtableError",
                "-ariError",
                "-reviewDetail"
            ]
        },
        "public.HackatimeLink": {
            "select": [
                "-accessToken"
            ]
        },
        "public.Order": {
            "select": [
                "-details"
            ]
        },
        "public.Screenshot": {
            "select": [
                "-data"
            ]
        },
        "public.Suggestion": None,
        "public.User": None
    }
}


@dg.asset(
    name="scare_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def scare_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the scare DB → warehouse in a single shot."""
    context.log.info("Starting scare → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=scare_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
