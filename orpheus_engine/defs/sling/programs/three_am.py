import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# 3am (Orchard: 3am, namespace ysws-3am).
three_am_db_connection = SlingConnectionResource(
    name="THREE_AM_DB",
    type="postgres",
    connection_string=_sling_connection_url("THREE_AM_K8S_URL"),
)


# Not mirrored: drizzle.__drizzle_migrations. webhook_events.payload is kept:
# every key across all rows was checked (2026-10-30) and holds only ship and
# review fields plus names, emails and Slack IDs.
three_am_replication_config = {
    "source": "THREE_AM_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "three_am.{stream_table}",
    },

    "streams": {
        "public.beans_ledger": None,
        "public.items": None,
        "public.orders": {
            "select": [
                "-address_line1", "-address_line2", "-postcode", "-tracking",
            ],
        },
        "public.projects": None,
        "public.reviewer_invoices": None,
        "public.reviewer_ledger": None,
        "public.reviewer_pay_config": None,
        "public.reviewers": None,
        "public.slack_order_digest_events": {
            # Payload holds tracking numbers
            "select": ["-payload"],
        },
        "public.users": {
            "select": [
                "-hackatime_token", "-address_line1", "-address_line2",
                "-postcode",
            ],
        },
        "public.webhook_events": None,
        "public.ysws_config": None,
        "public.ysws_submissions": None,
    },
}


@dg.asset(
    name="three_am_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def three_am_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the 3am DB → warehouse in a single shot."""
    context.log.info("Starting 3am → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=three_am_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
