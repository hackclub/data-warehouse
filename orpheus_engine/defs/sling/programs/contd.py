import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


contd_db_connection = SlingConnectionResource(
    name="CONTD_DB",
    type="postgres",
    connection_string=EnvVar("CONTD_DATABASE_URL"),
)


contd_replication_config = {
    "source": "CONTD_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "contd.{stream_table}",
    },

    "streams": {
        "drizzle.__drizzle_migrations": None,
        "public.airtable_exports": None,
        "public.audit_log": None,
        "public.beta_invite_links": {
            "select": [
                "-token",
            ],
        },
        "public.cdn_uploads": None,
        "public.coin_ledger": None,
        "public.devlogs": None,
        "public.orders": None,
        "public.pool_projects": None,
        "public.review_events": None,
        "public.sessions": None,
        "public.settings": None,
        "public.shop_items": None,
        "public.shop_requests": None,
        "public.shop_wishlist": None,
        "public.signups": None,
        "public.staff_invites": None,
        "public.submission_activity": None,
        "public.submissions": None,
        "public.users": {
            "select": [
                "-hackatime_token",
            ],
        },
    },
}


@dg.asset(
    name="contd_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def contd_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Contd DB → warehouse in a single shot."""
    context.log.info("Starting Contd → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=contd_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
