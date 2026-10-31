import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# contd (Orchard: production_database, namespace ysws-contd).
contd_db_connection = SlingConnectionResource(
    name="CONTD_DB",
    type="postgres",
    connection_string=_sling_connection_url("CONTD_K8S_URL"),
)


# Not mirrored: sessions, api_keys (key hashes) and drizzle.__drizzle_migrations.
# settings holds only launch dates and coins_per_hour (checked 2026-10-31).
contd_replication_config = {
    "source": "CONTD_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "contd.{stream_table}",
    },

    "streams": {
        "public.airtable_exports": {
            # Full Unified export payload, contact details included
            "select": ["-fields"],
        },
        "public.audit_log": None,
        "public.beta_invite_links": {
            # Unused invite links still work
            "select": ["-token"],
        },
        "public.cdn_uploads": None,
        "public.coin_ledger": None,
        "public.devlogs": None,
        "public.hackatime_activity": None,
        "public.orders": {
            # Shipping address
            "select": ["-address"],
        },
        "public.pool_projects": None,
        "public.review_events": None,
        "public.settings": None,
        "public.shop_items": None,
        "public.shop_requests": None,
        "public.shop_wishlist": None,
        "public.signups": None,
        "public.staff_invites": None,
        "public.submission_activity": None,
        "public.submissions": None,
        "public.users": {
            # address JSON holds street, postcode and region
            "select": ["-hackatime_token", "-address"],
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
    """Replicates the contd DB → warehouse in a single shot."""
    context.log.info("Starting contd → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=contd_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
