import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Scraps v2 (Orchard: database_prod, namespace ysws-scraps).
scraps_db_connection = SlingConnectionResource(
    name="SCRAPS_DB",
    type="postgres",
    connection_string=_sling_connection_url("SCRAPS_K8S_URL"),
)


# Not mirrored: sessions, the blazer_* BI tool tables, schema_migrations,
# ar_internal_metadata and drizzle.__drizzle_migrations.
scraps_replication_config = {
    "source": "SCRAPS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "scraps.{stream_table}",
    },

    "streams": {
        "public.admin_deleted_orders": {
            # deleted_payload is the whole order, address included
            "select": ["-deleted_payload", "-phone", "-shipping_address"],
        },
        "public.login_allowlist_entries": None,
        "public.news": None,
        "public.payout_rolls": None,
        "public.project_activity": None,
        "public.projects": None,
        "public.referral_reward_claims": None,
        "public.referrals": {
            "select": ["-code"],
        },
        "public.refinery_orders": None,
        "public.refinery_spending_history": None,
        "public.review_macros": None,
        "public.reviews": None,
        "public.shop_gachapon_items": None,
        "public.shop_gachapons": None,
        "public.shop_hearts": None,
        "public.shop_items": None,
        "public.shop_orders": {
            "select": ["-phone", "-shipping_address", "-tracking_number"],
        },
        "public.shop_penalties": None,
        "public.shop_retained_items": None,
        "public.shop_rolls": None,
        "public.signup_sources": None,
        "public.user_activity": None,
        "public.user_bonuses": None,
        "public.users": {
            "select": [
                "-access_token", "-id_token", "-refresh_token", "-phone",
                "-address_line1", "-address_line2", "-address_postal_code",
                "-referral_code",
            ],
        },
    },
}


@dg.asset(
    name="scraps_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def scraps_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Scraps v2 DB → warehouse in a single shot."""
    context.log.info("Starting Scraps v2 → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=scraps_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
