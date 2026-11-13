import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Forge (schema checked 2026-11-13).
forge_db_connection = SlingConnectionResource(
    name="FORGE_DB",
    type="postgres",
    connection_string=_sling_connection_url("FORGE_COOLIFY_URL"),
)


# Not mirrored: app_settings, versions, airtable_queue_items, the solid_queue
# and solid_cache tables, active_storage_variant_records, schema_migrations
# and ar_internal_metadata. Personal and free-text columns are excluded below.
forge_replication_config = {
    "source": "FORGE_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "forge.{stream_table}",
    },
    "streams": {
        "public.active_storage_attachments": None,
        "public.active_storage_blobs": {"select": ["-key", "-metadata"]},
        "public.ahoy_events": None,
        "public.ahoy_visits": {"select": ["-ip", "-visit_token", "-visitor_token", "-latitude", "-longitude"]},
        "public.audit_events": {"select": ["-metadata", "-ip_address"]},
        "public.badges": None,
        "public.coin_adjustments": None,
        "public.collaboration_invites": None,
        "public.contractor_agreements": {"select": ["-signed_ip"]},
        "public.contractor_invoices": {"select": ["-signed_ip"]},
        "public.devlogs": None,
        "public.feature_flags": None,
        "public.guild_states": None,
        "public.kudos": None,
        "public.orders": {"select": ["-hcb_grant_link"]},
        "public.orphaned_lapse_links": None,
        "public.project_collaborators": None,
        "public.project_notes": None,
        "public.project_payouts": None,
        "public.project_views": None,
        "public.projects": None,
        "public.reel_ads": None,
        "public.reel_comments": None,
        "public.reel_images": None,
        "public.reel_kudos": None,
        "public.reel_payout_requests": None,
        "public.reel_views": None,
        "public.reels": None,
        "public.referral_prize_pools": None,
        "public.referrals": None,
        "public.review_sessions": None,
        "public.rsvps": None,
        "public.ships": None,
        "public.shop_item_regions": None,
        "public.shop_items": None,
        "public.spork_checks": {"select": ["-screen_errors", "-error_message"]},
        "public.streak_break_notices": None,
        "public.streak_days": None,
        "public.support_tickets": None,
        "public.user_login_days": None,
        "public.user_notes": None,
        "public.users": {"select": ["-address_line1", "-address_line2", "-postal_code", "-phone_number", "-hca_token"]},
    },
}


@dg.asset(
    name="forge_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def forge_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Forge DB → warehouse in a single shot."""
    context.log.info("Starting Forge → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=forge_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
