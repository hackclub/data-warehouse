import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Coral (Orchard: primary, namespace ysws-coral).
coral_db_connection = SlingConnectionResource(
    name="CORAL_DB",
    type="postgres",
    connection_string=_sling_connection_url("CORAL_K8S_URL"),
)


# Not mirrored: app_settings (its one row is an Anthropic auth token),
# lookout_sessions (timelapse session tokens), schema_migrations and
# ar_internal_metadata. active_storage_blobs.key is an object key in a
# private R2 bucket (no public: true in storage.yml), not a credential.
coral_replication_config = {
    "source": "CORAL_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "coral.{stream_table}",
    },

    "streams": {
        "public.active_storage_attachments": None,
        "public.active_storage_blobs": None,
        "public.active_storage_variant_records": None,
        "public.ahoy_events": None,
        "public.ahoy_visits": {
            "select": [
                "-ip", "-latitude", "-longitude", "-visit_token",
                "-visitor_token",
            ],
        },
        "public.airtable_queue_items": {
            # Airtable payload holds address, birthday and ZIP
            "select": ["-payload"],
        },
        "public.audit_events": {
            # metadata.changes holds address, phone and hca_token diffs
            "select": ["-ip_address", "-metadata"],
        },
        "public.badges": None,
        "public.coin_adjustments": None,
        "public.collaboration_invites": None,
        "public.devlogs": None,
        "public.feature_flags": None,
        "public.fraud_checks": None,
        "public.kudos": None,
        "public.orders": {
            # HCB grant links and parcel tracking
            "select": [
                "-shipping_address", "-tracking_link", "-hcb_grant_link",
            ],
        },
        "public.project_collaborators": None,
        "public.project_notes": None,
        "public.project_payouts": None,
        "public.project_views": None,
        "public.projects": None,
        "public.referral_prize_pools": None,
        "public.referrals": None,
        "public.shop_item_option_groups": None,
        "public.shop_item_options": None,
        "public.shop_item_regions": None,
        "public.shop_items": None,
        "public.signup_intents": None,
        "public.streak_break_notices": None,
        "public.streak_days": None,
        "public.submission_reviews": None,
        "public.submissions": None,
        "public.support_tickets": None,
        "public.tribe_states": None,
        "public.user_login_days": None,
        "public.user_notes": None,
        "public.users": {
            # shop_address JSON holds the full shipping address
            "select": [
                "-hca_token", "-address_line1", "-address_line2",
                "-postal_code", "-phone_number", "-shop_address",
            ],
        },
        "public.versions": {
            # PaperTrail snapshots hold address, phone and hca_token
            "select": ["-object"],
        },
    },
}


@dg.asset(
    name="coral_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def coral_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Coral DB → warehouse in a single shot."""
    context.log.info("Starting Coral → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=coral_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
