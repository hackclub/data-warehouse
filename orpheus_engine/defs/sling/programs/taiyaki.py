import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Taiyaki (Orchard: taiyaki-db, namespace ysws-taiyaki; schema checked 2026-11-11).
taiyaki_db_connection = SlingConnectionResource(
    name="TAIYAKI_DB",
    type="postgres",
    connection_string=_sling_connection_url("TAIYAKI_K8S_URL"),
)


# Not mirrored: user_sessions (session ids), hackatime_link_otps (one-time
# codes), sticker_tokens (bearer claim tokens) and _prisma_migrations.
# Gift codes and their prefilled claim links are bearer codes. email_jobs
# metadata carries template variables; global_settings holds only flags.
taiyaki_replication_config = {
    "source": "TAIYAKI_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "taiyaki.{stream_table}",
    },
    "streams": {
        "public.announcement_reads": None,
        "public.announcements": None,
        "public.community_events": None,
        "public.devlogs": None,
        "public.email_jobs": {"select": ["-errorMessage", "-metadata"]},
        "public.gift_codes": {"select": ["-code", "-fillout_url"]},
        "public.global_settings": None,
        "public.historical_metrics": None,
        "public.pinned_items": None,
        "public.projects": None,
        "public.reviewer_checklists": None,
        "public.reviewer_payouts": None,
        "public.shop_item_variants": None,
        "public.shop_items": None,
        "public.shops": None,
        "public.submission_audit_logs": None,
        "public.submissions": None,
        "public.transactions": None,
        "public.user_daily_activity": None,
        "public.users": {"select": ["-address_line_1", "-address_line_2", "-zip_code", "-hackatime_access_token", "-phone_number"]},
        "public.users_airtable": {"select": ["-code"]},
    },
}


@dg.asset(
    name="taiyaki_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def taiyaki_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Taiyaki DB → warehouse in a single shot."""
    context.log.info("Starting Taiyaki → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=taiyaki_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
