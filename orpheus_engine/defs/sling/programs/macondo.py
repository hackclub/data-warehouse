import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


macondo_db_connection = SlingConnectionResource(
    name="MACONDO_DB",
    type="postgres",
    connection_string=_sling_connection_url("MACONDO_K8S_URL"),
)


macondo_replication_config = {
    "source": "MACONDO_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "macondo.{stream_table}",
    },
    "streams": {
        "public.*": None,
        # Sensitive tables: tokens / PII
        "public._prisma_migrations": {"disabled": True},
        "public.sessions": {"disabled": True},
        "public.pii_locker": {"disabled": True},
        "public.internal_oauth_connections": {"disabled": True},
        "public.users": {
            "select": [
                "id", "name", "email", "image", "created_at", "updated_at", "sub",
                "slack_id", "username", "hackatime_id", "hcb_email", "locale",
                "timezone", "roles", "is_temp", "onboarding_step", "completed_guides",
                "last_seen_at", "last_login_at", "streak_freezes_remaining",
                "last_hackatime_total_hours", "last_hackatime_synced_at", "github_id",
                "country", "region_override", "referral_code", "referred_by_user_id",
                "referred_at", "last_hackatime_total_seconds", "preferred_reminder_hour",
                "streak_slack_notifications", "last_active_date", "hca_verification_status",
                "hca_ysws_eligible", "auto_use_streak_freezes",
                "slack_macondo_auto_invited_at", "lifetime_fruits_earned",
                "hca_last_sync_at", "username_synced_at", "reminder_hours_before_day_end",
                "reminder_local_hours", "hackatime_timezone",
            ],  # Excludes github_token, hackatime_oauth_token, hca_refresh_token,
                # github access; onboarding_data dropped as free-form blob
        },
    },
}



@dg.asset(
    name="macondo_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def macondo_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Macondo DB → warehouse in a single shot."""
    context.log.info("Starting Macondo → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=macondo_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
