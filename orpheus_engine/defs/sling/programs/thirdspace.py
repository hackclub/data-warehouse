import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _sling_connection_url
from .._helpers import _ensure_incremental_target_indexes


thirdspace_db_connection = SlingConnectionResource(
    name="THIRDSPACE_DB",
    type="postgres",
    connection_string=_sling_connection_url("THIRDSPACE_K8S_URL"),
)


thirdspace_replication_config = {
    "source": "THIRDSPACE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "thirdspace.{stream_table}",
    },

    # All thirdspace tables are small (<2,000 rows each), so full-refresh
    # keeps upstream hard-deletes in sync with negligible load on the 6h job.
    "streams": {
        "public.addresses": {
            "select": [
                "id", "user_id", "city", "state", "postcode", "country",
                "is_default", "created_at", "updated_at",
            ],  # Excludes recipient, line_1, line_2
        },
        "public.claim_confirmations": None,
        "public.claims": None,
        "public.group_members": None,
        "public.groups": None,
        "public.hackatime_tokens": {
            "select": [
                "id", "user_id", "hackatime_user_id", "scope", "expires_at",
                "created_at", "updated_at",
            ],  # Excludes access_token, refresh_token
        },
        "public.hours_logs": None,
        "public.invites": None,
        "public.prize_tiers": None,
        "public.project_contributor_hours": None,
        "public.project_hackatime": None,
        "public.project_repos": None,
        "public.project_weeks": None,
        "public.projects": None,
        "public.rate_limit_events": None,
        "public.referrals": {
            "select": ["id", "source", "created_at", "consumed_at"],
            # excludes: ["invitee_email", "referrer_email"]
        },
        "public.shop_items": None,
        "public.shop_redemptions": None,
        "public.users": {
            "select": [
                "id", "hackatime_id", "slack_id", "airtable_record_id",
                "stamps", "created_at", "updated_at", "eliminated_at",
                "eliminated_week", "eliminated_reason", "trust_level",
                "trust_level_checked_at", "airtable_bonus_stamps", "hc_sub",
                "hour_debt_hours", "hour_debt_due_week",
            ],  # Excludes name, email, display_name
        },
        "public.week_saves": None,
    },
}



@dg.asset(
    name="thirdspace_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def thirdspace_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates analytics-safe thirdspace DB columns into the warehouse."""
    context.log.info("Starting thirdspace → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, thirdspace_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=thirdspace_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
