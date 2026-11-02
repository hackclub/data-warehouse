import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Stern (Orchard: stern-db, namespace ysws-archer-s-project).
stern_db_connection = SlingConnectionResource(
    name="STERN_DB",
    type="postgres",
    connection_string=_sling_connection_url("STERN_K8S_URL"),
)


# Stern is the shared YSWS platform behind OrpheusAthon, CloudFall, Phantom,
# Contd, Capitol, Future and Digit; its apps table lists them.
# Not mirrored: OAuth and API credentials (airtable_connections,
# hcb_connections, api_keys, developer_keys, platform_api_keys, oauth_codes,
# app_sessions), rate_limit_buckets, github_commit_cache, github_compare_cache
# and _prisma_migrations. games.config holds only scoring settings.
stern_replication_config = {
    "source": "STERN_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "stern.{stream_table}",
    },

    "streams": {
        "public.admin_pinned_hcb_users": None,
        "public.app_invites": {
            # Invite codes still work
            "select": ["-code"],
        },
        "public.apps": {
            "select": ["-clientSecretHash"],
        },
        "public.audit_log": None,
        "public.currency_transactions": None,
        "public.devlog_deflations": None,
        "public.devlogs": None,
        "public.events": None,
        "public.explore_examples": None,
        "public.faq_items": None,
        "public.faq_questions": None,
        "public.follow_ups": None,
        "public.game_scores": None,
        "public.games": None,
        "public.hackatime_project_files": None,
        "public.hcb_networth_snapshots": None,
        "public.hcb_org_ownerships": None,
        "public.hour_transactions": None,
        "public.news_items": None,
        "public.orders": None,
        "public.project_audits": None,
        "public.project_commit_snapshots": None,
        "public.project_fraud_reviews": None,
        "public.project_likes": None,
        "public.project_review_drafts": None,
        "public.project_reviews": None,
        "public.project_submissions": None,
        "public.projects": None,
        "public.shop_item_submissions": None,
        "public.shop_item_survey_questions": None,
        "public.shop_item_volume_tiers": None,
        "public.shop_items": None,
        "public.shop_price_grants": None,
        "public.shop_sales": None,
        "public.spot_check_invite_uses": None,
        "public.spot_check_invites": {
            # Invite codes still work
            "select": ["-code"],
        },
        "public.stern_admins": None,
        "public.stern_announcements": None,
        "public.stern_milestones": None,
        "public.stern_suggestion_votes": None,
        "public.stern_suggestions": {
            "select": ["-submitter_ip_hash"],
        },
        "public.survey_answers": None,
        "public.survey_questions": None,
        "public.survey_responses": None,
        "public.user_app_profiles": {
            # Encrypted address, coordinates and ZIP
            "select": ["-onboarding_answers"],
        },
        "public.users": {
            # Encrypted contact details and tokens
            "select": [
                "-email_enc", "-birthday_enc", "-address_line1_enc",
                "-address_line2_enc", "-address_city_enc",
                "-address_state_enc", "-address_country_enc",
                "-address_zip_enc", "-hackatime_token_enc",
                "-hca_access_token_enc", "-hca_refresh_token_enc",
                "-lapse_token_enc",
            ],
        },
    },
}


@dg.asset(
    name="stern_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stern_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Stern DB → warehouse in a single shot."""
    context.log.info("Starting Stern → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=stern_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
