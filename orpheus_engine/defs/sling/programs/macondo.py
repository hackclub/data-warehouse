import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


macondo_db_connection = SlingConnectionResource(
    name="MACONDO_DB",
    type="postgres",
    connection_string=_sling_connection_url("MACONDO_K8S_URL"),
)


# Macondo (Orchard: macondo-prod-pg18, namespace ysws-macondo; moved off Coolify
# 2026-10-02). Every table is listed; new tables need a review before they sync.
# Not mirrored: sessions, api_keys, mcp_tokens, mcp_auth_codes and
# internal_oauth_connections (tokens and hashes), pii_locker (phone, birthday,
# addresses), user_backups (payload copies a deleted user's sessions, OAuth
# connections and pii_locker rows) and drizzle.__drizzle_migrations.
# audit_logs old/new values can hold any changed field, including PII.
# app_settings holds only feature flags (key names checked 2026-11-10).
# Schema checked 2026-11-10.
macondo_replication_config = {
    "source": "MACONDO_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "macondo.{stream_table}",
    },
    "streams": {
        "public.analytics_pageviews": None,
        "public.announcements": None,
        "public.app_settings": None,
        "public.audit_logs": {"select": ["-old_value", "-new_value"]},
        "public.certificates": {"select": ["-code"]},
        "public.currency_transactions": None,
        "public.daily_project_activity": None,
        "public.dau_snapshots": None,
        "public.docs_search_misses": None,
        "public.event_attendees": None,
        "public.events": None,
        "public.faq_entries": None,
        "public.faq_suggestions": None,
        "public.hackatime_sync_audits": None,
        "public.internal_notes": None,
        "public.journals": None,
        "public.kb_entries": None,
        "public.kb_sources": None,
        "public.kb_suggestions": None,
        "public.landing_page_clicks": {"select": ["-visitor_cookie"]},
        "public.loops_event_logs": None,
        "public.macondo_bans": None,
        "public.notification_deliveries": None,
        "public.notification_preferences": None,
        "public.notifications": None,
        "public.nps_insights": None,
        "public.nps_response_replies": None,
        "public.nps_responses": None,
        "public.project_collaborators": None,
        "public.project_grants": None,
        "public.project_hackatime_links": None,
        "public.project_ideas": None,
        "public.project_notes": None,
        "public.project_upvotes": None,
        "public.projects": {"select": ["-invite_code"]},
        "public.referral_redemptions": None,
        "public.referral_visits": {"select": ["-visitor_cookie", "-ip_address"]},
        "public.review_escalations": None,
        "public.shadow_bans": None,
        "public.ship_contributors": None,
        "public.ships": None,
        "public.shop_item_request_votes": None,
        "public.shop_item_requests": None,
        "public.shop_item_stars": None,
        "public.shop_item_unlocks": None,
        "public.shop_items": None,
        "public.shop_orders": {"select": ["-shipping_address", "-phone", "-tracking_number", "-address_encrypted"]},
        "public.shop_pending_changes": None,
        "public.signup_emails": None,
        "public.streak_days": None,
        "public.support_ticket_attachments": {"select": ["-url"]},
        "public.support_ticket_messages": None,
        "public.support_tickets": None,
        "public.user_achievements": None,
        "public.users": {"select": ["-onboarding_data", "-github_token", "-hackatime_oauth_token", "-hca_refresh_token"]},
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
