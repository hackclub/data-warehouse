import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


phantom_db_connection = SlingConnectionResource(
    name="PHANTOM_DB",
    type="postgres",
    connection_string=EnvVar("PHANTOM_DATABASE_URL"),
)


phantom_replication_config = {
    "source": "PHANTOM_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "phantom.{stream_table}",
    },

    "streams": {
        "public.audit_events": None,
        "public.job_runs": None,
        "public.ledger_accounts": None,
        "public.ledger_entries": None,
        "public.oauth_states": {"select": ["-state", "-code_verifier"]},  # OAuth PKCE secrets
        "public.order_transitions": None,
        "public.orders": {
            "select": [
                "id", "user_id", "product_id", "product_version",
                "product_name", "product_kind", "currency", "unit_price",
                "quantity", "total_price", "options", "state", "state_at",
                "idempotency_key", "replaces_order_id", "stock_taken",
                "created_at",
            ],
        },
        "public.outbox_events": None,
        "public.product_categories": None,
        "public.product_revisions": None,
        "public.products": None,
        "public.project_showcase": None,
        "public.projects": None,
        "public.provider_accounts": {
            "select": [
                "id", "user_id", "provider", "provider_account_id", "scope",
                "profile", "linked_at", "updated_at",
            ],
        },
        "public.sessions": {
            "select": [
                "id", "user_id", "expires_at", "revoked_at", "user_agent",
                "ip_prefix", "created_at", "last_seen_at",
            ],
        },
        "public.site_settings": None,
        "public.submission_transitions": None,
        "public.users": None,
        "public.submissions": {
            "select": [
                "id", "project_id", "user_id", "name", "description",
                "repo_url", "demo_url", "screenshot_url",
                "hackatime_projects", "ai_declaration", "claimed_seconds",
                "changelog", "state", "state_at", "reviewer_id",
                "approver_id", "awarded_seconds", "public_message",
                "justification", "created_at", "update_declaration",
                "notes_for_reviewer",
            ],
        },
    },
}



@dg.asset(
    name="phantom_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def phantom_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Phantom DB → warehouse in a single shot."""
    context.log.info("Starting Phantom → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=phantom_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
