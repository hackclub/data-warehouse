import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


flavortown_db_connection = SlingConnectionResource(
    name="FLAVORTOWN_DB",
    type="postgres",
    connection_string=EnvVar("FLAVORTOWN_COOLIFY_URL"),
)


flavortown_replication_config = {
    "source": "FLAVORTOWN_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "flavortown.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # Rails internal tables - disable
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},

        # Tables with id + updated_at - use incremental sync
        "public.action_mailbox_inbound_emails": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        # Append-only; updated_at is unindexed on the source, so key off the indexed bigint PK.
        "public.active_insights_jobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_insights_requests": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.blazer_checks": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboard_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboards": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_credentials": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ledger_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.post_devlogs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        # public.post_ship_events is small, so it stays on the default full-refresh so
        # hard-deletes propagate to the warehouse instead of leaving stale rows.
        "public.posts": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_ideas": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_memberships": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.rsvps": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_card_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_items": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_orders": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_hackatime_projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_identities": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # Append-only; key off the indexed bigint PK rather than an unindexed updated_at.
        "public.extension_usages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.post_git_commits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.funnel_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_storage_blobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_storage_attachments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_storage_variant_records": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        # versions.id is a UUID (not monotonic) -> key off created_at instead.
        "public.versions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # Everything else stays full-refresh (default): tables are small, and a full
        # reload preserves hard-deletes that incremental can't propagate.
    }
}



@dg.asset(
    name="flavortown_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def flavortown_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire FlavorTown DB → warehouse in a single shot."""
    context.log.info("Starting FlavorTown → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, flavortown_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=flavortown_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
