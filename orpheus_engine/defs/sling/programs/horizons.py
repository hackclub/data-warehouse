import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _sling_connection_url
from .._helpers import _ensure_incremental_target_indexes


horizons_db_connection = SlingConnectionResource(
    name="HORIZONS_DB",
    type="postgres",
    connection_string=_sling_connection_url("HORIZONS_K8S_URL"),
)


horizons_replication_config = {
    "source": "HORIZONS_DB",
    "target": "WAREHOUSE_DB",

    # Create warehouse tables for listed streams that are currently empty, so
    # every configured stream exists in the warehouse. Streams are listed
    # explicitly rather than via public.* because hc_warehouse_reader holds
    # table and column SELECT grants only on these 26 tables.
    "env": {"SLING_ALLOW_EMPTY": True},

    # Every stream is full-refresh, so the mirror matches the source exactly
    # after each run. Horizons changes rows without bumping updated_at (soft
    # deletes, Joe fraud fields, screenshot/readme URLs), mutates rows in
    # "append-only" tables (transactions.fulfilled_at,
    # user_sessions.expires_at) and hard-deletes rows (including
    # submission_audit_logs); incremental mirrors drifted on thousands of rows.
    # Incremental streams also never backfill newly synced columns and keep
    # stale column types (projects broke on 2026-10-01 when
    # hours_justification outgrew varchar(500)). The largest table,
    # submission_audit_logs, is ~600k rows and copies in under a minute.
    "defaults": {
        "mode": "full-refresh",
        "object": "horizons.{stream_table}",
    },

    # All 26 tables are listed explicitly; live credentials are excluded.
    "streams": {
        "public._prisma_migrations": None,
        "public.announcement_reads": None,
        "public.announcements": None,
        "public.community_events": None,
        "public.email_jobs": None,
        "public.events": None,
        # Unredeemed codes are spendable.
        "public.gift_codes": {
            "select": ["-code"],
        },
        "public.global_settings": None,
        "public.hackatime_link_otps": {
            "select": ["-otp_code"],
        },
        "public.historical_metrics": None,
        "public.pinned_events": None,
        "public.pinned_items": None,
        "public.projects": None,
        "public.reviewer_checklists": None,
        "public.reviewer_payouts": None,
        "public.shop_item_variants": None,
        "public.shop_items": None,
        "public.shops": None,
        "public.sticker_tokens": {
            "select": ["-token"],
        },
        "public.submission_audit_logs": None,
        "public.submissions": None,
        "public.transactions": None,
        "public.user_daily_activity": None,
        # user_sessions.id is the sessionId login cookie. Sync a SHA-256 of it
        # instead so rows stay distinct and joinable in the warehouse without
        # storing the raw cookie. Note: hc_warehouse_reader still needs SELECT
        # on id because Postgres computes the digest on the source server.
        "public.user_sessions": {
            "sql": """
                select encode(sha256(convert_to(id, 'UTF8')), 'hex') as id_sha256,
                       user_id, expires_at, created_at
                from public.user_sessions
            """,
        },
        "public.users": {
            "select": ["-hackatime_access_token"],
        },
        "public.users_airtable": None,
    }
}



@dg.asset(
    name="horizons_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def horizons_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Horizons DB → warehouse in a single shot."""
    context.log.info("Starting Horizons → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, horizons_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=horizons_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
