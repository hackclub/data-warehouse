import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


stasis_db_connection = SlingConnectionResource(
    name="STASIS_DB",
    type="postgres",
    connection_string=EnvVar("STASIS_COOLIFY_URL"),
)


stasis_replication_config = {
    "source": "STASIS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "stasis.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updatedAt ---
        "public.account": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "id", "accountId", "providerId", "userId",
                "accessTokenExpiresAt", "refreshTokenExpiresAt",
                "scope", "createdAt", "updatedAt",
            ],  # Excludes accessToken, refreshToken, idToken, password
        },
        "public.event": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.reviewer_note": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.session": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "-token",
            ],  # Excludes token
        },
        "public.shop_item": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.temp_rsvp": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "-ip",
            ],  # Excludes ip
        },
        "public.user": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.verification": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "-value",
            ],  # Excludes the verification code
        },

        # --- Incremental: id + createdAt (append-only) ---
        "public.audit_log": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.bom_item": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.currency_transaction": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.hackatime_project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.kudos": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.project_review_action": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.project_submission": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.review_claim": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.session_media": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.session_timelapse": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.submission_review": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.work_session": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },

        # --- Disabled: infrastructure / no timestamps ---
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
        "public._prisma_migrations": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # project_badge (no timestamp, 2.5K rows)
        # sidekick_assignment (no timestamp, 2.6K rows)
        # user_role (no timestamp, 54 rows)
    }
}



@dg.asset(
    name="stasis_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stasis_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Stasis DB → warehouse in a single shot."""
    context.log.info("Starting Stasis → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, stasis_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=stasis_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
