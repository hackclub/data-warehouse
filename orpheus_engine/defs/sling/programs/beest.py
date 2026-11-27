import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


beest_db_connection = SlingConnectionResource(
    name="BEEST_DB",
    type="postgres",
    connection_string=EnvVar("BEEST_COOLIFY_URL"),
)


beest_replication_config = {
    "source": "BEEST_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "beest.{stream_table}",
    },
    "streams": {
        "public.*": None,
        # Sensitive tables: session/credential tokens
        "public._prisma_migrations": {"disabled": True},
        "public.sessions": {"disabled": True},
        "public.hcb_credentials": {"disabled": True},
        "public.lookout_sessions": {"select": ["-token"]},  # Lookout session token
        "public.users": {
            "select": [
                "id", "hca_sub", "email", "name", "nickname", "slack_id",
                "created_at", "updated_at", "two_emails", "hackatime_user_id",
                "has_address", "has_birthdate", "pipes", "gender", "utm_source",
                "utm_medium", "utm_campaign", "referrer", "landing_path", "intent",
                "reviewer_user_note",
            ],  # Excludes hackatime_token, hca_access_token, hca_refresh_token
        },
    },
}



@dg.asset(
    name="beest_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def beest_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Beest DB → warehouse in a single shot."""
    context.log.info("Starting Beest → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=beest_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
