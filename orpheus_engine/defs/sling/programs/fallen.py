import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Fallen (Orchard: fallen-db, namespace ysws-fallen).
fallen_db_connection = SlingConnectionResource(
    name="FALLEN_DB",
    type="postgres",
    connection_string=_sling_connection_url("FALLEN_K8S_URL"),
)


# Not mirrored: admin_tokens, mcp_actions, mcp_sql_changes (admin tooling and
# token digests), privacy_token_uses, fallout_invites (invite digests),
# journal_drafts, versions, active_storage_*, active_insights_*, solid_*,
# flipper_*.
fallen_replication_config = {
    "source": "FALLEN_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "fallen.{stream_table}",
    },

    "streams": {
        "public.users": {
            "select": ["-hca_token", "-slack_user_token", "-lapse_token"],
        },
        "public.projects": {
            "select": ["-join_token"],
        },
        "public.project_members": None,
        "public.project_invites": None,
        "public.project_transfers": None,
        "public.journals": None,
        "public.writing_checks": {
            "select": ["-body"],  # Copy of the journal text being checked
        },
        "public.lapse_timelapses": {
            "select": ["-playback_url", "-thumbnail_url"],
        },
        "public.xp_entries": None,
        "public.active_days": None,
        "public.streak_weeks": None,
        "public.streak_adjustments": None,
        "public.time_milestones": None,
        "public.timer_sessions": None,
        "public.huddles": None,
        "public.huddle_sessions": None,
        "public.huddle_events": None,
        "public.huddle_pauses": None,
    },
}


@dg.asset(
    name="fallen_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def fallen_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Fallen DB → warehouse in a single shot."""
    context.log.info("Starting Fallen → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=fallen_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
