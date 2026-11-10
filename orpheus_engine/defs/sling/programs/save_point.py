import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# save_point (Orchard source; schema checked 2026-10-31).
save_point_db_connection = SlingConnectionResource(
    name="SAVE_POINT_DB",
    type="postgres",
    connection_string=_sling_connection_url("SAVE_POINT_K8S_URL"),
)


# Admin tokens, Rails metadata/migrations, cable/cache and job queues stay out.
# Address and Hackatime credentials are encrypted at rest; buddy/nudge links are bearer codes.
# Visit paths and users' last seen paths can also contain buddy/nudge bearer codes.
# Blob keys/metadata and Slack errors can carry private access or contact details.
# insights views duplicate the public tables and would collide at {stream_table}.
# No application key/value settings tables exist (catalog checked 2026-10-31).
save_point_replication_config = {
    "source": "SAVE_POINT_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "save_point.{stream_table}"
    },
    "streams": {
        "public.active_storage_attachments": None,
        "public.active_storage_blobs": {
            "select": [
                "-key",
                "-metadata"
            ]
        },
        "public.active_storage_variant_records": None,
        "public.admin_audit_events": None,
        "public.buddy_pomodoros": None,
        "public.claims": None,
        "public.coded_buckets": None,
        "public.extensions": None,
        "public.faq_reads": None,
        "public.metric_snapshots": None,
        "public.nps_responses": None,
        "public.nudges": {
            "select": [
                "-token"
            ]
        },
        "public.orders": {
            "select": [
                "-address"
            ]
        },
        "public.pairs": None,
        "public.projects": {
            "select": [
                "-buddy_code"
            ]
        },
        "public.rewards": None,
        "public.ship_send_backs": None,
        "public.ships": None,
        "public.site_days": None,
        "public.streak_activities": None,
        "public.users": {
            "select": [
                "-hackatime_access_token",
                "-address",
                "-phone_number",
                "-slack_dm_error",
                "-last_seen_path"
            ]
        },
        "public.visits": {
            "select": [
                "-path"
            ]
        }
    }
}


@dg.asset(
    name="save_point_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def save_point_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the save_point DB → warehouse in a single shot."""
    context.log.info("Starting save_point → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=save_point_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
