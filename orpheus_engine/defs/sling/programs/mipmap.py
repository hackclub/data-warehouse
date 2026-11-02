import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Mipmap (Orchard: mipmap-db, project "Active YSWS Websites").
mipmap_db_connection = SlingConnectionResource(
    name="MIPMAP_DB",
    type="postgres",
    connection_string=_sling_connection_url("MIPMAP_K8S_URL"),
)


# Not mirrored: auth_token (encrypted OAuth tokens), session (token hashes),
# _sqlx_migrations, and the project_hackatime and user_loops views.
# Street address lines, postcodes, phone numbers and tracking numbers are
# dropped; city, state and country are kept.
mipmap_replication_config = {
    "source": "MIPMAP_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "mipmap.{stream_table}",
    },

    "streams": {
        "public.active_day": None,
        "public.admin_action": None,
        "public.airtable_sync": None,
        "public.feedback": None,
        "public.login": None,
        "public.order": {
            "select": [
                "-tracking_number", "-address_line_1", "-address_line_2",
                "-address_postal_code", "-address_phone_number",
            ],
        },
        "public.project": None,
        "public.slack_announcement": None,
        "public.slack_message": None,
        "public.slack_preference": None,
        "public.snapshot": {
            "select": ["-held_image"],  # Image bytes held for moderation
        },
        "public.snapshot_rejection": None,
        "public.sticker_wave": None,
        "public.submission": None,
        "public.survey_response": None,
        "public.unlock": None,
        "public.user": {
            "select": [
                "-phone_number", "-address_line_1", "-address_line_2",
                "-address_postal_code", "-address_phone_number",
            ],
        },
        "public.visitor": None,
    },
}


@dg.asset(
    name="mipmap_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def mipmap_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Mipmap DB → warehouse in a single shot."""
    context.log.info("Starting Mipmap → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=mipmap_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
