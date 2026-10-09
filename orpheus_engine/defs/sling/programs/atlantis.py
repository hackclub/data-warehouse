import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


atlantis_db_connection = SlingConnectionResource(
    name="ATLANTIS_DB",
    type="postgres",
    connection_string=EnvVar("ATLANTIS_DATABASE_URL"),
)


atlantis_replication_config = {
    "source": "ATLANTIS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "atlantis.{stream_table}",
    },

    "streams": {
        "public.atlantis_cache": None,
        "public.atlantis_site_activeday": None,
        "public.atlantis_site_airtablesubmission": None,
        "public.atlantis_site_auditlog": None,
        "public.atlantis_site_internalcomment": None,
        "public.atlantis_site_item": None,
        "public.atlantis_site_journal": None,
        "public.atlantis_site_lapseaccount": {
            "select": [
                "-encrypted_token",
            ],
        },
        "public.atlantis_site_metricssnapshot": None,
        "public.atlantis_site_order": None,
        "public.atlantis_site_pearlbracket": None,
        "public.atlantis_site_permissions": None,
        "public.atlantis_site_printerclaim": None,
        "public.atlantis_site_profile": {
            "select": [
                "-encrypted_hca_token",
            ],
        },
        "public.atlantis_site_project": None,
        "public.atlantis_site_project_followers": None,
        "public.atlantis_site_savercredit": None,
        "public.atlantis_site_ship": None,
        "public.atlantis_site_shopcategory": None,
        "public.atlantis_site_t1": None,
        "public.atlantis_site_t2": None,
        "public.atlantis_site_t3": None,
        "public.atlantis_site_timelapse": {
            "select": [
                "-token",
            ],
        },
        "public.atlantis_site_timelapseannotation": None,
        "public.atlantis_site_timelapseremoval": None,
        "public.atlantis_site_timelapsereview": None,
        "public.atlantis_site_weekoutcome": None,
        "public.atlantis_site_weekreminder": None,
        "public.auth_group": None,
        "public.auth_group_permissions": None,
        "public.auth_permission": None,
        "public.auth_user": {
            "select": [
                "-password",
            ],
        },
        "public.auth_user_groups": None,
        "public.auth_user_user_permissions": None,
        "public.django_admin_log": None,
        "public.django_content_type": None,
        "public.django_migrations": None,
        "public.django_session": None,
    },
}


@dg.asset(
    name="atlantis_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def atlantis_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Atlantis DB → warehouse in a single shot."""
    context.log.info("Starting Atlantis → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=atlantis_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
