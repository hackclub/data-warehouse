import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


fabricate_db_connection = SlingConnectionResource(
    name="FABRICATE_DB",
    type="postgres",
    connection_string=EnvVar("FABRICATE_DATABASE_URL"),
)


fabricate_replication_config = {
    "source": "FABRICATE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "fabricate.{stream_table}",
    },

    "streams": {
        "public.active_storage_attachments": None,
        "public.active_storage_blobs": None,
        "public.active_storage_variant_records": {
            "select": [
                "-variation_digest",
            ],
        },
        "public.ahoy_events": None,
        "public.ahoy_visits": {
            "select": [
                "-visit_token", "-visitor_token",
            ],
        },
        "public.chip_transactions": None,
        "public.feature_flags": None,
        "public.grants": None,
        "public.journal_entries": {
            "select": [
                "-sync_digest",
            ],
        },
        "public.path_redemptions": None,
        "public.project_votes": None,
        "public.projects": None,
        "public.referrals": None,
        "public.review_notes": None,
        "public.ships": None,
        "public.uploads": {
            "select": [
                "-digest",
            ],
        },
        "public.users": {
            "select": [
                "-hca_token",
            ],
        },
        "public.versions": None,
    },
}


@dg.asset(
    name="fabricate_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def fabricate_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Fabricate DB → warehouse in a single shot."""
    context.log.info("Starting Fabricate → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=fabricate_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
