import os

import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource

from .._helpers import _sling_connection_url


# Haggle (Django app). Not mirrored: public.competitions_answerkey and Django
# auth/session/migration tables (public.auth_*, public.django_*).
# Excluded columns on competitions_ship: screenshot, predictions (binary blobs) and
# log (staff-only output).
haggle_db_connection = SlingConnectionResource(
    name="HAGGLE_DB",
    type="postgres",
    connection_string=_sling_connection_url("HAGGLE_K8S_URL"),
)


haggle_replication_config = {
    "source": "HAGGLE_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "haggle.{stream_table}",
    },
    "streams": {
        "public.competitions_ship": {
            "select": [
                "-screenshot",
                "-predictions",
                "-log",
            ],
        },
        "public.rsvp_event": None,
        "public.rsvp_rsvp": None,
        "public.rsvp_visitor": None,
    },
}


@dg.asset(
    name="haggle_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
    output_required=False,
)
def haggle_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
):
    """Replicates the Haggle DB → warehouse in a single shot.

    When HAGGLE_K8S_URL is unset, yields no Output (so no ASSET_MATERIALIZATION
    is logged and warehouse_metadata.mirror_freshness continues reporting
    never_synced) and records an AssetObservation instead of failing the job.
    """
    if not os.getenv("HAGGLE_K8S_URL"):
        context.log.warning(
            "HAGGLE_K8S_URL is not set; skipping Haggle replication until the "
            "Orchard reader is provisioned."
        )
        context.log_event(
            dg.AssetObservation(
                asset_key=context.asset_key,
                metadata={"replicated": False, "skipped_reason": "HAGGLE_K8S_URL_unset"},
            )
        )
        return

    context.log.info("Starting Haggle → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=haggle_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    yield dg.Output(None, metadata={"replicated": True})
