import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Player Two (Orchard; schema checked 2026-11-11).
player_two_db_connection = SlingConnectionResource(
    name="PLAYER_TWO_DB",
    type="postgres",
    connection_string=_sling_connection_url("PLAYER_TWO_K8S_URL"),
)


# Not mirrored: jobs (worker queue; payloads and errors) and
# drizzle.__drizzle_migrations. identities.token_enc and orders.address_enc are
# encrypted secrets; invite codes are share codes. audit_log meta and send or
# sync errors are free text.
player_two_replication_config = {
    "source": "PLAYER_TWO_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "player_two.{stream_table}",
    },
    "streams": {
        "public.activity_days": None,
        "public.admin_messages": {"select": ["-error"]},
        "public.audit_log": {"select": ["-meta"]},
        "public.devlogs": None,
        "public.friendships": None,
        "public.identities": {"select": ["-token_enc"]},
        "public.item_stars": None,
        "public.ledger_entries": None,
        "public.nudges": None,
        "public.orders": {"select": ["-address_enc", "-reference"]},
        "public.project_hackatime_links": None,
        "public.projects": None,
        "public.rsvps": None,
        "public.ships": {"select": ["-ari_error", "-airtable_error"]},
        "public.shop_items": None,
        "public.users": {"select": ["-invite_code"]},
        "public.webhook_deliveries": None,
    },
}


@dg.asset(
    name="player_two_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def player_two_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Player Two DB → warehouse in a single shot."""
    context.log.info("Starting Player Two → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=player_two_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
