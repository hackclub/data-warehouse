import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Canvas uses sealed session cookies, not a session table. hca and hackatime
# contain encrypted OAuth credentials; bot_log.details includes referral links.
# shop_orders.address is an encrypted shipping address. No key/value settings tables.
# Not mirrored: drizzle.__drizzle_migrations.
# Schema and kept-column reads checked 2026-10-31.
canvas_db_connection = SlingConnectionResource(
    name="CANVAS_DB",
    type="postgres",
    connection_string=_sling_connection_url("CANVAS_K8S_URL"),
)


canvas_replication_config = {
    "source": "CANVAS_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "canvas.{stream_table}"
    },
    "streams": {
        "public.admin_actions": None,
        "public.admin_notes": None,
        "public.bonus_point_transactions": None,
        "public.bot_log": {
            "select": [
                "-details"
            ]
        },
        "public.canvas_pixel_log": None,
        "public.canvas_pixels": None,
        "public.daily_active_logins": None,
        "public.daily_active_users": None,
        "public.pixel_transactions": None,
        "public.projects": None,
        "public.ships": None,
        "public.shop_items": None,
        "public.shop_orders": {
            "select": [
                "-address"
            ]
        },
        "public.stat_snapshots": None,
        "public.user_activity": None,
        "public.user_coding_days": None,
        "public.users": {
            "select": [
                "-hca",
                "-hackatime",
                "-referral_code"
            ]
        }
    }
}


@dg.asset(
    name="canvas_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def canvas_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the canvas DB → warehouse in a single shot."""
    context.log.info("Starting canvas → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=canvas_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
