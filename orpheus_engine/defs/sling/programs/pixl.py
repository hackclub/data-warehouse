import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Pixl (Orchard: pixl-new-db, namespace ysws-pixl).
pixl_db_connection = SlingConnectionResource(
    name="PIXL_DB",
    type="postgres",
    connection_string=_sling_connection_url("PIXL_K8S_URL"),
)


# Not mirrored: form_verifications (verification code hashes),
# ships_snapshot_state (refresh token), cdn_upload_quota (rate limits),
# pixo_qa_cache, drizzle.__drizzle_migrations, and the one-off _bak_* and
# *_backup_* snapshot tables. Street address lines, postcodes, phone numbers
# and tracking numbers are dropped; city, state and country are kept.
pixl_replication_config = {
    "source": "PIXL_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "pixl.{stream_table}",
    },

    "streams": {
        "public.admins": None,
        "public.audit_log": {
            "select": ["-ip"],
        },
        "public.ban_proposals": None,
        "public.bans": None,
        "public.blocks": None,
        "public.bounty_claims": None,
        "public.chat_messages": None,
        "public.collectible_purchases": None,
        "public.collectibles": None,
        "public.events": None,
        "public.form_configs": None,
        "public.form_submissions": {
            "select": ["-ip_hash"],
        },
        "public.friends": None,
        "public.fulfiller_order_alerts": None,
        "public.fulfillers": None,
        "public.fulfillment_commitments": None,
        "public.helpers": None,
        "public.idea_downvotes": None,
        "public.idea_upvotes": None,
        "public.ideas": None,
        "public.lobbies": {
            "select": ["-password"],  # Private lobby password
        },
        "public.member_joins": None,
        "public.mod_actions": None,
        "public.news": None,
        "public.notifications": None,
        "public.npc_state": None,
        "public.npcs": None,
        "public.operation_audit": None,
        "public.operation_entries": None,
        "public.operation_entry_contributors": None,
        "public.operations": None,
        "public.pixel_transactions": None,
        "public.player_state": None,
        "public.polls": None,
        "public.program_memory": None,
        "public.project_collaborators": None,
        "public.project_downvotes": None,
        "public.project_journals": None,
        "public.project_notes": None,
        "public.project_upvotes": None,
        "public.projects": {
            # Join and share links
            "select": [
                "-join_code", "-journal_share_token",
            ],
        },
        "public.referrals": None,
        "public.reports": None,
        "public.review_audits": None,
        "public.review_payout_settings": None,
        "public.review_payouts": None,
        "public.reviewer_guidelines_ack": None,
        "public.self_review_exceptions": None,
        "public.ships_snapshot_records": None,
        "public.ships_snapshots": None,
        "public.shop_claims": None,
        "public.shop_items": None,
        "public.shop_option_stock": None,
        "public.shop_orders": {
            "select": ["-tracking"],
        },
        "public.shop_saves": None,
        "public.show_n_tell_entries": None,
        "public.show_n_tell_rounds": None,
        "public.show_n_tell_votes": None,
        "public.sidequest_unlocks": None,
        "public.sidequests": None,
        "public.staff_settings": None,
        "public.story_nodes": None,
        "public.style_memory": None,
        "public.team_log": None,
        "public.tickets": None,
        "public.user_cosmetics": None,
        "public.user_memory": None,
        "public.user_personality": None,
        "public.users": {
            "select": [
                "-hackatime_token", "-address_line1", "-address_line2",
                "-address_postal", "-phone",
            ],
        },
        "public.vault_chapter_awards": None,
        "public.vault_levels": None,
        "public.village_invites": None,
        "public.village_upgrades": None,
        "public.violations": None,
    },
}


@dg.asset(
    name="pixl_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def pixl_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Pixl DB → warehouse in a single shot."""
    context.log.info("Starting Pixl → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=pixl_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
