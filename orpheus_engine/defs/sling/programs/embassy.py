import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# embassy (Orchard).
embassy_db_connection = SlingConnectionResource(
    name="EMBASSY_DB",
    type="postgres",
    connection_string=_sling_connection_url("EMBASSY_K8S_URL"),
)


# embassy source catalog and exclusions checked live 2026-10-31.
# Omitted: Account, Session, VerificationToken, McpOAuthCode, McpOAuthRefreshToken, McpToken, _prisma_migrations.
# Auth/session/OAuth/MCP secrets and migrations stay out; no settings tables.
# Audit snapshots, order metadata and passport versions can embed addresses/tokens.
# Encrypted profile/address/photo fields are excluded, even city/country/name.
embassy_replication_config = {
    "source": "EMBASSY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "embassy.{stream_table}",
    },

    "streams": {
        "public.AriWebhookDelivery": None,
        "public.AuditLog": {"select": ["-beforeValue", "-afterValue", "-ipAddress"]},
        "public.CreditTransaction": None,
        "public.EmailDelivery": None,
        "public.HackatimeDay": None,
        "public.JournalEntry": None,
        "public.McpOAuthClient": None,
        "public.OrderBatch": None,
        "public.OrderBatchPassport": None,
        "public.OrderEvent": {"select": ["-metadata"]},
        "public.Org": None,
        "public.OrgMember": None,
        "public.OrganizerYSWSMembership": None,
        "public.PassportOrder": {"select": ["-recipientToken", "-recipientTokenHash"]},
        "public.PassportRecipient": {"select": ["-addressLine1", "-addressLine2", "-postalCode", "-emergencyContact", "-city", "-stateProvince", "-country", "-photoUrl"]},
        "public.PassportVersion": {"select": ["-data"]},
        "public.Project": {"select": ["-inviteCode"]},
        "public.ProjectHackatimeClaim": None,
        "public.ProjectMember": None,
        "public.RaffleDraw": None,
        "public.Shipment": {"select": ["-trackingNumber"]},
        "public.ShopItem": None,
        "public.ShopOrder": None,
        "public.SlackAiEvent": None,
        "public.SlackCampaign": None,
        "public.SlackCampaignDelivery": None,
        "public.SlackChannelInvite": None,
        "public.SlackChannelInviteIgnore": None,
        "public.SlackChannelPost": None,
        "public.SlackDirectDm": None,
        "public.SlackOutreach": None,
        "public.SlackOutreachRun": None,
        "public.StreakCelebration": None,
        "public.StreakReminder": None,
        "public.Submission": None,
        "public.User": {"select": ["-hackatimeTokenEncrypted", "-addressLine1", "-addressLine2", "-addressPostal", "-firstName", "-lastName", "-addressCity", "-addressState", "-addressCountry"]},
        "public.Vote": None,
        "public.WaitlistSignup": None,
        "public.YSWS": {"select": ["-apiKeyDisplay", "-apiKeyHash", "-apiKeyPrefix"]},
    },
}


@dg.asset(
    name="embassy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def embassy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the embassy DB → warehouse in a single shot."""
    context.log.info("Starting embassy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=embassy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
