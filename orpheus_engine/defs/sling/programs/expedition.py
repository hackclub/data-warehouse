import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Expedition (Orchard: expedition, namespace ysws-expedition; schema checked 2026-11-11).
expedition_db_connection = SlingConnectionResource(
    name="EXPEDITION_DB",
    type="postgres",
    connection_string=_sling_connection_url("EXPEDITION_K8S_URL"),
)


# Not mirrored: sessions (token hashes, IPs) and hackatime_connections (OAuth
# access and refresh tokens). files.bytes is raw upload content. ip_hash is a
# hashed IP; booking references and links can manage a flight booking.
# referral_config and ireland_signup_settings hold only numbers and flags.
expedition_replication_config = {
    "source": "EXPEDITION_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "expedition.{stream_table}",
    },
    "streams": {
        "public.admin_actions": None,
        "public.checkpoints": None,
        "public.daily_project_coding": None,
        "public.expedition_projects": None,
        "public.files": {"select": ["-bytes"]},
        "public.flight_quotes": None,
        "public.hackclub_submissions": None,
        "public.hour_transactions": None,
        "public.ireland_notify_emails": {"select": ["-ip_hash"]},
        "public.ireland_signup_settings": None,
        "public.ireland_signups": {"select": ["-ip_hash", "-attend_error"]},
        "public.mailing_signups": None,
        "public.outreach_access": None,
        "public.outreach_contacts": None,
        "public.qualification_claims": None,
        "public.referral_attributions": None,
        "public.referral_codes": None,
        "public.referral_config": None,
        "public.referral_events": None,
        "public.referral_reward_projects": None,
        "public.referral_rewards": None,
        "public.review_claims": None,
        "public.review_decisions": None,
        "public.review_flags": None,
        "public.review_payouts": None,
        "public.reward_claims": None,
        "public.submission_queue": {"select": ["-address_line1", "-address_line2", "-zip"]},
        "public.submission_reviews": None,
        "public.travel_claims": None,
        "public.travel_grant_requests": None,
        "public.travel_origins": None,
        "public.travel_payouts": None,
        "public.trip_flights": {"select": ["-booking_ref", "-booking_url"]},
        "public.users": {"select": ["-hca_address"]},
    },
}


@dg.asset(
    name="expedition_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def expedition_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Expedition DB → warehouse in a single shot."""
    context.log.info("Starting Expedition → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=expedition_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
