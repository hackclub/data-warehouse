import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Attend moved from Coolify to Orchard in 2026-08. The env var keeps its old
# name; point it at the Orchard database (attend-db, namespace ysws-attend).
attend_db_connection = SlingConnectionResource(
    name="ATTEND_DB",
    type="postgres",
    connection_string=_sling_connection_url("ATTEND_COOLIFY_URL"),
)


attend_replication_config = {
    "source": "ATTEND_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "attend.{stream_table}",
    },

    "streams": {
        # --- Core: events, staff users, participants ---
        "public.events": {
            "select": [
                "id", "created_at", "docuseal_consent_template_id",
                "docuseal_participant_template_id", "docuseal_waiver_template_id",
                "ends_at", "location_address", "location_city", "location_country",
                "location_latitude", "location_longitude", "name",
                "registration_close_at", "registration_open_at", "slug", "starts_at",
                "support_email", "timezone", "updated_at",
                "docuseal_minor_waiver_template_id", "docuseal_adult_waiver_template_id",
                "docuseal_freedom_waiver_template_id", "slack_channel_id", "venue_name",
                "last_slack_sync_at", "hotel_scan_context_id", "docuseal_field_mappings",
                "airtable_sync_source_id", "airtable_sync_table_id", "airtable_synced_at",
                "docuseal_host",
            ],  # Excludes api_key_digest, luma_api_key_encrypted, config (free-form jsonb)
                # Dropped upstream (2026-08): luma_event_id -- Attend removed its
                # Luma integration columns; do not re-add without a schema check.
        },
        "public.users": {
            "select": [
                "id", "created_at", "current_sign_in_at", "email", "global_role",
                "hack_club_account_id", "last_sign_in_at", "name", "sign_in_count",
                "time_zone", "updated_at", "display_name", "theme", "phone_number",
                "slack_user_id",
            ],  # Excludes encrypted_password, reset_password_token/_sent_at,
                # remember_created_at, sign-in IPs, oidc_claims
        },
        "public.participants": None,
        "public.participant_events": {
            "select": [
                "id", "airtable_record_id",
                "code_of_conduct_accepted_at", "created_at", "event_id",
                "onboarding_step",
                "participant_id", "status", "updated_at", "onboarding_completed_at",
                "slack_user_id", "nfc_badge_assigned_at", "nfc_badge_assigned_by_id",
            ],  # Excludes nfc_badge_token (badge auth), onboarding_payload
                # (free-form answers), code_of_conduct_signature
                # Dropped upstream (2026-08): luma_guest_id, luma_sync_error --
                # Attend removed its Luma integration columns.
                # Dropped upstream: checked_in_at. Attend replaced the denormalized
                # check-in timestamp with the scans model -- derive check-in from
                # attend.scans joined to attend.scan_contexts where checks_in IS TRUE
                # (both mirrored in full below).
                # Not mirrored (deliberate, sensitive): um_status, um_verified_at,
                # um_guardian_confirmed_at, um_review_requested_at, um_verified_by_id
                # (underage-minor verification workflow). Add only if consciously needed.
        },
        "public.guardians": None,
        "public.guardian_participant_events": {
            "select": [
                "id", "accepted_at", "airtable_record_id", "completed_at",
                "created_at", "emergency_contact_priority",
                "emergency_medical_consent", "guardian_id", "invited_via_email",
                "is_primary_guardian", "media_permission", "otc_medication_consent",
                "participant_event_id", "participant_info_reviewed_at",
                "phone_override", "photo_permission", "relationship", "status",
                "travel_permission", "updated_at", "invite_token_sent_at",
            ],  # Excludes invite_token_digest, invite_token_ciphertext
        },
        "public.invitations": {
            "select": [
                "id", "accepted_at", "created_at", "email", "event_id",
                "expires_at", "updated_at", "group_ids",
            ],  # Excludes token
        },
        "public.consents": {
            "select": [
                "id", "consent_type", "created_at", "docuseal_envelope_id",
                "docuseal_template_id", "guardian_participant_event_id",
                "participant_event_id", "sent_at", "signed_at", "status",
                "updated_at", "guardian_signed_at", "participant_signed_at",
                "pending_on", "failure_reason", "docuseal_host",
            ],  # Excludes raw_metadata, document_url, docuseal_*_slug (signing links)
        },

        # --- Travel & rooming logistics ---
        "public.travels": {
            "select": [
                "id", "arrival_city", "arrival_station", "arrival_time",
                "bus_arrival_location", "bus_departure_location", "carrier",
                "created_at", "departure_city", "departure_station",
                "departure_time", "direction", "expected_arrival_time",
                "flight_number", "is_unaccompanied_minor", "mode", "notes",
                "origin_address", "other_details", "participant_event_id",
                "train_arrival_station", "train_departure_station", "updated_at",
                "visa_required", "visa_status", "visa_type", "pickup_dismissed_at",
            ],  # Excludes visa_number, passport_nationality
        },
        "public.travel_legs": {
            "select": [
                "id", "arrival_airport", "arrival_time", "created_at",
                "departure_airport", "departure_time", "flight_code", "position",
                "travel_id", "updated_at", "live_status", "live_departure_time",
                "live_arrival_time", "live_data", "last_tracked_at",
                "travel_picked_up_at", "picked_up_by_user_id",
                "oag_schedule_instance_key", "oag_flight_data",
            ],  # Excludes confirmation_code (PNR grants booking access)
                # Renamed upstream (2026-08-24): airport_picked_up_at ->
                # travel_picked_up_at.
        },
        "public.accommodations": {
            "select": [
                "id", "airtable_record_id", "assigned_room", "check_in_date",
                "check_out_date", "created_at", "participant_event_id",
                "quiet_room_preference", "room_type_preference", "updated_at",
                "venue_name", "roommate_links_reviewed", "rooming_exempt",
            ],  # Excludes gender_identity(_other), preferred_roommate_genders,
                # accessibility_needs, notes, free-text roommate prefs/exclusions
        },
        "public.rooms": None,
        "public.room_assignments": {
            "select": [
                "id", "room_id", "participant_event_id", "staff_override",
                "created_at", "updated_at",
            ],  # Excludes staff_override_notes, flags, trans_nb_acknowledged
        },
        "public.rooming_plans": None,
        "public.roommate_preferences": None,
        "public.roommate_exclusions": None,
        "public.sibling_groups": None,
        "public.sibling_memberships": None,
        "public.groups": None,
        "public.group_memberships": None,
        "public.event_role_assignments": None,

        # --- Attendance scans (the core analytics signal) ---
        "public.scans": None,
        "public.scan_contexts": None,

        # --- Comms & support ---
        "public.messages": None,
        "public.message_deliveries": None,
        "public.slack_blasts": None,
        "public.slack_blast_recipients": None,
        "public.tickets": None,
        "public.ticket_messages": {
            "select": [
                "id", "ticket_id", "direction", "channel", "user_id",
                "twilio_message_sid", "twilio_status", "sent_at", "created_at",
                "updated_at", "signal_message_sid",
            ],  # Excludes body, raw_payload, error_message (support convo content)
        },
        "public.email_logs": {
            "select": [
                "id", "to_address", "from_address", "subject", "mailer_class",
                "mailer_action", "postmark_message_id", "status", "delivered_at",
                "opened_at", "bounced_at", "bounce_type", "bounce_description",
                "emailable_type", "emailable_id", "event_id", "created_at",
                "updated_at",
            ],  # Excludes body
        },
        "public.email_log_events": None,

        # --- Moderation & misc ---
        "public.bans": None,
        "public.ban_emails": None,
        "public.import_batches": {
            "select": [
                "id", "event_id", "status", "total_count", "imported_count",
                "skipped_count", "error_count", "invites_sent_count",
                "send_invitations", "completed_at", "created_at", "updated_at",
            ],  # Excludes rows_data, errors_data (raw import dumps)
        },
        "public.passkit_passes": {
            "select": [
                "id", "created_at", "generator_type", "klass", "serial_number",
                "updated_at", "version", "generator_id",
            ],  # Excludes authentication_token, data
        },
        "public.passkit_registrations": None,
        "public.passkit_devices": {
            "select": [
                "id", "created_at", "identifier", "updated_at",
            ],  # Excludes push_token
        },
    },
}



@dg.asset(
    name="attend_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def attend_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the analytics-safe Attend DB tables/columns into the warehouse."""
    context.log.info("Starting Attend → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=attend_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
