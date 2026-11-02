import importlib
import re

import pytest

PROGRAMS = [
    "haven", "haven_booking", "atlantis", "pixl", "cubed", "mipmap", "contd",
    "coral", "grandma", "stern", "out_to_c", "scraps", "trmnt", "three_am",
    "canvas", "club_starter_grant", "dyad", "embassy", "jame_gam",
    "save_point", "scare",
]

# Tables that hold login state, bearer secrets, caches or one-off backups.
UNMIRRORED_TABLE = re.compile(
    r"(^|_)(sessions?|auth_tokens?|rate_limits?|cache|migrations)$"
    r"|_bak_|backup|verifications?$|snapshot_state$"
)

# Secret-holding tables whose names the pattern above does not catch. A secret
# stored as a row value (a key/value settings table) is invisible to the
# column checks below, so the whole table stays out.
UNMIRRORED_TABLES = {
    "canvas": ["drizzle.__drizzle_migrations"],
    "club_starter_grant": ["public.sessions", "public.knex_migrations", "public.knex_migrations_lock"],
    "dyad": ["public._sqlx_migrations", "public.wrong_friend_code"],
    "embassy": [
        "public.Account",
        "public.Session",
        "public.VerificationToken",
        "public.McpOAuthCode",
        "public.McpOAuthRefreshToken",
        "public.McpToken",
        "public._prisma_migrations",
    ],
    "jame_gam": [
        "public.Account",
        "public.McpAuthCode",
        "public.McpToken",
        "public.Session",
        "public.ReviewerReauth",
        "public.WebhookSecret",
        "public._prisma_migrations",
        "public.ariwmigrations",
        "public.VmTombstone",
        "ariw.job",
        "ariw.outboundSchedule",
    ],
    "save_point": [
        "public.admin_tokens",
        "public.ar_internal_metadata",
        "public.schema_migrations",
        "public.solid_cable_messages",
        "public.solid_cache_entries",
        "public.solid_queue_batch_executions",
        "public.solid_queue_batches",
        "public.solid_queue_blocked_executions",
        "public.solid_queue_claimed_executions",
        "public.solid_queue_failed_executions",
        "public.solid_queue_jobs",
        "public.solid_queue_pauses",
        "public.solid_queue_processes",
        "public.solid_queue_ready_executions",
        "public.solid_queue_recurring_executions",
        "public.solid_queue_recurring_tasks",
        "public.solid_queue_scheduled_executions",
        "public.solid_queue_semaphores",
        "insights.faq_reads",
        "insights.nps_responses",
        "insights.nudges",
        "insights.orders",
        "insights.projects",
        "insights.ships",
        "insights.site_days",
        "insights.streak_activities",
        "insights.users",
        "insights.visits",
    ],
    "scare": ["public.Session", "public.VerificationToken"],
    "contd": ["public.api_keys"],
    "coral": ["public.app_settings"],  # anthropic_auth_token row
    "stern": [
        "public.airtable_connections", "public.hcb_connections",
        "public.api_keys", "public.developer_keys", "public.platform_api_keys",
        "public.oauth_codes",
    ],
    "trmnt": ["public.sign_in_codes"],
}

# Secret or contact columns that must stay excluded wherever their table is
# mirrored, checked against each program's live schema when it was added.
EXCLUDED_COLUMNS = {
    "canvas": {
        "public.users": ["hca", "hackatime", "referral_code"],
        "public.shop_orders": ["address"],
        "public.bot_log": ["details"],
    },
    "club_starter_grant": {
        "public.users": [
            "access_token",
            "refresh_token",
            "address_line_1",
            "address_line_2",
            "postal_code",
        ],
    },
    "dyad": {
        "public.hacker": ["hackatime_token", "friend_code", "invite_token"],
    },
    "embassy": {
        "public.AuditLog": ["beforeValue", "afterValue", "ipAddress"],
        "public.OrderEvent": ["metadata"],
        "public.PassportOrder": ["recipientToken", "recipientTokenHash"],
        "public.PassportRecipient": [
            "addressLine1",
            "addressLine2",
            "postalCode",
            "emergencyContact",
            "city",
            "stateProvince",
            "country",
            "photoUrl",
        ],
        "public.PassportVersion": ["data"],
        "public.Project": ["inviteCode"],
        "public.Shipment": ["trackingNumber"],
        "public.User": [
            "hackatimeTokenEncrypted",
            "addressLine1",
            "addressLine2",
            "addressPostal",
            "firstName",
            "lastName",
            "addressCity",
            "addressState",
            "addressCountry",
        ],
        "public.YSWS": ["apiKeyDisplay", "apiKeyHash", "apiKeyPrefix"],
    },
    "jame_gam": {
        "public.ActivityEvent": ["meta"],
        "public.Draft": [
            "fieldValues",
            "checks",
            "adjustments",
            "fixChecks",
            "collaboratorDeflates",
            "collaboratorNotes",
        ],
        "public.Flag": ["matched"],
        "public.FlagRule": ["config"],
        "public.OutboundDelivery": ["url", "errorDetail", "payload"],
        "public.OutboundEndpoint": ["url", "secretEnc"],
        "public.Program": ["fraudApiKeyEnc", "fraudWebhookToken", "priorityReviewToken"],
        "public.Review": [
            "fieldValues",
            "checklist",
            "adjustments",
            "fixChecks",
            "collaboratorDeflates",
            "collaboratorNotes",
        ],
        "public.ReviewerVm": ["guacUrl", "rdpUri", "rdpPasswordEnc"],
        "public.Submission": ["programMeta", "aiCheckResult", "preReview"],
        "public.WebhookDelivery": ["errorDetail"],
    },
    "save_point": {
        "public.active_storage_blobs": ["key", "metadata"],
        "public.nudges": ["token"],
        "public.orders": ["address"],
        "public.projects": ["buddy_code"],
        "public.users": [
            "hackatime_access_token", "address", "phone_number", "slack_dm_error",
            "last_seen_path",
        ],
        "public.visits": ["path"],
    },
    "scare": {
        "public.Account": ["refresh_token", "access_token", "id_token", "session_state"],
        "public.HackatimeLink": ["accessToken"],
        "public.AdminAudit": ["detail"],
        "public.Order": ["details"],
        "public.Game": ["airtableError", "ariError", "reviewDetail"],
        "public.Screenshot": ["data"],
    },
    "haven": {
        "public.users": ["token", "hcaToken", "phoneNumber", "address"],
        "public.events": ["venueAddress", "latitude", "longitude"],
    },
    "haven_booking": {
        "public.hosts": ["refresh_token_enc"],
        "public.bookings": ["meet_url"],
    },
    "atlantis": {
        "public.auth_user": ["password"],
        "public.atlantis_site_lapseaccount": ["encrypted_token"],
        "public.atlantis_site_profile": ["encrypted_hca_token"],
        "public.atlantis_site_timelapse": ["token", "playback_url"],
        "public.atlantis_site_auditlog": ["ip_address"],
    },
    "pixl": {
        "public.users": [
            "hackatime_token", "address_line1", "address_line2",
            "address_postal", "phone",
        ],
        "public.lobbies": ["password"],
        "public.projects": ["join_code", "journal_share_token"],
        "public.audit_log": ["ip"],
        "public.form_submissions": ["ip_hash"],
        "public.shop_orders": ["tracking"],
    },
    "cubed": {},
    "mipmap": {
        "public.user": [
            "phone_number", "address_line_1", "address_line_2",
            "address_postal_code", "address_phone_number",
        ],
        "public.order": [
            "tracking_number", "address_line_1", "address_line_2",
            "address_postal_code", "address_phone_number",
        ],
        "public.snapshot": ["held_image"],
    },
    "contd": {
        "public.users": ["hackatime_token", "address"],
        "public.orders": ["address"],
        "public.beta_invite_links": ["token"],
        "public.airtable_exports": ["fields"],
    },
    "coral": {
        "public.users": [
            "hca_token", "address_line1", "address_line2", "postal_code",
            "phone_number", "shop_address",
        ],
        "public.orders": ["shipping_address", "tracking_link", "hcb_grant_link"],
        "public.ahoy_visits": [
            "ip", "latitude", "longitude", "visit_token", "visitor_token",
        ],
        "public.audit_events": ["ip_address", "metadata"],
        "public.airtable_queue_items": ["payload"],
        "public.versions": ["object"],
    },
    "grandma": {
        "public.auth_user": ["password"],
        "public.accounts_profile": ["hackatime_token", "identity"],
        "public.orders_order": ["nonce"],
        "public.submissions_aridelivery": ["body"],
    },
    "stern": {
        "public.users": [
            "email_enc", "birthday_enc", "address_line1_enc", "address_line2_enc",
            "address_city_enc", "address_state_enc", "address_country_enc",
            "address_zip_enc", "hackatime_token_enc", "hca_access_token_enc",
            "hca_refresh_token_enc", "lapse_token_enc",
        ],
        "public.apps": ["clientSecretHash"],
        "public.user_app_profiles": ["onboarding_answers"],
        "public.stern_suggestions": ["submitter_ip_hash"],
        "public.app_invites": ["code"],
        "public.spot_check_invites": ["code"],
    },
    "out_to_c": {
        "public.users": ["token", "hca_token"],
    },
    "scraps": {
        "public.users": [
            "access_token", "id_token", "refresh_token", "phone",
            "address_line1", "address_line2", "address_postal_code",
            "referral_code",
        ],
        "public.shop_orders": ["phone", "shipping_address", "tracking_number"],
        "public.admin_deleted_orders": [
            "deleted_payload", "phone", "shipping_address",
        ],
        "public.referrals": ["code"],
    },
    "trmnt": {
        "public.users": [
            "hack_club_access_token", "hack_club_refresh_token",
            "hackatime_access_token",
        ],
        "public.orders": ["tracking_number"],
    },
    "three_am": {
        "public.users": [
            "hackatime_token", "address_line1", "address_line2", "postcode",
        ],
        "public.orders": [
            "address_line1", "address_line2", "postcode", "tracking",
        ],
        "public.slack_order_digest_events": ["payload"],
    },
}


def _config(program):
    module = importlib.import_module(f"orpheus_engine.defs.sling.programs.{program}")
    return getattr(module, f"{program}_replication_config")


@pytest.mark.parametrize("program", PROGRAMS)
def test_streams_are_listed_explicitly(program):
    config = _config(program)
    assert config["source"] == f"{program.upper()}_DB"
    assert config["target"] == "WAREHOUSE_DB"
    assert config["defaults"]["mode"] == "full-refresh"
    assert config["streams"]
    assert config["defaults"]["object"] == f"{program}.{{stream_table}}"
    for stream, options in config["streams"].items():
        assert (options or {}).get("mode", "full-refresh") == "full-refresh"
        assert "*" not in stream
        table = stream.split(".", 1)[1]
        assert not UNMIRRORED_TABLE.search(table), stream
        assert stream not in UNMIRRORED_TABLES.get(program, []), stream
        # Exclusion lists only: a new column is reviewed by adding it here.
        for column in (options or {}).get("select", []):
            assert column.startswith("-"), (stream, column)


@pytest.mark.parametrize("program", PROGRAMS)
def test_secret_columns_are_excluded(program):
    streams = _config(program)["streams"]
    for stream, columns in EXCLUDED_COLUMNS[program].items():
        assert stream in streams, stream
        select = (streams[stream] or {}).get("select", [])
        for column in columns:
            assert f"-{column}" in select, (stream, column)
