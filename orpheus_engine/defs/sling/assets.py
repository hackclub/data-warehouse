from dagster import EnvVar, Nothing
from dagster_sling import SlingResource, SlingConnectionResource
from typing import Any
from urllib.parse import urlparse, parse_qs
import dagster as dg
import ipaddress
import logging
import os
import base64
import psycopg2
from psycopg2 import sql
from ._helpers import (
    _drop_invalid_indexes,
    _ensure_target_indexes,
    _ensure_incremental_target_indexes,
    _sling_connection_url,
)

_logger = logging.getLogger(__name__)

def _validate_sslmode_disable_is_tailscale(env_var_name: str) -> None:
    """
    Validates that if a connection URL uses sslmode=disable, the host must be a
    Tailscale IP (100.64.0.0/10 CGNAT range). This prevents accidentally disabling
    SSL for public-facing databases.
    """
    url = os.getenv(env_var_name, "")
    if not url:
        return

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    # Check if sslmode=disable is set
    sslmode = query_params.get("sslmode", [None])[0]
    if sslmode != "disable":
        return

    # Validate host is a Tailscale IP (100.64.0.0/10)
    host = parsed.hostname
    if not host:
        return

    try:
        ip = ipaddress.ip_address(host)
        tailscale_range = ipaddress.ip_network("100.64.0.0/10")
        if ip not in tailscale_range:
            raise ValueError(
                f"{env_var_name}: sslmode=disable is only allowed for Tailscale IPs "
                f"(100.64.0.0/10). Got host: {host}"
            )
    except ValueError as e:
        if "does not appear to be an IPv4 or IPv6 address" in str(e):
            # Host is a hostname, not an IP - sslmode=disable not allowed
            raise ValueError(
                f"{env_var_name}: sslmode=disable is only allowed for Tailscale IPs "
                f"(100.64.0.0/10), not hostnames. Got host: {host}"
            )
        raise

# Validate special connection URLs at import time
# (Program connection URLs are validated by auto-discovery below)
_SLING_SPECIAL_ENV_VARS = [
    "HACKATIME_COOLIFY_URL",
    "HCER_PUBLIC_GITHUB_DATA_COOLIFY_URL",
    "WAREHOUSE_COOLIFY_URL",
]

for _env_var in _SLING_SPECIAL_ENV_VARS:
    _validate_sslmode_disable_is_tailscale(_env_var)


# --- Special Connection Resources ---
# (Program-specific connections are auto-discovered from programs/)

hackatime_db_connection = SlingConnectionResource(
    name="HACKATIME_DB",  # This name MUST match the 'source' key in replication_config
    type="postgres",
    connection_string=EnvVar("HACKATIME_COOLIFY_URL"),
)

hcer_public_github_data_connection = SlingConnectionResource(
    name="HCER_PUBLIC_GITHUB_DATA_DB",
    type="postgres",
    connection_string=EnvVar("HCER_PUBLIC_GITHUB_DATA_COOLIFY_URL"),
)


def _get_auth_ssh_private_key() -> str:
    """Decode base64-encoded SSH private key from env var."""
    key_b64 = os.getenv("AUTH_SSH_PRIVATE_KEY_B64", "")
    if not key_b64:
        return ""
    return base64.b64decode(key_b64).decode("utf-8")

auth_db_connection = SlingConnectionResource(
    name="AUTH_DB",
    type="postgres",
    host=EnvVar("AUTH_DB_HOST"),
    port=EnvVar("AUTH_DB_PORT"),
    database=EnvVar("AUTH_DB_DATABASE"),
    user=EnvVar("AUTH_DB_USER"),
    password=EnvVar("AUTH_DB_PASSWORD"),
    sslmode="disable",  # SSL not needed through SSH tunnel
    ssh_tunnel=EnvVar("AUTH_SSH_TUNNEL"),
    ssh_private_key=_get_auth_ssh_private_key(),
)


def _get_hcb_ssh_private_key() -> str:
    """Decode base64-encoded SSH private key from env var."""
    key_b64 = os.getenv("HCB_SSH_PRIVATE_KEY_B64", "")
    if not key_b64:
        return ""
    return base64.b64decode(key_b64).decode("utf-8")

hcb_db_connection = SlingConnectionResource(
    name="HCB_DB",
    type="postgres",
    host=EnvVar("HCB_DB_HOST"),
    port=EnvVar("HCB_DB_PORT"),
    database=EnvVar("HCB_DB_DATABASE"),
    user=EnvVar("HCB_DB_USER"),
    password=EnvVar("HCB_DB_PASSWORD"),
    ssh_tunnel=EnvVar("HCB_SSH_TUNNEL"),
    ssh_private_key=_get_hcb_ssh_private_key(),
)

# 2. Target Connection (Warehouse Database)
warehouse_db_connection = SlingConnectionResource(
    name="WAREHOUSE_DB",  # This name MUST match the 'target' key in replication_config
    type="postgres",
    connection_string=_sling_connection_url("WAREHOUSE_COOLIFY_URL"),
)



def _isolate_missing_program_connection(conn: SlingConnectionResource) -> SlingConnectionResource:
    """
    Keep one program's missing env var from breaking every job that uses Sling.

    All program connections share the single `sling` resource. Dagster resolves
    every EnvVar in that resource's config before a run starts, so one unset
    `<PROGRAM>_DATABASE_URL` makes run-config validation fail for EVERY job that
    includes any Sling asset (materialize_all_assets_job, materialize_frequent_job).
    That is what froze the whole warehouse from 2026-10-09 to 2026-10-13, when
    `WRONG_TOOL_DATABASE_URL` was added in code but not in the prod environment.

    When the env var is unset, swap in a placeholder URL on an `.invalid` host
    (RFC 2606: it never resolves). Only that program's mirror asset fails, with an
    error that names the missing variable; everything else keeps syncing.
    A module can instead skip on purpose while its reader is not provisioned
    yet, without failing or logging a materialization (see programs/haggle.py).
    """
    env_var_name = getattr(conn.connection_string, "env_var_name", None)
    if not env_var_name or os.getenv(env_var_name):
        return conn

    host = f"missing-env-{env_var_name.lower().replace('_', '-')}.invalid"
    _logger.warning(
        "Sling connection %s: env var %s is not set; its mirror asset will fail "
        "until it is configured. Other Sling assets are unaffected.",
        conn.name,
        env_var_name,
    )
    return SlingConnectionResource(
        name=conn.name,
        type=conn.type,
        connection_string=f"postgres://{host}:5432/unset",
    )


# --- Auto-discover program connections and assets from programs/ ---
import pkgutil as _pkgutil
import importlib as _importlib
from . import programs as _programs_pkg

_program_connections = []
_program_assets = []

for _, _mod_name, _ in _pkgutil.iter_modules(_programs_pkg.__path__):
    _mod = _importlib.import_module(f".programs.{_mod_name}", __package__)
    if getattr(_mod, "__disabled__", False):
        continue
    _conn = getattr(_mod, f"{_mod_name}_db_connection", None)
    if _conn is not None:
        _conn = _isolate_missing_program_connection(_conn)
        _program_connections.append(_conn)
        # Validate SSL configuration
        if hasattr(_conn, 'connection_string') and hasattr(_conn.connection_string, 'env_var_name'):
            _validate_sslmode_disable_is_tailscale(_conn.connection_string.env_var_name)
    _asset_fn = getattr(_mod, f"{_mod_name}_warehouse_mirror", None)
    if _asset_fn is not None:
        _program_assets.append(_asset_fn)

sling_replication_resource = SlingResource(
    connections=[
        hackatime_db_connection,
        hcer_public_github_data_connection,
        auth_db_connection,
        hcb_db_connection,
        *_program_connections,
        warehouse_db_connection,
    ]
)

_HACKATIME_UPDATED_AT_STREAMS = {
    "admin_api_keys": ["id"],
    "api_keys": ["id"],
    "commits": ["sha"],
    "dashboard_rollups": ["id"],
    "deletion_requests": ["id"],
    "email_addresses": ["id"],
    "email_verification_requests": ["id"],
    "flipper_features": ["id"],
    "flipper_gates": ["id"],
    "goals": ["id"],
    "good_job_batches": ["id"],
    "good_job_executions": ["id"],
    "good_job_processes": ["id"],
    "good_job_settings": ["id"],
    "good_jobs": ["id"],
    "heartbeat_import_runs": ["id"],
    "heartbeat_import_sources": ["id"],
    "heartbeat_user_agents": ["user_agent"],
    "heartbeats": ["id"],
    "instance_import_sources": ["id"],
    "leaderboard_entries": ["id"],
    "leaderboards": ["id"],
    "mailkick_subscriptions": ["id"],
    "oauth_applications": ["id"],
    "project_labels": ["id"],
    "project_repo_mappings": ["id"],
    "repo_host_events": ["id"],
    "repositories": ["id"],
    "sailors_log_leaderboards": ["id"],
    "sailors_log_notification_preferences": ["id"],
    "sailors_log_slack_notifications": ["id"],
    "sailors_logs": ["id"],
    "sign_in_tokens": ["id"],
    "trust_level_audit_logs": ["id"],
    "users": ["id"],
    "wakatime_mirrors": ["id"],
}

_HACKATIME_CREATED_AT_STREAMS = {
    "active_storage_attachments": ["id"],
    "active_storage_blobs": ["id"],
    "notable_jobs": ["id"],
    "notable_requests": ["id"],
    "oauth_access_grants": ["id"],
    "oauth_access_tokens": ["id"],
    "versions": ["id"],
}

_HACKATIME_FULL_REFRESH_STREAMS = [
    # These tables currently have no safe timestamp cursor in the source.
    "active_storage_variant_records",
    "pghero_query_stats",
    "pghero_space_stats",
]

# Hackatime's application indexes (hackclub/hackatime db/schema.rb @ 0af027e),
# mirrored onto the warehouse copy so ad-hoc queries (hackatime-mcp, analytics)
# get the same access paths as production - hackatime.heartbeats is tens of
# millions of rows and unusable without them. Deliberate differences from
# production, because the warehouse is an eventually-consistent mirror queried
# with arbitrary SQL:
#   - plain non-unique btrees only: uniqueness can't hold when soft-deletes
#     land late, and partial WHERE / INCLUDE / DESC / gin-trgm variants are
#     dropped - a plain btree on the same columns serves a superset of the
#     queries those serve
#   - prefix-redundant specs collapsed: (user_id, project) is served by
#     (user_id, project, time, id)
#   - PK / sync-cursor indexes already come from _replication_index_specs
#   - expression indexes and indexes touching array columns skipped
_HACKATIME_APP_INDEXES: dict[str, list[tuple[str, ...]]] = {
    "active_storage_attachments": [
        ("blob_id",),
        ("record_type", "record_id", "name", "blob_id"),
    ],
    "active_storage_blobs": [
        ("key",),
    ],
    "admin_api_keys": [
        ("token",),
        ("user_id", "name"),
    ],
    "api_keys": [
        ("token",),
        ("user_id", "name"),
        ("user_id", "token"),
    ],
    "commits": [
        ("repository_id",),
        ("user_id", "created_at"),
    ],
    "dashboard_rollups": [
        ("bucket_value",),
        ("dimension",),
        ("user_id", "dimension", "bucket_value_present", "bucket_value"),
    ],
    "deletion_requests": [
        ("status",),
        ("user_id", "status"),
    ],
    "email_addresses": [
        ("email",),
        ("user_id",),
    ],
    "email_verification_requests": [
        ("email",),
        ("user_id",),
    ],
    "flipper_features": [
        ("key",),
    ],
    "flipper_gates": [
        ("feature_key", "key", "value"),
    ],
    "goals": [
        ("user_id",),
    ],
    "good_job_executions": [
        ("active_job_id", "created_at"),
        ("process_id", "created_at"),
    ],
    "good_job_settings": [
        ("key",),
    ],
    "good_jobs": [
        ("active_job_id", "created_at"),
        ("batch_callback_id",),
        ("batch_id",),
        ("concurrency_key",),
        ("cron_key", "created_at"),
        ("cron_key", "cron_at"),
        ("finished_at",),
        ("locked_by_id",),
        ("priority", "created_at"),
        ("priority", "scheduled_at"),
        ("queue_name", "scheduled_at"),
        ("scheduled_at",),
    ],
    "heartbeat_import_runs": [
        ("user_id", "created_at"),
        ("user_id", "state"),
    ],
    "heartbeat_import_sources": [
        ("user_id",),
    ],
    "heartbeats": [
        ("category", "time"),
        ("fields_hash",),
        ("ip_address",),
        ("ja4_id",),
        ("machine",),
        ("project", "time"),
        ("source_type", "time", "user_id", "project"),
        ("time", "source_type"),
        ("time", "user_id"),
        ("user_agent",),
        ("user_id", "category", "time"),
        ("user_id", "editor", "time"),
        ("user_id", "id"),
        ("user_id", "language", "time", "id"),
        ("user_id", "operating_system", "time"),
        ("user_id", "project", "time", "id"),
        ("user_id", "source_type", "id"),
        ("user_id", "time", "category"),
        ("user_id", "time", "id"),
        ("user_id", "time", "language"),
        ("user_id", "time", "project"),
    ],
    "instance_import_sources": [
        ("user_id",),
    ],
    "leaderboard_entries": [
        ("leaderboard_id", "user_id"),
    ],
    "leaderboards": [
        ("start_date", "period_type", "timezone_utc_offset"),
    ],
    "mailkick_subscriptions": [
        ("subscriber_type", "subscriber_id", "list"),
    ],
    "notable_requests": [
        ("user_type", "user_id"),
    ],
    "oauth_access_grants": [
        ("application_id",),
        ("resource_owner_id",),
        ("token",),
    ],
    "oauth_access_tokens": [
        ("application_id",),
        ("refresh_token",),
        ("resource_owner_id",),
        ("token",),
    ],
    "oauth_applications": [
        ("owner_type", "owner_id"),
        ("uid",),
    ],
    "project_labels": [
        ("user_id", "project_key"),
    ],
    "project_repo_mappings": [
        ("project_name",),
        ("repository_id",),
        ("user_id", "archived_at"),
        ("user_id", "project_name"),
    ],
    "repo_host_events": [
        ("provider",),
        ("user_id", "provider", "created_at"),
    ],
    "repositories": [
        ("url",),
    ],
    "sailors_log_notification_preferences": [
        ("slack_uid", "slack_channel_id"),
    ],
    "sailors_logs": [
        ("slack_uid",),
    ],
    "sign_in_tokens": [
        ("token",),
        ("user_id",),
    ],
    "trust_level_audit_logs": [
        ("changed_by_id", "created_at"),
        ("user_id", "created_at"),
    ],
    "users": [
        # prod pairs this with github_access_token for token-auth lookups;
        # no warehouse query needs a secrets column as an index key
        ("github_uid",),
        ("hca_id",),
        ("leaderboard_shadowbanned",),
        ("leaderboard_shadowbanned_by_id",),
        ("slack_uid",),
        ("timezone", "trust_level"),
        ("username",),
    ],
    "versions": [
        ("item_type", "item_id"),
    ],
    "wakatime_mirrors": [
        ("user_id", "endpoint_url"),
    ],
}

def _hackatime_app_index_specs() -> list[tuple[str, str, tuple[str, ...]]]:
    """Target index specs for Hackatime's mirrored application indexes."""
    return [
        ("hackatime", table_name, columns)
        for table_name, specs in _HACKATIME_APP_INDEXES.items()
        for columns in specs
    ]



def _incremental_stream(primary_key: list[str], update_key: str) -> dict[str, Any]:
    return {
        "mode": "incremental",
        "primary_key": primary_key,
        "update_key": update_key,
    }


def _hackatime_streams() -> dict[str, Any]:
    streams: dict[str, Any] = {
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_cache_entries": {"disabled": True},
    }

    for table_name, primary_key in _HACKATIME_UPDATED_AT_STREAMS.items():
        streams[f"public.{table_name}"] = _incremental_stream(primary_key, "updated_at")

    for table_name, primary_key in _HACKATIME_CREATED_AT_STREAMS.items():
        streams[f"public.{table_name}"] = _incremental_stream(primary_key, "created_at")

    for table_name in _HACKATIME_FULL_REFRESH_STREAMS:
        streams[f"public.{table_name}"] = {"mode": "full-refresh"}

    # Dropped upstream: raw_heartbeat_uploads, ahoy_events, ahoy_visits.
    # Listing missing streams causes Sling to fail.
    return streams


# --- Define Replication Configuration ---
hackatime_replication_config = {
    "source": "HACKATIME_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "object": "hackatime.{stream_table}",
    },

    "streams": _hackatime_streams(),
}


# --- Define Replication Configuration ---
hcer_public_github_data_replication_config = {
    "source": "HCER_PUBLIC_GITHUB_DATA_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hcer_public_github_data.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
    }
}

# For calculating monthly actives and transaction ledger
hcb_replication_config = {
    "source": "HCB_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "incremental",
        "primary_key": ["id"],
        "update_key": "updated_at",
        "object": "hcb.{stream_table}",
    },

    "streams": {
        # --- Users & Activity ---
        "public.users": None,
        "public.user_seen_at_histories": None,
        "public.organizer_positions": None,

        # --- Core Financial Tables ---
        "public.events": None,
        "public.event_plans": None,
        "public.canonical_transactions": None,
        "public.canonical_event_mappings": None,
        "public.canonical_pending_transactions": None,
        "public.canonical_pending_event_mappings": None,
        "public.canonical_pending_settled_mappings": None,
        "public.canonical_pending_declined_mappings": None,
        "public.hcb_codes": None,
        "public.fees": None,

        # --- Payment/Vendor Tables ---
        "public.disbursements": None,
        "public.ach_transfers": None,
        "public.donations": None,
        "public.wires": None,
        "public.checks": None,
        "public.increase_checks": None,
        "public.wise_transfers": {
            "select": [
                "id", "aasm_state", "amount_cents", "approved_at", "sent_at",
                "currency", "payment_for", "recipient_country",
                "recipient_email", "recipient_name", "return_reason",
                "quoted_usd_amount_cents", "usd_amount_cents", "event_id",
                "user_id", "created_at", "updated_at",
            ],  # Excludes recipient bank/address/phone and *_ciphertext PII
        },
        "public.paypal_transfers": None,

        # --- Reimbursements ---
        # The chain behind every HCB-710 expense-payout ledger row:
        # expense_payouts -> expenses (the human-readable memo) -> reports
        # (title + who gets reimbursed). payout_holdings links a report's
        # HCB-712 holding to the transfer that actually paid the person.
        "public.reimbursement_reports": {
            "select": [
                "id", "user_id", "event_id", "invited_by_id", "name",
                "maximum_amount_cents", "aasm_state", "submitted_at",
                "reimbursement_requested_at", "reimbursement_approved_at",
                "rejected_at", "reimbursed_at", "created_at", "updated_at",
                "deleted_at", "reviewer_id", "conversion_rate", "currency",
                "card_grant_id",
            ],  # Excludes invite_message free text
        },
        "public.reimbursement_expenses": {
            "select": [
                "id", "reimbursement_report_id", "approved_by_id", "memo",
                "amount_cents", "description", "aasm_state", "approved_at",
                "created_at", "updated_at", "expense_number", "deleted_at",
                "type", "value", "category",
            ],
        },
        "public.reimbursement_expense_payouts": None,
        "public.reimbursement_payout_holdings": None,

        # --- Card/Authorization Tables ---
        "public.stripe_cards": None,
        "public.stripe_cardholders": None,
        "public.stripe_authorizations": None,  # Frozen upstream since 2023-09; kept for history
        "public.card_grants": None,
        # Modern card-transaction detail (merchant, card, cardholder, auth
        # method) lives in the stripe_transaction jsonb of these two tables.
        # NOTE: the replication role has a 30s source-side statement_timeout;
        # incremental catch-ups fit easily, but a from-scratch reload of
        # raw_pending_stripe_transactions (~1 GB) needs the session override
        # `options=-c statement_timeout=0` on the source connection.
        "public.raw_stripe_transactions": None,
        "public.raw_pending_stripe_transactions": None,

        # --- Tags/Metadata ---
        "public.tags": None,
        "public.event_tags": None,
        "public.hcb_codes_tags": {
            "primary_key": ["hcb_code_id", "tag_id"],  # Join table, no id column
        },

        # --- Receipts ---
        "public.receipts": {
            "select": [
                "id", "user_id", "receiptable_type", "receiptable_id",
                "upload_method", "suggested_memo", "data_extracted",
                "extracted_subtotal_amount_cents", "extracted_total_amount_cents",
                "extracted_date", "extracted_merchant_name", "extracted_merchant_url",
                "extracted_merchant_zip_code", "extracted_currency",
                "textual_content_source", "created_at", "updated_at"
            ],  # Excludes *_ciphertext and *_bidx columns
        },
    }
}

# --- Auth Database Replication Configuration ---
# Absolute minimum permissions - only columns needed to generate events for monthly
# active stats (e.g. "logged in at"). SELECT * is blocked, explicit columns only.
auth_replication_config = {
    "source": "AUTH_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "incremental",
        "primary_key": ["id"],
    },

    "streams": {
        "public.activities": {
            "object": "auth.activities",
            "select": ["id", "owner_id", "owner_type", "key", "trackable_type", "trackable_id", "parameters", "created_at", "updated_at"],
            "update_key": "updated_at",
        },
        "public.identities": {
            "object": "auth.identities",
            "select": ["id", "primary_email", "updated_at"],
            "update_key": "updated_at",
        },
        "public.oauth_access_tokens": {
            "object": "auth.oauth_access_tokens",
            "select": ["id", "application_id", "resource_owner_id", "created_at"],
            "update_key": "created_at",
        },
        "public.oauth_applications": {
            "object": "auth.oauth_applications",
            "select": ["id", "name", "trust_level", "updated_at"],
            "update_key": "updated_at",
        },
    }
}


# --- Special Asset Functions ---
# (Program-specific assets are auto-discovered from programs/)

@dg.asset(
    name="hackatime_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, hackatime_replication_config)

    # Iterate through the generator **without yielding** its events.
    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    # Optionally attach run‑level metadata
    context.add_output_metadata({"replicated": True})
    return None



@dg.asset(
    name="hackatime_warehouse_app_indexes",
    group_name="sling",
    compute_kind="postgres",
    deps=[dg.AssetKey("hackatime_warehouse_mirror")],
)
def hackatime_warehouse_app_indexes(
    context: dg.AssetExecutionContext,
) -> None:
    """Creates Hackatime's application indexes on the warehouse copy.

    Runs as a downstream dependency of hackatime_warehouse_mirror so that
    index creation never blocks the Sling sync. If this asset fails or times
    out, heartbeats keep flowing — queries are just slower until indexes land.
    """
    _drop_invalid_indexes(context, "hackatime")
    _ensure_target_indexes(context, _hackatime_app_index_specs())



@dg.asset(
    name="hcer_public_github_data_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hcer_public_github_data_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire HCER Public GitHub Data DB → warehouse in a single shot."""
    context.log.info("Starting HCER Public GitHub Data → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hcer_public_github_data_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None


@dg.asset(
    name="hcb_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hcb_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates HCB users and user_seen_at_histories → warehouse via SSH tunnel."""
    context.log.info("Starting HCB → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, hcb_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=hcb_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None


@dg.asset(
    name="auth_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def auth_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Auth DB tables → warehouse with explicit column selection."""
    context.log.info("Starting Auth → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, auth_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=auth_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
