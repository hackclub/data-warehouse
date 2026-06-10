from dagster import EnvVar, AssetExecutionContext, Nothing
from dagster_sling import SlingResource, SlingConnectionResource
from typing import Mapping, Any
from urllib.parse import urlparse, parse_qs
import dagster as dg
import hashlib
import ipaddress
import os
import base64
import psycopg2
from psycopg2 import sql


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


# Validate all sling connection URLs - sslmode=disable only allowed for Tailscale IPs
_SLING_CONNECTION_URL_ENV_VARS = [
    "HACKATIME_COOLIFY_URL",
    "HCER_PUBLIC_GITHUB_DATA_COOLIFY_URL",
    "SHIPWRECKED_THE_BAY_COOLIFY_URL",
    "JOURNEY_COOLIFY_URL",
    "SUMMER_OF_MAKING_2025_COOLIFY_URL",
    "HACKATIME_LEGACY_COOLIFY_URL",
    "FLAVORTOWN_COOLIFY_URL",
    "FLAVORTOWN_AHOY_COOLIFY_URL",
    "STARDANCE_AHOY_COOLIFY_URL",
    "STARDANCE_COOLIFY_URL",
    "HACK_CLUB_THE_GAME_COOLIFY_URL",
    "BLUEPRINT_COOLIFY_URL",
    "STASIS_COOLIFY_URL",
    "FALLOUT_COOLIFY_URL",
    "HORIZONS_K8S_URL",
    "REVIEW_COOLIFY_URL",
    "JOE_COOLIFY_URL",
    "STACK_COOLIFY_URL",
    "OFFTRACK_COOLIFY_URL",
    "MACONDO_COOLIFY_URL",
    "BEEST_COOLIFY_URL",
    "WAREHOUSE_COOLIFY_URL",
]

for _env_var in _SLING_CONNECTION_URL_ENV_VARS:
    _validate_sslmode_disable_is_tailscale(_env_var)

# --- Define Connections ---

# 1. Source Connection (Hackatime Database)
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

shipwrecked_the_bay_db_connection = SlingConnectionResource(
    name="SHIPWRECKED_THE_BAY_DB",
    type="postgres",
    connection_string=EnvVar("SHIPWRECKED_THE_BAY_COOLIFY_URL"),
)

journey_db_connection = SlingConnectionResource(
    name="JOURNEY_DB",
    type="postgres",
    connection_string=EnvVar("JOURNEY_COOLIFY_URL"),
)

summer_of_making_2025_db_connection = SlingConnectionResource(
    name="SUMMER_OF_MAKING_2025_DB",
    type="postgres",
    connection_string=EnvVar("SUMMER_OF_MAKING_2025_COOLIFY_URL"),
)

hackatime_legacy_db_connection = SlingConnectionResource(
    name="HACKATIME_LEGACY_DB",
    type="postgres",
    connection_string=EnvVar("HACKATIME_LEGACY_COOLIFY_URL"),
)

flavortown_db_connection = SlingConnectionResource(
    name="FLAVORTOWN_DB",
    type="postgres",
    connection_string=EnvVar("FLAVORTOWN_COOLIFY_URL"),
)

flavortown_ahoy_db_connection = SlingConnectionResource(
    name="FLAVORTOWN_AHOY_DB",
    type="postgres",
    connection_string=EnvVar("FLAVORTOWN_AHOY_COOLIFY_URL"),
)

stardance_ahoy_db_connection = SlingConnectionResource(
    name="STARDANCE_AHOY_DB",
    type="postgres",
    connection_string=EnvVar("STARDANCE_AHOY_COOLIFY_URL"),
)

stardance_db_connection = SlingConnectionResource(
    name="STARDANCE_DB",
    type="postgres",
    connection_string=EnvVar("STARDANCE_COOLIFY_URL"),
)

hack_club_the_game_db_connection = SlingConnectionResource(
    name="HACK_CLUB_THE_GAME_DB",
    type="postgres",
    connection_string=EnvVar("HACK_CLUB_THE_GAME_COOLIFY_URL"),
)

blueprint_db_connection = SlingConnectionResource(
    name="BLUEPRINT_DB",
    type="postgres",
    connection_string=EnvVar("BLUEPRINT_COOLIFY_URL"),
)

stasis_db_connection = SlingConnectionResource(
    name="STASIS_DB",
    type="postgres",
    connection_string=EnvVar("STASIS_COOLIFY_URL"),
)

fallout_db_connection = SlingConnectionResource(
    name="FALLOUT_DB",
    type="postgres",
    connection_string=EnvVar("FALLOUT_COOLIFY_URL"),
)

horizons_db_connection = SlingConnectionResource(
    name="HORIZONS_DB",
    type="postgres",
    connection_string=EnvVar("HORIZONS_K8S_URL"),
)

stack_db_connection = SlingConnectionResource(
    name="STACK_DB",
    type="postgres",
    connection_string=EnvVar("STACK_COOLIFY_URL"),
)

offtrack_db_connection = SlingConnectionResource(
    name="OFFTRACK_DB",
    type="postgres",
    connection_string=EnvVar("OFFTRACK_COOLIFY_URL"),
)

macondo_db_connection = SlingConnectionResource(
    name="MACONDO_DB",
    type="postgres",
    connection_string=EnvVar("MACONDO_COOLIFY_URL"),
)

beest_db_connection = SlingConnectionResource(
    name="BEEST_DB",
    type="postgres",
    connection_string=EnvVar("BEEST_COOLIFY_URL"),
)

review_db_connection = SlingConnectionResource(
    name="REVIEW_DB",
    type="postgres",
    connection_string=EnvVar("REVIEW_COOLIFY_URL"),
)

joe_db_connection = SlingConnectionResource(
    name="JOE_DB",
    type="postgres",
    connection_string=EnvVar("JOE_COOLIFY_URL"),
)

# Auth DB connection - absolute minimum permissions to generate events for monthly
# active stats (e.g. "logged in at", "created oauth app"). No tokens or secrets.
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
    connection_string=EnvVar("WAREHOUSE_COOLIFY_URL"),
)

# --- Create Sling Resource ---
sling_replication_resource = SlingResource(
    connections=[
        hackatime_db_connection,
        hcer_public_github_data_connection,
        shipwrecked_the_bay_db_connection,
        journey_db_connection,
        summer_of_making_2025_db_connection,
        hackatime_legacy_db_connection,
        flavortown_db_connection,
        flavortown_ahoy_db_connection,
        stardance_ahoy_db_connection,
        stardance_db_connection,
        hack_club_the_game_db_connection,
        blueprint_db_connection,
        stasis_db_connection,
        fallout_db_connection,
        horizons_db_connection,
        stack_db_connection,
        offtrack_db_connection,
        macondo_db_connection,
        beest_db_connection,
        review_db_connection,
        joe_db_connection,
        auth_db_connection,
        hcb_db_connection,
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


def _safe_index_name(*parts: str) -> str:
    raw_name = "_".join(["idx", *parts])
    digest = hashlib.sha1(raw_name.encode("utf-8")).hexdigest()[:8]
    return f"{raw_name[:52]}_{digest}"


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


def _ensure_hackatime_target_indexes(context: AssetExecutionContext) -> None:
    """Keep Sling incremental cursor lookups and merges off full-table scans."""
    warehouse_url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not warehouse_url:
        raise ValueError("WAREHOUSE_COOLIFY_URL is required for Hackatime index preflight")

    index_specs: list[tuple[str, tuple[str, ...]]] = []
    for table_name, primary_key in {
        **_HACKATIME_UPDATED_AT_STREAMS,
        **_HACKATIME_CREATED_AT_STREAMS,
    }.items():
        update_key = "updated_at" if table_name in _HACKATIME_UPDATED_AT_STREAMS else "created_at"
        index_specs.append((table_name, tuple(primary_key)))
        index_specs.append((table_name, (update_key,)))

    conn = psycopg2.connect(warehouse_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS hackatime")
            for table_name, columns in index_specs:
                cursor.execute("SELECT to_regclass(%s)", (f"hackatime.{table_name}",))
                if cursor.fetchone()[0] is None:
                    continue

                index_name = _safe_index_name("hackatime", table_name, *columns)
                context.log.info(
                    "Ensuring Hackatime target index %s on %s(%s)",
                    index_name,
                    table_name,
                    ", ".join(columns),
                )
                cursor.execute(
                    sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}.{} ({})").format(
                        sql.Identifier(index_name),
                        sql.Identifier("hackatime"),
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    )
                )
    finally:
        conn.close()

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

# --- Journey Database Replication Configuration ---
journey_replication_config = {
    "source": "JOURNEY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "journey.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
    }
}

# --- Shipwrecked The Bay Database Replication Configuration ---
shipwrecked_the_bay_replication_config = {
    "source": "SHIPWRECKED_THE_BAY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "shipwrecked_the_bay.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
    }
}

# --- Summer of Making 2025 Database Replication Configuration ---
summer_of_making_2025_replication_config = {
    "source": "SUMMER_OF_MAKING_2025_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "summer_of_making_2025.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # Disabled: Rails infrastructure tables
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_cache_entries": {"disabled": True},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_pauses": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
        "public.solid_queue_scheduled_executions": {"disabled": True},
        "public.solid_queue_semaphores": {"disabled": True},
        # Large tables configured for incremental sync
        "public.active_insights_requests": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 87M rows
        },
        "public.active_insights_jobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 8.6M rows
        },
        "public.vote_changes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 290K rows
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",  # 274K rows
        },
        "public.hackatime_projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 192K rows
        },
        "public.view_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 161K rows
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 149K rows
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",  # 46K rows
        },
        "public.devlogs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 48K rows
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 39K rows
        },
        "public.activities": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",  # 30K rows
        },
    }
}

# --- Hackatime Legacy Database Replication Configuration ---
hackatime_legacy_replication_config = {
    "source": "HACKATIME_LEGACY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hackatime_legacy.{stream_table}",
    },

    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
        # Large tables configured for incremental sync
        "public.heartbeats": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",  # 52M rows - heartbeat timestamp
        },
    }
}

# --- FlavorTown Database Replication Configuration ---
flavortown_replication_config = {
    "source": "FLAVORTOWN_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "flavortown.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # Rails internal tables - disable
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},

        # Tables with id + updated_at - use incremental sync
        "public.action_mailbox_inbound_emails": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        # Append-only; updated_at is unindexed on the source, so key off the indexed bigint PK.
        "public.active_insights_jobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_insights_requests": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.blazer_checks": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboard_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboards": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_credentials": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ledger_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.post_devlogs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.post_ship_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.posts": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_ideas": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_memberships": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.rsvps": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_card_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_items": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_orders": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_hackatime_projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_identities": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # Append-only; key off the indexed bigint PK rather than an unindexed updated_at.
        "public.extension_usages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.post_git_commits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.funnel_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_storage_blobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_storage_attachments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.active_storage_variant_records": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        # versions.id is a UUID (not monotonic) -> key off created_at instead.
        "public.versions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # Everything else stays full-refresh (default): tables are small, and a full
        # reload preserves hard-deletes that incremental can't propagate.
    }
}

# --- Hack Club: The Game Database Replication Configuration ---
hack_club_the_game_replication_config = {
    "source": "HACK_CLUB_THE_GAME_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hack_club_the_game.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # Exclude sensitive/internal tables
        "public.one_time_passwords": {"disabled": True},
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.blazer_audits": {"disabled": True},
        "public.blazer_checks": {"disabled": True},
        "public.blazer_dashboard_queries": {"disabled": True},
        "public.blazer_dashboards": {"disabled": True},
        "public.blazer_queries": {"disabled": True},
        "public.versions": {"disabled": True},
        # Users: exclude encrypted auth tokens
        "public.users": {
            "select": [
                "id", "account_id", "avatar", "ban_type", "birthday",
                "email", "hackatime_id", "internal_notes", "is_banned",
                "last_active", "referrer_id", "slack_id", "username",
                "ysws_verified", "role", "deleted_at", "created_at",
                "updated_at", "referral_code", "verification_status",
                "address_street", "address_locality", "address_region",
                "address_postal", "address_country", "first_name", "last_name",
            ],
        },
    },
}

# --- FlavorTown Ahoy Database Replication Configuration ---
flavortown_ahoy_replication_config = {
    "source": "FLAVORTOWN_AHOY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "flavortown_ahoy.{stream_table}",
    },

    "streams": {
        # Insert-only; started_at/time are only non-leading columns of composite indexes,
        # so key off the indexed bigint PK instead.
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
    }
}

# --- Stardance Ahoy Database Replication Configuration ---
stardance_ahoy_replication_config = {
    "source": "STARDANCE_AHOY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "stardance_ahoy.{stream_table}",
    },

    "streams": {
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
    }
}

# --- Stardance Database Replication Configuration ---
# Main Stardance app DB (Rails). Almost every table is id + updated_at, so defaults
# are incremental; the wildcard mirrors the whole schema and per-table entries below
# only encode the exceptions (different update key, full-refresh, disabled, or a
# column allow-list that drops encrypted/token columns).
stardance_replication_config = {
    "source": "STARDANCE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "incremental",
        "primary_key": ["id"],
        "update_key": "updated_at",
        "object": "stardance.{stream_table}",
    },

    "streams": {
        # All tables: incremental on id + updated_at (inherits defaults)
        "public.*": None,

        # --- Incremental: id + created_at (no updated_at column) ---
        "public.active_storage_attachments": {"update_key": "created_at"},
        "public.active_storage_blobs": {"update_key": "created_at"},
        "public.blazer_audits": {"update_key": "created_at"},
        "public.versions": {"update_key": "created_at"},

        # --- Full-refresh: no timestamp column to drive incremental ---
        "public.active_storage_variant_records": {"mode": "full-refresh"},

        # --- Materialized view: the "public.*" wildcard only discovers base
        # tables (Postgres omits matviews from information_schema), so it must
        # be named explicitly. It has no id/updated_at and is rebuilt wholesale
        # by REFRESH, so full-refresh is the only workable mode. ---
        "public.materialized_all_signups": {"mode": "full-refresh"},

        # --- Disabled: Rails infrastructure (no id) ---
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},

        # --- Disabled: pg_stat_statements extension views (no update_key) ---
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},

        # --- Sensitive: explicit column allow-list (excludes ciphertext/bidx/token) ---
        "public.users": {
            "select": [
                "id", "banned", "banned_at", "banned_reason", "created_at",
                "display_name", "email", "enriched_ref", "first_name",
                "granted_roles", "has_gotten_free_stickers",
                "has_pending_achievements", "hcb_email", "internal_notes",
                "last_name", "manual_ysws_override", "ref", "regions",
                "shop_region", "slack_id", "synced_at", "things_dismissed",
                "updated_at", "verification_status", "vote_balance",
                "votes_count", "ysws_eligible", "bio",
                "mission_review_notifications", "age_attestation",
                "experience_level", "interests", "onboarded_at",
                "shop_tutorial_started_at", "shop_tutorial_completed_at",
                "verification_checked_at", "guest_email", "user_ref",
                "ip_address", "user_agent", "geocoded_lat", "geocoded_lon",
                "geocoded_country", "geocoded_subdivision",
                "approx_balance", "approx_total_earned",
            ],  # Excludes session_token
        },
        "public.user_identities": {
            "select": [
                "id", "created_at", "provider", "uid", "updated_at", "user_id",
            ],  # Excludes access_token_*/refresh_token_* (ciphertext + bidx)
        },
        "public.rsvps": {
            "select": [
                "id", "click_confirmed_at", "created_at", "email", "ip_address",
                "ref", "reply_confirmed_at", "signup_confirmation_sent_at",
                "synced_at", "updated_at", "user_agent", "geocoded_lat",
                "geocoded_lon", "geocoded_country", "geocoded_subdivision",
                "user_ref",
            ],  # Excludes confirmation_token
        },
        "public.shop_orders": {
            "select": [
                "id", "aasm_state", "assigned_to_user_id",
                "awaiting_periodical_fulfillment_at", "created_at",
                "external_ref", "fraud_related_project_id", "frozen_item_price",
                "fulfilled_at", "fulfilled_by", "fulfillment_cost",
                "fulfillment_payout_line_id", "internal_notes",
                "internal_rejection_reason", "joe_case_url", "on_hold_at",
                "parent_order_id", "quantity", "region", "rejected_at",
                "rejection_reason", "shop_card_grant_id", "shop_item_id",
                "tracking_number", "updated_at", "user_id",
                "warehouse_package_id", "frozen_modifiers_price",
            ],  # Excludes frozen_address_ciphertext
        },
        "public.shop_warehouse_packages": {
            "select": [
                "id", "created_at", "frozen_contents", "theseus_package_id",
                "updated_at", "user_id",
            ],  # Excludes frozen_address_ciphertext
        },
        "public.hcb_credentials": {
            "select": [
                "id", "base_url", "client_id", "created_at", "redirect_uri",
                "slug", "updated_at",
            ],  # Excludes access_token/client_secret/refresh_token ciphertext
        },
        "public.report_review_tokens": {
            "select": [
                "id", "action", "created_at", "expires_at", "report_id",
                "updated_at", "used_at",
            ],  # Excludes token
        },
    }
}

# --- Blueprint Database Replication Configuration ---
blueprint_replication_config = {
    "source": "BLUEPRINT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "blueprint.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updated_at ---
        "public.ai_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.airtable_syncs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.allowed_emails": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboard_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboards": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.build_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.design_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.disco_recommendations": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.email_tracks": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.follows": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.guild_signups": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.guilds": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_transactions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.journal_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.kudos": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.manual_ticket_adjustments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.packages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.privileged_session_expiries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_items": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_orders": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.stored_recommendations": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.task_lists": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "avatar", "slack_id", "username", "timezone_raw",
                "is_banned", "created_at", "updated_at", "email", "is_mcg",
                "github_username", "last_active", "github_installation_id",
                "referrer_id", "identity_vault_id", "ysws_verified",
                "internal_notes", "free_stickers_claimed", "ban_type",
                "birthday", "is_pro", "admin", "reviewer", "fulfiller",
                "idv_country", "shopkeeper", "last_impersonated_at",
                "last_impersonation_ended_at", "first_synced_to_airtable",
                "hcb_integration_enabled", "hcb_token_expires_at",
            ],  # Excludes identity_vault_access_token, hcb_access_token, hcb_refresh_token
        },

        # --- Incremental: id + special timestamp ---
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },

        # --- Incremental: append-only with created_at ---
        "public.versions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Incremental: append-only tables ---
        "public.active_storage_attachments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.active_storage_blobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.project_user_views": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "first_viewed_at",
        },
        "public.blazer_audits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Disabled: sensitive data ---
        "public.one_time_passwords": {"disabled": True},

        # --- Disabled: Rails infrastructure tables ---
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_cache_entries": {"disabled": True},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_pauses": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
        "public.solid_queue_scheduled_executions": {"disabled": True},
        "public.solid_queue_semaphores": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # active_storage_variant_records (no timestamp, 41K rows)
    }
}

# --- Fallout Database Replication Configuration ---
fallout_replication_config = {
    "source": "FALLOUT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "fallout.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updated_at ---
        "public.airtable_syncs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.journal_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.onboarding_responses": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.recordings": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "avatar", "created_at", "discarded_at", "display_name",
                "email", "hca_id", "is_adult", "is_banned", "onboarded",
                "roles", "slack_id", "timezone", "type", "updated_at",
                "verification_status",
            ],  # Excludes device_token, hca_token, lapse_token
        },

        # --- Incremental: id + special timestamp ---
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },
        "public.active_storage_attachments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.active_storage_blobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.versions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.lapse_timelapses": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.you_tube_videos": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # --- Incremental: remaining data tables ---
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.mail_interactions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.mail_messages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ships": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # --- Disabled: Rails infrastructure tables ---
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_pauses": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
        "public.solid_queue_scheduled_executions": {"disabled": True},
        "public.solid_queue_semaphores": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # active_storage_variant_records (no timestamp)
    }
}

# --- Stasis Database Replication Configuration ---
stasis_replication_config = {
    "source": "STASIS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "stasis.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updatedAt ---
        "public.account": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "id", "accountId", "providerId", "userId",
                "accessTokenExpiresAt", "refreshTokenExpiresAt",
                "scope", "createdAt", "updatedAt",
            ],  # Excludes accessToken, refreshToken, idToken, password
        },
        "public.event": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.reviewer_note": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.session": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "id", "expiresAt", "createdAt", "updatedAt",
                "ipAddress", "userAgent", "userId",
            ],  # Excludes token
        },
        "public.shop_item": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.temp_rsvp": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "id", "email", "utmSource", "referredBy", "firstName",
                "lastName", "finishedAccount", "syncedToAirtable",
                "createdAt", "updatedAt",
            ],  # Excludes ip
        },
        "public.user": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.verification": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },

        # --- Incremental: id + createdAt (append-only) ---
        "public.audit_log": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.bom_item": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.currency_transaction": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.hackatime_project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.kudos": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.project_review_action": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.project_submission": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.review_claim": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.session_media": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.session_timelapse": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.submission_review": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },
        "public.work_session": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "createdAt",
        },

        # --- Disabled: infrastructure / no timestamps ---
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
        "public._prisma_migrations": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # project_badge (no timestamp, 2.5K rows)
        # sidekick_assignment (no timestamp, 2.6K rows)
        # user_role (no timestamp, 54 rows)
    }
}

# --- Horizons Database Replication Configuration ---
horizons_replication_config = {
    "source": "HORIZONS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "horizons.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: updated_at / updatedAt ---
        "public.email_jobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
        },
        "public.gift_codes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.global_settings": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["project_id"],
            "update_key": "updated_at",
        },
        "public.shop_item_variants": {
            "mode": "incremental",
            "primary_key": ["variant_id"],
            "update_key": "updated_at",
        },
        "public.shop_items": {
            "mode": "incremental",
            "primary_key": ["item_id"],
            "update_key": "updated_at",
        },
        "public.sticker_tokens": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "id", "email", "rsvpNumber", "isUsed",
                "usedAt", "createdAt", "updatedAt",
            ],  # Excludes token
        },
        "public.submissions": {
            "mode": "incremental",
            "primary_key": ["submission_id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["user_id"],
            "update_key": "updated_at",
            "select": [
                "user_id", "email", "first_name", "last_name", "birthday",
                "role", "onboard_complete", "onboarded_at", "address_line_1",
                "address_line_2", "city", "state", "country", "zip_code",
                "airtable_rec_id", "hackatime_account", "created_at",
                "updated_at", "referral_code", "raffle_pos", "is_fraud",
                "is_sus", "slack_user_id", "hca_id", "verification_status",
            ],  # Excludes hackatime_access_token
        },

        # --- Incremental: append-only (created_at) ---
        "public.hackatime_link_otps": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
            "select": [
                "id", "user_id", "email", "expires_at",
                "is_used", "used_at", "created_at",
            ],  # Excludes otp_code
        },
        "public.submission_audit_logs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.transactions": {
            "mode": "incremental",
            "primary_key": ["transaction_id"],
            "update_key": "created_at",
        },
        "public.user_sessions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Disabled: infrastructure ---
        "public._prisma_migrations": {"disabled": True},
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # users_airtable (no id, no timestamps)
    }
}

# --- Stack Database Replication Configuration ---
# Small app DB; full-refresh. users allow-list excludes OAuth/Hackatime tokens.
stack_replication_config = {
    "source": "STACK_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "stack.{stream_table}",
    },
    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        "public.users": {
            "select": [
                "id", "hackclub_sub", "email", "name", "slug", "profile_image_url",
                "slack_id", "verification_status", "role", "token_type",
                "token_expires_at", "expires_in_seconds", "scope",
                "created_at", "updated_at", "password_set_at", "coins",
                "hackatime_token_expires_at", "hackatime_connected_at",
                "hackatime_total_hours", "bricks", "hackatime_github_username",
            ],  # Excludes access_token, refresh_token, raw_token, password_hash,
                # raw_profile, hackatime_access_token, hackatime_refresh_token
        },
    },
}

# --- Off-Track Database Replication Configuration ---
offtrack_replication_config = {
    "source": "OFFTRACK_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "offtrack.{stream_table}",
    },
    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        "public.users": {
            "select": [
                "id", "hackclub_sub", "email", "name", "slug", "profile_image_url",
                "slack_id", "verification_status", "role", "coins", "token_type",
                "token_expires_at", "expires_in_seconds", "scope",
                "created_at", "updated_at", "hackatime_token_expires_at",
                "hackatime_connected_at", "hackatime_total_hours",
                "hackatime_github_username",
            ],  # Excludes access_token, refresh_token, raw_token,
                # raw_profile, hackatime_access_token, hackatime_refresh_token
        },
    },
}

# --- Macondo Database Replication Configuration ---
macondo_replication_config = {
    "source": "MACONDO_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "macondo.{stream_table}",
    },
    "streams": {
        "public.*": None,
        # Sensitive tables: tokens / PII
        "public._prisma_migrations": {"disabled": True},
        "public.sessions": {"disabled": True},
        "public.pii_locker": {"disabled": True},
        "public.internal_oauth_connections": {"disabled": True},
        "public.users": {
            "select": [
                "id", "name", "email", "image", "created_at", "updated_at", "sub",
                "slack_id", "username", "hackatime_id", "hcb_email", "locale",
                "timezone", "roles", "is_temp", "onboarding_step", "completed_guides",
                "last_seen_at", "last_login_at", "streak_freezes_remaining",
                "last_hackatime_total_hours", "last_hackatime_synced_at", "github_id",
                "country", "region_override", "referral_code", "referred_by_user_id",
                "referred_at", "last_hackatime_total_seconds", "preferred_reminder_hour",
                "streak_slack_notifications", "last_active_date", "hca_verification_status",
                "hca_ysws_eligible", "auto_use_streak_freezes",
                "slack_macondo_auto_invited_at", "lifetime_fruits_earned",
                "hca_last_sync_at", "username_synced_at", "reminder_hours_before_day_end",
                "reminder_local_hours", "hackatime_timezone",
            ],  # Excludes github_token, hackatime_oauth_token, hca_refresh_token,
                # github access; onboarding_data dropped as free-form blob
        },
    },
}

# --- Beest Database Replication Configuration ---
beest_replication_config = {
    "source": "BEEST_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "beest.{stream_table}",
    },
    "streams": {
        "public.*": None,
        # Sensitive tables: session/credential tokens
        "public._prisma_migrations": {"disabled": True},
        "public.sessions": {"disabled": True},
        "public.hcb_credentials": {"disabled": True},
        "public.users": {
            "select": [
                "id", "hca_sub", "email", "name", "nickname", "slack_id",
                "created_at", "updated_at", "two_emails", "hackatime_user_id",
                "has_address", "has_birthdate", "pipes", "gender", "utm_source",
                "utm_medium", "utm_campaign", "referrer", "landing_path", "intent",
                "reviewer_user_note",
            ],  # Excludes hackatime_token, hca_access_token, hca_refresh_token
        },
    },
}

# --- HCB Database Replication Configuration ---
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

        # --- Card/Authorization Tables ---
        "public.stripe_cards": None,
        "public.stripe_cardholders": None,
        "public.stripe_authorizations": None,
        "public.card_grants": None,

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

# --- Single Assets per Database ---

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
    _ensure_hackatime_target_indexes(context)

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
    name="journey_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def journey_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Journey DB → warehouse in a single shot."""
    context.log.info("Starting Journey → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=journey_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="shipwrecked_the_bay_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def shipwrecked_the_bay_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Shipwrecked The Bay DB → warehouse in a single shot."""
    context.log.info("Starting Shipwrecked The Bay → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=shipwrecked_the_bay_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="summer_of_making_2025_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def summer_of_making_2025_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Summer of Making 2025 DB → warehouse in a single shot."""
    context.log.info("Starting Summer of Making 2025 → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=summer_of_making_2025_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hackatime_legacy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hackatime_legacy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hackatime Legacy DB → warehouse in a single shot."""
    context.log.info("Starting Hackatime Legacy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hackatime_legacy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="flavortown_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def flavortown_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire FlavorTown DB → warehouse in a single shot."""
    context.log.info("Starting FlavorTown → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=flavortown_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="hack_club_the_game_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hack_club_the_game_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hack Club: The Game DB → warehouse in a single shot."""
    context.log.info("Starting Hack Club: The Game → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hack_club_the_game_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="blueprint_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def blueprint_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Blueprint DB → warehouse in a single shot."""
    context.log.info("Starting Blueprint → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=blueprint_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="stasis_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stasis_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Stasis DB → warehouse in a single shot."""
    context.log.info("Starting Stasis → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=stasis_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="fallout_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def fallout_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Fallout DB → warehouse in a single shot."""
    context.log.info("Starting Fallout → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=fallout_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="horizons_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def horizons_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Horizons DB → warehouse in a single shot."""
    context.log.info("Starting Horizons → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=horizons_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="stack_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stack_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Stack DB → warehouse in a single shot."""
    context.log.info("Starting Stack → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=stack_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="offtrack_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def offtrack_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Off-Track DB → warehouse in a single shot."""
    context.log.info("Starting Off-Track → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=offtrack_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="macondo_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def macondo_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Macondo DB → warehouse in a single shot."""
    context.log.info("Starting Macondo → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=macondo_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="beest_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def beest_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Beest DB → warehouse in a single shot."""
    context.log.info("Starting Beest → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=beest_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="flavortown_ahoy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def flavortown_ahoy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates FlavorTown Ahoy analytics DB → warehouse with incremental sync."""
    context.log.info("Starting FlavorTown Ahoy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=flavortown_ahoy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="stardance_ahoy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stardance_ahoy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Stardance Ahoy analytics DB → warehouse with incremental sync."""
    context.log.info("Starting Stardance Ahoy → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=stardance_ahoy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

@dg.asset(
    name="stardance_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stardance_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the main Stardance app DB → warehouse with incremental sync."""
    context.log.info("Starting Stardance → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=stardance_replication_config,
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

    for _ in sling.replicate(
        context=context,
        replication_config=hcb_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

# --- Joe (Fraud Case Management) Database Replication Configuration ---
joe_replication_config = {
    "source": "JOE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "fraud_joe.{stream_table}",
    },

    "streams": {
        # --- Cases & case activity ---
        "public.cases": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.case_status_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.case_comments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.case_assignees": None,  # No update key, small table

        # --- Fraudpheus threads & messages ---
        "public.fraudpheus_messages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.fraudpheus_thread_status_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.fraudpheus_v2_threads": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.fraudpheus_v2_messages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Users & profiles ---
        "public.user": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.profiles": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.permissions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

    }
}

@dg.asset(
    name="joe_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def joe_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Joe (fraud case management) DB → warehouse."""
    context.log.info("Starting Joe → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=joe_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

# --- Review Database Replication Configuration ---
review_replication_config = {
    "source": "REVIEW_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "review.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # ysws_reviews has id + updated_at - use incremental sync
        "public.ysws_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
    }
}

@dg.asset(
    name="review_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def review_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Review DB → warehouse in a single shot."""
    context.log.info("Starting Review → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=review_replication_config,
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

    for _ in sling.replicate(
        context=context,
        replication_config=auth_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
