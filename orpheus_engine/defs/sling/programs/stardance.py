from datetime import datetime, timezone

import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _drop_invalid_indexes, _ensure_incremental_target_indexes

STARDANCE_DAILY_FULL_REFRESH_UTC_HOUR = 3
STARDANCE_INDEX_LOCK_TIMEOUT_MS = 30_000
STARDANCE_OBSERVATION_SCAN_LIMIT = 25


stardance_db_connection = SlingConnectionResource(
    name="STARDANCE_DB",
    type="postgres",
    connection_string=EnvVar("STARDANCE_COOLIFY_URL"),
)


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
        # All tables: incremental on id + updated_at (inherits defaults).
        # Any new upstream table without id/updated_at fails this whole mirror
        # (2026-10: privacy_token_uses stalled it ~11 days, 2,068 step failures).
        # Fix: add an explicit override below -- {"update_key": "created_at"}
        # if it has created_at, {"mode": "full-refresh"} if small with no
        # timestamp, {"disabled": True} if sensitive/unused; set "primary_key"
        # too if it has no id.
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

        # --- Disabled: ActiveInsights APM telemetry ---
        # These tables are operational request/job traces rather than app data,
        # and nothing in this repository consumes the warehouse copies. On
        # 2026-08-15, copying the 55M-row requests table ran for nearly four
        # hours and held locks that backed up 112 warehouse sessions, including
        # readers of the unified YSWS NPS table. Keep the existing warehouse
        # data, but do not refresh either high-volume telemetry table.
        "public.active_insights_requests": {"disabled": True},
        "public.active_insights_jobs": {"disabled": True},

        # --- Disabled / Sensitive: JWT replay ledger ---
        # privacy_token_uses (jti, used_at) records used token ids. It is
        # security data, not analytics, and has no id/updated_at, so the
        # defaults fail on it. Nothing in this repo reads it.
        "public.privacy_token_uses": {"disabled": True},

        # --- Sensitive: Lookout session bearer token (secret_columns/inventory.py) ---
        "public.lookout_sessions": {"select": ["-token"]},

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
                "-access_token_bidx", "-access_token_ciphertext",
                "-refresh_token_bidx", "-refresh_token_ciphertext",
            ],
        },
        "public.rsvps": {
            "select": [
                "-confirmation_token",
            ],
        },
        "public.shop_orders": {
            "select": [
                "-frozen_address_ciphertext", "-fraud_payout_line_id",
                "-fraud_review_payout_id", "-country",
            ],
        },
        "public.shop_warehouse_packages": {
            "select": [
                "-frozen_address_ciphertext",
            ],
        },
        "public.hcb_credentials": {
            "select": [
                "-access_token_ciphertext", "-client_secret_ciphertext",
                "-refresh_token_ciphertext", "-expires_at",
            ],
        },
        "public.report_review_tokens": {
            "select": [
                "-token",
            ],
        },
    }
}



def _stardance_full_refresh_config() -> dict:
    """Build a full-refresh copy of stardance_replication_config preserving exclusions/disabled streams."""
    streams = {}
    for stream_name, cfg in stardance_replication_config["streams"].items():
        if isinstance(cfg, dict):
            if cfg.get("disabled"):
                streams[stream_name] = dict(cfg)
            else:
                cleaned = {k: v for k, v in cfg.items() if k not in ("mode", "update_key", "primary_key")}
                streams[stream_name] = cleaned or None
        else:
            streams[stream_name] = cfg
    return {
        "source": stardance_replication_config["source"],
        "target": stardance_replication_config["target"],
        "defaults": {
            "mode": "full-refresh",
            "object": "stardance.{stream_table}",
        },
        "streams": streams,
    }


def _last_full_refresh_date(context: dg.AssetExecutionContext) -> str | None:
    """Return the latest UTC date (YYYY-MM-DD) when a daily full refresh succeeded."""
    last_event = context.instance.get_latest_materialization_event(context.asset_key)
    if last_event and last_event.asset_materialization:
        md = last_event.asset_materialization.metadata
        if "last_full_refresh_date" in md:
            return str(md["last_full_refresh_date"].value)
    return None


def _last_full_refresh_attempt_date(context: dg.AssetExecutionContext) -> str | None:
    """Return the latest UTC date when a full-refresh attempt was started (AssetObservation)
    or completed (AssetMaterialization). Because AssetObservation persists immediately in
    event_logs even if the step raises or is killed by run monitoring, this acts as a
    once-per-day circuit breaker: a failed or killed full refresh falls back to incremental
    for the rest of the day instead of starving materialize_frequent_job.
    """
    dates: list[str] = []
    done_date = _last_full_refresh_date(context)
    if done_date:
        dates.append(done_date)

    obs_res = context.instance.fetch_observations(
        records_filter=context.asset_key,
        limit=STARDANCE_OBSERVATION_SCAN_LIMIT,
    )
    for rec in obs_res.records:
        obs = rec.asset_observation
        if obs and obs.metadata and "full_refresh_attempt_date" in obs.metadata:
            dates.append(str(obs.metadata["full_refresh_attempt_date"].value))
            break

    return max(dates) if dates else None


def _should_run_daily_full_refresh(now_utc: datetime, last_attempt_or_done_date: str | None) -> bool:
    return (
        now_utc.hour >= STARDANCE_DAILY_FULL_REFRESH_UTC_HOUR
        and last_attempt_or_done_date != now_utc.date().isoformat()
    )


@dg.asset(
    name="stardance_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stardance_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the main Stardance app DB → warehouse (15m incremental + once-daily full-refresh)."""
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date().isoformat()
    last_done = _last_full_refresh_date(context)
    last_attempt = _last_full_refresh_attempt_date(context)
    run_full_refresh = _should_run_daily_full_refresh(now_utc, last_attempt)

    if run_full_refresh:
        # Persist the attempt observation before starting Sling so a failure or
        # 60-minute max_runtime kill trips the circuit breaker for the rest of the day.
        context.log_event(
            dg.AssetObservation(
                asset_key=context.asset_key,
                metadata={
                    "full_refresh_attempt_date": today_utc,
                    "sync_mode": "full-refresh",
                },
            )
        )
        context.log.info("Starting Stardance → warehouse once-daily full-refresh Sling replication")
        config = _stardance_full_refresh_config()
    else:
        context.log.info("Starting Stardance → warehouse incremental Sling replication")
        _drop_invalid_indexes(context, "stardance")
        _ensure_incremental_target_indexes(
            context,
            stardance_replication_config,
            lock_timeout_ms=STARDANCE_INDEX_LOCK_TIMEOUT_MS,
        )
        config = stardance_replication_config

    for _ in sling.replicate(
        context=context,
        replication_config=config,
    ):
        pass

    context.log.info("Replication finished")
    metadata: dict[str, object] = {
        "replicated": True,
        "sync_mode": "full-refresh" if run_full_refresh else "incremental",
    }
    effective_done_date = today_utc if run_full_refresh else last_done
    if effective_done_date:
        metadata["last_full_refresh_date"] = effective_done_date
    context.add_output_metadata(metadata)
    return None
