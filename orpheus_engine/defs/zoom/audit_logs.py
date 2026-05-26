# TODO: these assets need account owner role (role_id 0) to access /report/operationlogs
# and /report/activities. currently the S2S app acts as admin (role_id 1) which isn't
# enough. but....who cares, pick up a foot ball.

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import dagster as dg

from orpheus_engine.defs.shared.zoom import ZoomResource
from orpheus_engine.defs.zoom.db import SCHEMA_NAME, clean_json, ensure_table, get_db_connection, upsert_rows


BATCH_SIZE = 1000
OVERLAP = timedelta(days=1)
LOOKBACK = timedelta(days=180)

OPERATION_LOG_COLUMNS = [
    "log_id", "time", "operator", "category_type", "action",
    "operation_detail", "raw", "_synced_at",
]

ACTIVITY_COLUMNS = [
    "activity_id", "email", "time", "type", "ip_address",
    "client_type", "version", "raw", "_synced_at",
]


class ZoomAuditLogSyncConfig(dg.Config):
    start_date_override: Optional[str] = None


def _operation_log_id(entry: dict) -> str:
    key = f"{entry.get('time')}:{entry.get('operator')}:{entry.get('action')}:{entry.get('operation_detail', '')}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def _operation_log_row(entry: dict, run_ts: datetime) -> tuple:
    return (
        _operation_log_id(entry),
        entry.get("time"),
        entry.get("operator"),
        entry.get("category_type"),
        entry.get("action"),
        entry.get("operation_detail"),
        clean_json(entry),
        run_ts,
    )


def _activity_id(entry: dict) -> str:
    key = f"{entry.get('time')}:{entry.get('email')}:{entry.get('type')}:{entry.get('ip_address', '')}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def _activity_row(entry: dict, run_ts: datetime) -> tuple:
    return (
        _activity_id(entry),
        entry.get("email"),
        entry.get("time"),
        entry.get("type"),
        entry.get("ip_address"),
        entry.get("client_type"),
        entry.get("version"),
        clean_json(entry),
        run_ts,
    )


def _resolve_start(context: dg.AssetExecutionContext, config: ZoomAuditLogSyncConfig, log) -> datetime:
    if config.start_date_override:
        log.info(f"Using manual start_date override: {config.start_date_override}")
        return datetime.fromisoformat(config.start_date_override).replace(tzinfo=timezone.utc)

    last_event = context.instance.get_latest_materialization_event(context.asset_key)
    if last_event and last_event.asset_materialization:
        md = last_event.asset_materialization.metadata
        if "latest_timestamp" in md:
            hwm = md["latest_timestamp"].value
            start = datetime.fromisoformat(hwm) - OVERLAP
            log.info(f"Resuming from {hwm} (with {OVERLAP} overlap: {start.isoformat()})")
            return start

    start = datetime.now(timezone.utc) - LOOKBACK
    log.info(f"No previous materialization — fetching from {start.date()} (6-month lookback)")
    return start


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="Zoom admin operation (audit) logs",
)
def zoom_operation_logs(
    context: dg.AssetExecutionContext,
    config: ZoomAuditLogSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)
    end = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "operation_logs", """
            log_id          TEXT PRIMARY KEY,
            time            TIMESTAMPTZ,
            operator        TEXT,
            category_type   TEXT,
            action          TEXT,
            operation_detail TEXT,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_oplog_time ON {SCHEMA_NAME}.operation_logs (time)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_oplog_action ON {SCHEMA_NAME}.operation_logs (action)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_oplog_operator ON {SCHEMA_NAME}.operation_logs (operator)",
        ])
    finally:
        conn.close()

    batch: List[tuple] = []
    total = 0
    latest_ts: Optional[str] = None

    for entry in zoom.fetch_operation_logs(start=start, end=end, log=log):
        batch.append(_operation_log_row(entry, run_ts))
        ts = entry.get("time")
        if ts and (latest_ts is None or ts > latest_ts):
            latest_ts = ts

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "operation_logs", OPERATION_LOG_COLUMNS, batch,
                            "log_id", ["raw", "_synced_at"],
                            "(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            log.info(f"Upserted {total} operation logs so far...")
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "operation_logs", OPERATION_LOG_COLUMNS, batch,
                        "log_id", ["raw", "_synced_at"],
                        "(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} operation logs")

    metadata: Dict[str, Any] = {"rows_synced": dg.MetadataValue.int(total)}
    if latest_ts:
        metadata["latest_timestamp"] = dg.MetadataValue.text(latest_ts)
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="Zoom sign-in/sign-out activity logs",
)
def zoom_sign_in_activities(
    context: dg.AssetExecutionContext,
    config: ZoomAuditLogSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)
    end = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "sign_in_activities", """
            activity_id     TEXT PRIMARY KEY,
            email           TEXT,
            time            TIMESTAMPTZ,
            type            TEXT,
            ip_address      TEXT,
            client_type     TEXT,
            version         TEXT,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_signin_time ON {SCHEMA_NAME}.sign_in_activities (time)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_signin_email ON {SCHEMA_NAME}.sign_in_activities (email)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_signin_type ON {SCHEMA_NAME}.sign_in_activities (type)",
        ])
    finally:
        conn.close()

    batch: List[tuple] = []
    total = 0
    latest_ts: Optional[str] = None

    for entry in zoom.fetch_sign_in_activities(start=start, end=end, log=log):
        batch.append(_activity_row(entry, run_ts))
        ts = entry.get("time")
        if ts and (latest_ts is None or ts > latest_ts):
            latest_ts = ts

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "sign_in_activities", ACTIVITY_COLUMNS, batch,
                            "activity_id", ["raw", "_synced_at"],
                            "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            log.info(f"Upserted {total} sign-in activities so far...")
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "sign_in_activities", ACTIVITY_COLUMNS, batch,
                        "activity_id", ["raw", "_synced_at"],
                        "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} sign-in activities")

    metadata: Dict[str, Any] = {"rows_synced": dg.MetadataValue.int(total)}
    if latest_ts:
        metadata["latest_timestamp"] = dg.MetadataValue.text(latest_ts)
    return dg.MaterializeResult(metadata=metadata)
