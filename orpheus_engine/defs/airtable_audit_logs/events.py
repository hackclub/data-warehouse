import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

import dagster as dg

from orpheus_engine.defs.shared.airtable_enterprise import AirtableEnterpriseResource


SCHEMA_NAME = "airtable_audit_logs"
OVERLAP = timedelta(minutes=30)
BATCH_SIZE = 1000


class AuditLogSyncConfig(dg.Config):
    start_time_override: Optional[str] = None


def _get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def _ensure_events_table(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(SCHEMA_NAME)
        ))
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.events (
                event_id        TEXT PRIMARY KEY,
                timestamp       TIMESTAMPTZ NOT NULL,
                action          TEXT NOT NULL,
                actor_type      TEXT,
                actor_user_id   TEXT,
                actor_email     TEXT,
                actor_name      TEXT,
                model_id        TEXT,
                model_type      TEXT,
                workspace_id    TEXT,
                base_id         TEXT,
                ip_address      TEXT,
                user_agent      TEXT,
                session_id      TEXT,
                payload_version TEXT,
                payload         JSONB,
                context         JSONB,
                _synced_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for col in ("timestamp", "action", "workspace_id", "base_id", "actor_user_id", "model_id"):
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_audit_{col}
                ON {SCHEMA_NAME}.events ({col})
            """)
    conn.commit()


def _event_to_row(event: dict, run_ts: datetime) -> tuple:
    actor = event.get("actor") or {}
    actor_user = actor.get("user") or {}
    origin = event.get("origin") or {}
    ctx = event.get("context") or {}
    return (
        event.get("id"),
        event.get("timestamp"),
        event.get("action"),
        actor.get("type"),
        actor_user.get("id"),
        actor_user.get("email"),
        actor_user.get("name"),
        event.get("modelId"),
        event.get("modelType"),
        ctx.get("workspaceId"),
        ctx.get("baseId"),
        origin.get("ipAddress"),
        origin.get("userAgent"),
        origin.get("sessionId"),
        event.get("payloadVersion"),
        json.dumps(event.get("payload") or {}).replace("\\u0000", ""),
        json.dumps(ctx).replace("\\u0000", ""),
        run_ts,
    )


def _upsert_events(conn, rows: list):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.events
                (event_id, timestamp, action,
                 actor_type, actor_user_id, actor_email, actor_name,
                 model_id, model_type, workspace_id, base_id,
                 ip_address, user_agent, session_id,
                 payload_version, payload, context, _synced_at)
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                _synced_at = EXCLUDED._synced_at
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)",
            page_size=500,
        )
    conn.commit()


@dg.asset(
    compute_kind="airtable_api",
    group_name="airtable_audit_logs",
    description="Incrementally syncs Airtable Enterprise audit log events",
)
def airtable_audit_log_events(
    context: dg.AssetExecutionContext,
    config: AuditLogSyncConfig,
    airtable_enterprise: AirtableEnterpriseResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)

    start_time = None
    if config.start_time_override:
        start_time = config.start_time_override
        log.info(f"Using manual start_time override: {start_time}")
    else:
        last_event = context.instance.get_latest_materialization_event(
            context.asset_key
        )
        if last_event and last_event.asset_materialization:
            md = last_event.asset_materialization.metadata
            if "latest_event_timestamp" in md:
                hwm = md["latest_event_timestamp"].value
                overlap_start = datetime.fromisoformat(hwm) - OVERLAP
                start_time = overlap_start.isoformat()
                log.info(f"Resuming from {hwm} (with {OVERLAP} overlap: {start_time})")

    if not start_time:
        log.info("No previous materialization — fetching all available events (180-day retention)")

    conn = _get_db_connection()
    try:
        _ensure_events_table(conn)
    finally:
        conn.close()

    batch: List[tuple] = []
    total_events = 0
    latest_timestamp: Optional[str] = None

    for event in airtable_enterprise.fetch_events(start_time=start_time, log=log):
        batch.append(_event_to_row(event, run_ts))

        ts = event.get("timestamp")
        if ts and (latest_timestamp is None or ts > latest_timestamp):
            latest_timestamp = ts

        if len(batch) >= BATCH_SIZE:
            conn = _get_db_connection()
            try:
                _upsert_events(conn, batch)
            finally:
                conn.close()
            total_events += len(batch)
            log.info(f"Upserted {total_events} events so far...")
            batch = []

    if batch:
        conn = _get_db_connection()
        try:
            _upsert_events(conn, batch)
        finally:
            conn.close()
        total_events += len(batch)

    log.info(f"Sync complete: {total_events} events upserted")

    metadata: Dict[str, Any] = {
        "events_synced": dg.MetadataValue.int(total_events),
    }
    if latest_timestamp:
        metadata["latest_event_timestamp"] = dg.MetadataValue.text(latest_timestamp)
    if start_time:
        metadata["start_time"] = dg.MetadataValue.text(start_time)

    return dg.MaterializeResult(metadata=metadata)
