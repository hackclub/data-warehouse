"""
Airtable Audit Logs Sync

Incrementally syncs audit log events from the Airtable Enterprise API
into the `airtable_audit_logs.events` PostgreSQL table.

Cursor strategy:
  - Reads the latest_event_timestamp from the previous materialization
  - Subtracts a 30-minute overlap window and fetches from there
  - Upserts by event_id so the overlap deduplicates naturally
"""

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import requests
from pydantic import PrivateAttr
from psycopg2 import sql
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import dagster as dg


SCHEMA_NAME = "airtable_audit_logs"
OVERLAP = timedelta(minutes=30)
PAGE_SIZE = 1000
PAGE_DELAY = 0.5
BATCH_SIZE = 1000


class AirtableEnterpriseResource(dg.ConfigurableResource):
    api_key: str = dg.EnvVar("AIRTABLE_ENTERPRISE_PAT")
    enterprise_account_id: str = dg.EnvVar("AIRTABLE_ENTERPRISE_ACCOUNT_ID")

    _session: Optional[requests.Session] = PrivateAttr(default=None)

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers["Authorization"] = f"Bearer {self.api_key}"
            retry = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
            self._session.mount("https://", HTTPAdapter(max_retries=retry))
        return self._session

    def _request_with_retry(self, url: str, params: dict, *, max_attempts: int = 30, log=None) -> dict:
        for attempt in range(max_attempts):
            resp = self._get_session().get(url, params=params)
            if resp.status_code == 429:
                retry_after = None
                try:
                    retry_after = int(resp.headers.get("Retry-After", ""))
                except (ValueError, TypeError):
                    pass
                wait = (retry_after if retry_after else min(30 * (2 ** attempt), 300)) + random.uniform(0, 5)
                if log:
                    log.warning(
                        f"Rate limited, waiting {wait:.1f}s (attempt {attempt + 1}/{max_attempts})"
                        f" — headers: {dict(resp.headers)}"
                    )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        raise requests.exceptions.HTTPError(f"Exhausted {max_attempts} retries on {url}")

    def fetch_events(self, *, start_time: Optional[str] = None, log=None):
        url = (
            f"https://api.airtable.com/v0/meta/enterpriseAccounts"
            f"/{self.enterprise_account_id}/auditLogEvents"
        )
        params: Dict[str, str] = {"pageSize": str(PAGE_SIZE), "sortOrder": "ascending"}
        if start_time:
            params["startTime"] = start_time

        page = 0
        while True:
            page += 1
            if log:
                log.info(f"Fetching audit events page {page}...")

            data = self._request_with_retry(url, params, log=log)
            events = data.get("events", [])

            if log:
                log.info(f"Page {page}: {len(events)} events")

            yield from events

            next_cursor = data.get("pagination", {}).get("next")
            if not next_cursor or not events:
                break

            params = {"pageSize": str(PAGE_SIZE), "next": next_cursor}
            time.sleep(PAGE_DELAY)


def _get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def _ensure_schema_and_table(conn):
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
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp
            ON {SCHEMA_NAME}.events (timestamp)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_audit_action
            ON {SCHEMA_NAME}.events (action)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_audit_workspace_id
            ON {SCHEMA_NAME}.events (workspace_id)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_audit_base_id
            ON {SCHEMA_NAME}.events (base_id)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_audit_actor_user_id
            ON {SCHEMA_NAME}.events (actor_user_id)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_audit_model_id
            ON {SCHEMA_NAME}.events (model_id)
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
    airtable_enterprise: AirtableEnterpriseResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)

    start_time = None
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
        _ensure_schema_and_table(conn)
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


defs = dg.Definitions(
    assets=[airtable_audit_log_events],
    resources={
        "airtable_enterprise": AirtableEnterpriseResource(
            api_key=dg.EnvVar("AIRTABLE_ENTERPRISE_PAT"),
            enterprise_account_id=dg.EnvVar("AIRTABLE_ENTERPRISE_ACCOUNT_ID"),
        ),
    },
)
