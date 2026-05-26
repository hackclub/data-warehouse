import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import dagster as dg

from orpheus_engine.defs.shared.zoom import ZoomResource
from orpheus_engine.defs.zoom.db import SCHEMA_NAME, clean_json, ensure_table, get_db_connection, upsert_rows


BATCH_SIZE = 500
OVERLAP = timedelta(days=1)
LOOKBACK = timedelta(days=180)


class ZoomRecordingSyncConfig(dg.Config):
    start_date_override: Optional[str] = None


def _resolve_start(context: dg.AssetExecutionContext, config: ZoomRecordingSyncConfig, log) -> datetime:
    if config.start_date_override:
        log.info(f"Using manual start_date override: {config.start_date_override}")
        return datetime.fromisoformat(config.start_date_override).replace(tzinfo=timezone.utc)

    last_event = context.instance.get_latest_materialization_event(context.asset_key)
    if last_event and last_event.asset_materialization:
        md = last_event.asset_materialization.metadata
        if "latest_recording_start" in md:
            hwm = md["latest_recording_start"].value
            start = datetime.fromisoformat(hwm) - OVERLAP
            log.info(f"Resuming from {hwm} (with {OVERLAP} overlap: {start.isoformat()})")
            return start

    start = datetime.now(timezone.utc) - LOOKBACK
    log.info(f"No previous materialization — fetching from {start.date()}")
    return start


# ── recordings ──

RECORDING_COLUMNS = [
    "recording_id", "meeting_uuid", "meeting_id", "host_id", "host_email",
    "topic", "type", "start_time", "duration", "total_size",
    "recording_count", "recording_files", "raw", "_synced_at",
]


def _recording_row(m: dict, run_ts: datetime) -> tuple:
    return (
        m.get("uuid"),
        m.get("uuid"),
        m.get("id"),
        m.get("host_id"),
        m.get("host_email"),
        m.get("topic"),
        m.get("type"),
        m.get("start_time"),
        m.get("duration"),
        m.get("total_size"),
        m.get("recording_count"),
        clean_json(m.get("recording_files")),
        clean_json(m),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="Zoom cloud recording metadata (per meeting, includes file list)",
)
def zoom_recordings(
    context: dg.AssetExecutionContext,
    config: ZoomRecordingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)
    end = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "recordings", """
            recording_id    TEXT PRIMARY KEY,
            meeting_uuid    TEXT,
            meeting_id      BIGINT,
            host_id         TEXT,
            host_email      TEXT,
            topic           TEXT,
            type            INTEGER,
            start_time      TIMESTAMPTZ,
            duration        INTEGER,
            total_size      BIGINT,
            recording_count INTEGER,
            recording_files JSONB,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_rec_start ON {SCHEMA_NAME}.recordings (start_time)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_rec_host ON {SCHEMA_NAME}.recordings (host_id)",
        ])
    finally:
        conn.close()

    user_ids = [u["id"] for u in zoom.fetch_users(log=log)]
    log.info(f"Fetching recordings for {len(user_ids)} users from {start.date()} to {end.date()}")

    seen: dict[str, tuple] = {}
    total = 0
    latest_start: Optional[str] = None
    errors = 0

    for i, uid in enumerate(user_ids):
        try:
            for meeting in zoom.fetch_user_recordings(user_id=uid, start=start, end=end, log=log):
                row = _recording_row(meeting, run_ts)
                seen[row[0]] = row
                ms = meeting.get("start_time")
                if ms and (latest_start is None or ms > latest_start):
                    latest_start = ms

                if len(seen) >= BATCH_SIZE:
                    batch = list(seen.values())
                    seen.clear()
                    conn = get_db_connection()
                    try:
                        upsert_rows(conn, "recordings", RECORDING_COLUMNS, batch,
                                    "recording_id", [c for c in RECORDING_COLUMNS if c != "recording_id"],
                                    "(" + ", ".join(["%s"] * 11 + ["%s::jsonb", "%s::jsonb", "%s"]) + ")")
                    finally:
                        conn.close()
                    total += len(batch)
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch recordings for user {uid}: {e}")

        if (i + 1) % 20 == 0:
            log.info(f"Processed {i + 1}/{len(user_ids)} users, {total + len(seen)} recordings")

    if seen:
        batch = list(seen.values())
        conn = get_db_connection()
        try:
            upsert_rows(conn, "recordings", RECORDING_COLUMNS, batch,
                        "recording_id", [c for c in RECORDING_COLUMNS if c != "recording_id"],
                        "(" + ", ".join(["%s"] * 11 + ["%s::jsonb", "%s::jsonb", "%s"]) + ")")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} recordings ({errors} errors)")

    metadata: Dict[str, Any] = {
        "recordings_synced": dg.MetadataValue.int(total),
        "errors": dg.MetadataValue.int(errors),
    }
    if latest_start:
        metadata["latest_recording_start"] = dg.MetadataValue.text(latest_start)
    return dg.MaterializeResult(metadata=metadata)


# ── recording analytics ──

ANALYTICS_COLUMNS = [
    "analytics_id", "meeting_uuid", "date_time", "name", "email",
    "duration", "type", "raw", "_synced_at",
]


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_recordings],
    description="Zoom recording view/download analytics (who watched what, when)",
)
def zoom_recording_analytics(
    context: dg.AssetExecutionContext,
    config: ZoomRecordingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "recording_analytics", """
            analytics_id    TEXT PRIMARY KEY,
            meeting_uuid    TEXT NOT NULL,
            date_time       TIMESTAMPTZ,
            name            TEXT,
            email           TEXT,
            duration        INTEGER,
            type            TEXT,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_rec_analytics_mtg ON {SCHEMA_NAME}.recording_analytics (meeting_uuid)",
        ])
    finally:
        conn.close()

    recording_uuids = _get_recording_uuids_since(start)
    log.info(f"Fetching analytics for {len(recording_uuids)} recordings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(recording_uuids):
        try:
            for entry in zoom.fetch_recording_analytics(meeting_id=uuid, log=log):
                aid = hashlib.sha256(f"{uuid}:{entry.get('date_time', '')}:{entry.get('email', '')}:{entry.get('type', '')}:{entry.get('name', '')}".encode()).hexdigest()[:24]
                batch.append((
                    aid, uuid,
                    entry.get("date_time"), entry.get("name"), entry.get("email"),
                    entry.get("duration"), entry.get("type"),
                    clean_json(entry), run_ts,
                ))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch analytics for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "recording_analytics", ANALYTICS_COLUMNS, batch,
                            "analytics_id", [c for c in ANALYTICS_COLUMNS if c != "analytics_id"],
                            "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "recording_analytics", ANALYTICS_COLUMNS, batch,
                        "analytics_id", [c for c in ANALYTICS_COLUMNS if c != "analytics_id"],
                        "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} recording analytics ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "analytics_synced": dg.MetadataValue.int(total),
        "recordings_processed": dg.MetadataValue.int(len(recording_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── meeting summaries (AI companion) ──

SUMMARY_COLUMNS = [
    "summary_id", "meeting_uuid", "meeting_id", "meeting_host_id",
    "meeting_host_email", "meeting_topic", "meeting_start_time", "meeting_end_time",
    "summary_start_time", "summary_end_time", "summary_content",
    "next_steps", "raw", "_synced_at",
]


def _summary_row(s: dict, run_ts: datetime) -> tuple:
    return (
        s.get("meeting_uuid") or s.get("meeting_id"),
        s.get("meeting_uuid"),
        s.get("meeting_id"),
        s.get("meeting_host_id"),
        s.get("meeting_host_email"),
        s.get("meeting_topic"),
        s.get("meeting_start_time"),
        s.get("meeting_end_time"),
        s.get("summary_start_time"),
        s.get("summary_end_time"),
        s.get("summary_content"),
        clean_json(s.get("next_steps")),
        clean_json(s),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="Zoom AI Companion meeting summaries",
)
def zoom_meeting_summaries(
    context: dg.AssetExecutionContext,
    config: ZoomRecordingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)
    end = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_summaries", """
            summary_id          TEXT PRIMARY KEY,
            meeting_uuid        TEXT,
            meeting_id          BIGINT,
            meeting_host_id     TEXT,
            meeting_host_email  TEXT,
            meeting_topic       TEXT,
            meeting_start_time  TIMESTAMPTZ,
            meeting_end_time    TIMESTAMPTZ,
            summary_start_time  TIMESTAMPTZ,
            summary_end_time    TIMESTAMPTZ,
            summary_content     TEXT,
            next_steps          JSONB,
            raw                 JSONB,
            _synced_at          TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_summary_start ON {SCHEMA_NAME}.meeting_summaries (meeting_start_time)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_summary_host ON {SCHEMA_NAME}.meeting_summaries (meeting_host_id)",
        ])
    finally:
        conn.close()

    batch: List[tuple] = []
    total = 0

    for s in zoom.fetch_meeting_summaries(start=start, end=end, log=log):
        batch.append(_summary_row(s, run_ts))

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_summaries", SUMMARY_COLUMNS, batch,
                            "summary_id", [c for c in SUMMARY_COLUMNS if c != "summary_id"],
                            "(" + ", ".join(["%s"] * 11 + ["%s::jsonb", "%s::jsonb", "%s"]) + ")")
            finally:
                conn.close()
            total += len(batch)
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_summaries", SUMMARY_COLUMNS, batch,
                        "summary_id", [c for c in SUMMARY_COLUMNS if c != "summary_id"],
                        "(" + ", ".join(["%s"] * 11 + ["%s::jsonb", "%s::jsonb", "%s"]) + ")")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} meeting summaries")

    return dg.MaterializeResult(metadata={
        "summaries_synced": dg.MetadataValue.int(total),
    })


# ── cloud recording usage report ──

USAGE_COLUMNS = [
    "usage_id", "user_id", "user_name", "email", "dept",
    "free_usage", "plan_usage", "usage", "raw", "_synced_at",
]


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="Zoom cloud recording storage usage per user",
)
def zoom_cloud_recording_usage(
    context: dg.AssetExecutionContext,
    config: ZoomRecordingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)
    end = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "cloud_recording_usage", """
            usage_id    TEXT PRIMARY KEY,
            user_id     TEXT,
            user_name   TEXT,
            email       TEXT,
            dept        TEXT,
            free_usage  TEXT,
            plan_usage  TEXT,
            usage       TEXT,
            raw         JSONB,
            _synced_at  TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_rec_usage_email ON {SCHEMA_NAME}.cloud_recording_usage (email)",
        ])
    finally:
        conn.close()

    seen: dict[str, tuple] = {}
    for entry in zoom.fetch_cloud_recording_usage(start=start, end=end, log=log):
        uid = entry.get("user_id") or entry.get("email", "unknown")
        seen[uid] = (
            uid,
            entry.get("user_id"),
            entry.get("user_name"),
            entry.get("email"),
            entry.get("dept"),
            entry.get("free_usage"),
            entry.get("plan_usage"),
            entry.get("usage"),
            clean_json(entry),
            run_ts,
        )
    rows = list(seen.values())

    conn = get_db_connection()
    try:
        upsert_rows(conn, "cloud_recording_usage", USAGE_COLUMNS, rows,
                    "usage_id", [c for c in USAGE_COLUMNS if c != "usage_id"],
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
    finally:
        conn.close()

    log.info(f"Sync complete: {len(rows)} cloud recording usage entries")

    return dg.MaterializeResult(metadata={
        "entries_synced": dg.MetadataValue.int(len(rows)),
    })


def _get_recording_uuids_since(start: datetime) -> List[str]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT recording_id FROM {SCHEMA_NAME}.recordings WHERE start_time >= %s ORDER BY start_time",
                (start,),
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
