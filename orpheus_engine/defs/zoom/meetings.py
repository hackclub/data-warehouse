import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import dagster as dg

from orpheus_engine.defs.shared.zoom import ZoomResource
from orpheus_engine.defs.zoom.db import SCHEMA_NAME, clean_json, ensure_table, get_db_connection, upsert_rows


BATCH_SIZE = 500
OVERLAP = timedelta(days=1)
LOOKBACK = timedelta(days=180)


class ZoomMeetingSyncConfig(dg.Config):
    start_date_override: Optional[str] = None


def _resolve_start(context: dg.AssetExecutionContext, config: ZoomMeetingSyncConfig, log) -> datetime:
    if config.start_date_override:
        log.info(f"Using manual start_date override: {config.start_date_override}")
        return datetime.fromisoformat(config.start_date_override).replace(tzinfo=timezone.utc)

    last_event = context.instance.get_latest_materialization_event(context.asset_key)
    if last_event and last_event.asset_materialization:
        md = last_event.asset_materialization.metadata
        if "latest_meeting_end" in md:
            hwm = md["latest_meeting_end"].value
            start = datetime.fromisoformat(hwm) - OVERLAP
            log.info(f"Resuming from {hwm} (with {OVERLAP} overlap: {start.isoformat()})")
            return start

    start = datetime.now(timezone.utc) - LOOKBACK
    log.info(f"No previous materialization — fetching from {start.date()}")
    return start


# ── meetings ──

MEETING_COLUMNS = [
    "uuid", "meeting_id", "host_id", "host_email", "topic", "type",
    "start_time", "end_time", "duration", "total_minutes",
    "participants_count", "source", "raw", "_synced_at",
]


def _meeting_row(m: dict, run_ts: datetime) -> tuple:
    return (
        m.get("uuid"),
        m.get("id"),
        m.get("host_id"),
        m.get("host"),  # email in report endpoint
        m.get("topic"),
        m.get("type"),
        m.get("start_time"),
        m.get("end_time"),
        m.get("duration"),
        m.get("total_minutes"),
        m.get("participants_count"),
        m.get("source"),
        clean_json(m),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="All Zoom meetings (discovered via per-user meeting reports)",
)
def zoom_meetings(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)
    end = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meetings", """
            uuid                TEXT PRIMARY KEY,
            meeting_id          BIGINT,
            host_id             TEXT,
            host_email          TEXT,
            topic               TEXT,
            type                INTEGER,
            start_time          TIMESTAMPTZ,
            end_time            TIMESTAMPTZ,
            duration            INTEGER,
            total_minutes       INTEGER,
            participants_count  INTEGER,
            source              TEXT,
            raw                 JSONB,
            _synced_at          TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_mtg_start ON {SCHEMA_NAME}.meetings (start_time)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_mtg_host ON {SCHEMA_NAME}.meetings (host_id)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_mtg_id ON {SCHEMA_NAME}.meetings (meeting_id)",
        ])
    finally:
        conn.close()

    user_ids = []
    for user in zoom.fetch_users(log=log):
        user_ids.append(user["id"])
    log.info(f"Fetching meetings for {len(user_ids)} users from {start.date()} to {end.date()}")

    batch: List[tuple] = []
    total = 0
    latest_end: Optional[str] = None

    for i, uid in enumerate(user_ids):
        for meeting in zoom.fetch_user_meetings(user_id=uid, start=start, end=end, log=log):
            batch.append(_meeting_row(meeting, run_ts))
            me = meeting.get("end_time")
            if me and (latest_end is None or me > latest_end):
                latest_end = me

            if len(batch) >= BATCH_SIZE:
                conn = get_db_connection()
                try:
                    upsert_rows(conn, "meetings", MEETING_COLUMNS, batch,
                                "uuid", [c for c in MEETING_COLUMNS if c != "uuid"],
                                "(" + ", ".join(["%s"] * 12 + ["%s::jsonb", "%s"]) + ")")
                finally:
                    conn.close()
                total += len(batch)
                batch = []

        if (i + 1) % 20 == 0:
            log.info(f"Processed {i + 1}/{len(user_ids)} users, {total + len(batch)} meetings so far")

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meetings", MEETING_COLUMNS, batch,
                        "uuid", [c for c in MEETING_COLUMNS if c != "uuid"],
                        "(" + ", ".join(["%s"] * 12 + ["%s::jsonb", "%s"]) + ")")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} meetings")

    metadata: Dict[str, Any] = {"meetings_synced": dg.MetadataValue.int(total)}
    if latest_end:
        metadata["latest_meeting_end"] = dg.MetadataValue.text(latest_end)
    return dg.MaterializeResult(metadata=metadata)


# ── meeting participants (report) ──

PARTICIPANT_COLUMNS = [
    "participant_id", "meeting_uuid", "user_id", "name", "user_email",
    "join_time", "leave_time", "duration", "registrant_id",
    "failover", "status", "bo_mtg_id", "customer_key",
    "raw", "_synced_at",
]


def _participant_row(p: dict, meeting_uuid: str, run_ts: datetime) -> tuple:
    pid = f"{meeting_uuid}:{p.get('id', '')}:{p.get('join_time', '')}"
    return (
        pid,
        meeting_uuid,
        p.get("user_id"),
        p.get("name"),
        p.get("user_email"),
        p.get("join_time"),
        p.get("leave_time"),
        p.get("duration"),
        p.get("registrant_id"),
        p.get("failover"),
        p.get("status"),
        p.get("bo_mtg_id"),
        p.get("customer_key"),
        clean_json(p),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_meetings],
    description="Zoom meeting participants from report API (join/leave times, breakout room IDs)",
)
def zoom_meeting_participants(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_participants", """
            participant_id  TEXT PRIMARY KEY,
            meeting_uuid    TEXT NOT NULL,
            user_id         TEXT,
            name            TEXT,
            user_email      TEXT,
            join_time       TIMESTAMPTZ,
            leave_time      TIMESTAMPTZ,
            duration        INTEGER,
            registrant_id   TEXT,
            failover        BOOLEAN,
            status          TEXT,
            bo_mtg_id       TEXT,
            customer_key    TEXT,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_part_mtg ON {SCHEMA_NAME}.meeting_participants (meeting_uuid)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_part_email ON {SCHEMA_NAME}.meeting_participants (user_email)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_part_join ON {SCHEMA_NAME}.meeting_participants (join_time)",
        ])
    finally:
        conn.close()

    meeting_uuids = _get_meeting_uuids_since(start)
    log.info(f"Fetching participants for {len(meeting_uuids)} meetings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(meeting_uuids):
        try:
            for p in zoom.fetch_meeting_participants(meeting_id=uuid, log=log):
                batch.append(_participant_row(p, uuid, run_ts))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch participants for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_participants", PARTICIPANT_COLUMNS, batch,
                            "participant_id", [c for c in PARTICIPANT_COLUMNS if c != "participant_id"],
                            "(" + ", ".join(["%s"] * 13 + ["%s::jsonb", "%s"]) + ")")
            finally:
                conn.close()
            total += len(batch)
            batch = []

        if (i + 1) % 50 == 0:
            log.info(f"Processed {i + 1}/{len(meeting_uuids)} meetings, {total + len(batch)} participants")

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_participants", PARTICIPANT_COLUMNS, batch,
                        "participant_id", [c for c in PARTICIPANT_COLUMNS if c != "participant_id"],
                        "(" + ", ".join(["%s"] * 13 + ["%s::jsonb", "%s"]) + ")")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} participants across {len(meeting_uuids)} meetings ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "participants_synced": dg.MetadataValue.int(total),
        "meetings_processed": dg.MetadataValue.int(len(meeting_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── meeting participant details (dashboard/metrics) ──

DETAIL_COLUMNS = [
    "detail_id", "meeting_uuid", "user_id", "user_name", "device",
    "client", "ip_address", "internal_ip_addresses",
    "location", "network_type", "data_center", "full_data_center",
    "connection_type", "video_connection_type", "as_connection_type",
    "join_time", "leave_time",
    "pc_name", "domain", "mac_addr", "harddisk_id",
    "version", "microphone", "speaker", "camera",
    "share_application", "share_desktop", "share_whiteboard", "recording",
    "raw", "_synced_at",
]


def _detail_row(p: dict, meeting_uuid: str, run_ts: datetime) -> tuple:
    did = f"{meeting_uuid}:{p.get('id', '')}:{p.get('join_time', '')}"
    return (
        did,
        meeting_uuid,
        p.get("user_id"),
        p.get("user_name"),
        p.get("device"),
        p.get("client"),
        p.get("ip_address"),
        clean_json(p.get("internal_ip_addresses")),
        p.get("location"),
        p.get("network_type"),
        p.get("data_center"),
        p.get("full_data_center"),
        p.get("connection_type"),
        p.get("video_connection_type"),
        p.get("as_connection_type"),
        p.get("join_time"),
        p.get("leave_time"),
        p.get("pc_name"),
        p.get("domain"),
        p.get("mac_addr"),
        p.get("harddisk_id"),
        p.get("version"),
        p.get("microphone"),
        p.get("speaker"),
        p.get("camera"),
        p.get("share_application"),
        p.get("share_desktop"),
        p.get("share_whiteboard"),
        p.get("recording"),
        clean_json(p),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_meetings],
    description="Zoom meeting participant device/network details from dashboard API (MAC, IP, hardware, etc.)",
)
def zoom_meeting_participant_details(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_participant_details", """
            detail_id               TEXT PRIMARY KEY,
            meeting_uuid            TEXT NOT NULL,
            user_id                 TEXT,
            user_name               TEXT,
            device                  TEXT,
            client                  TEXT,
            ip_address              TEXT,
            internal_ip_addresses   JSONB,
            location                TEXT,
            network_type            TEXT,
            data_center             TEXT,
            full_data_center        TEXT,
            connection_type         TEXT,
            video_connection_type   TEXT,
            as_connection_type      TEXT,
            join_time               TIMESTAMPTZ,
            leave_time              TIMESTAMPTZ,
            pc_name                 TEXT,
            domain                  TEXT,
            mac_addr                TEXT,
            harddisk_id             TEXT,
            version                 TEXT,
            microphone              TEXT,
            speaker                 TEXT,
            camera                  TEXT,
            share_application       BOOLEAN,
            share_desktop           BOOLEAN,
            share_whiteboard        BOOLEAN,
            recording               BOOLEAN,
            raw                     JSONB,
            _synced_at              TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_detail_mtg ON {SCHEMA_NAME}.meeting_participant_details (meeting_uuid)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_detail_mac ON {SCHEMA_NAME}.meeting_participant_details (mac_addr)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_detail_ip ON {SCHEMA_NAME}.meeting_participant_details (ip_address)",
        ])
    finally:
        conn.close()

    meeting_uuids = _get_meeting_uuids_since(start)
    log.info(f"Fetching participant details for {len(meeting_uuids)} meetings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(meeting_uuids):
        try:
            for p in zoom.fetch_meeting_participant_details(meeting_id=uuid, log=log):
                batch.append(_detail_row(p, uuid, run_ts))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch details for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_participant_details", DETAIL_COLUMNS, batch,
                            "detail_id", [c for c in DETAIL_COLUMNS if c != "detail_id"],
                            "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            batch = []

        if (i + 1) % 50 == 0:
            log.info(f"Processed {i + 1}/{len(meeting_uuids)} meetings, {total + len(batch)} details")

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_participant_details", DETAIL_COLUMNS, batch,
                        "detail_id", [c for c in DETAIL_COLUMNS if c != "detail_id"],
                        "(" + ", ".join(["%s"] * 23 + ["%s::jsonb", "%s"]) + ")")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} participant details ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "details_synced": dg.MetadataValue.int(total),
        "meetings_processed": dg.MetadataValue.int(len(meeting_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── meeting participant QoS ──

QOS_COLUMNS = [
    "qos_id", "meeting_uuid", "user_id", "user_name",
    "device", "ip_address", "location", "network_type",
    "data_center", "connection_type", "join_time", "leave_time",
    "qos_data", "raw", "_synced_at",
]


def _qos_row(p: dict, meeting_uuid: str, run_ts: datetime) -> tuple:
    qid = f"{meeting_uuid}:{p.get('id', '')}:{p.get('join_time', '')}"
    return (
        qid,
        meeting_uuid,
        p.get("user_id"),
        p.get("user_name"),
        p.get("device"),
        p.get("ip_address"),
        p.get("location"),
        p.get("network_type"),
        p.get("data_center"),
        p.get("connection_type"),
        p.get("join_time"),
        p.get("leave_time"),
        clean_json(p.get("user_qos")),
        clean_json(p),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_meetings],
    description="Zoom per-participant quality of service metrics (bitrate, latency, jitter, packet loss)",
)
def zoom_meeting_participant_qos(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_participant_qos", """
            qos_id          TEXT PRIMARY KEY,
            meeting_uuid    TEXT NOT NULL,
            user_id         TEXT,
            user_name       TEXT,
            device          TEXT,
            ip_address      TEXT,
            location        TEXT,
            network_type    TEXT,
            data_center     TEXT,
            connection_type TEXT,
            join_time       TIMESTAMPTZ,
            leave_time      TIMESTAMPTZ,
            qos_data        JSONB,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_qos_mtg ON {SCHEMA_NAME}.meeting_participant_qos (meeting_uuid)",
        ])
    finally:
        conn.close()

    meeting_uuids = _get_meeting_uuids_since(start)
    log.info(f"Fetching QoS for {len(meeting_uuids)} meetings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(meeting_uuids):
        try:
            for p in zoom.fetch_meeting_participant_qos(meeting_id=uuid, log=log):
                batch.append(_qos_row(p, uuid, run_ts))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch QoS for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_participant_qos", QOS_COLUMNS, batch,
                            "qos_id", [c for c in QOS_COLUMNS if c != "qos_id"],
                            "(" + ", ".join(["%s"] * 12 + ["%s::jsonb", "%s::jsonb", "%s"]) + ")")
            finally:
                conn.close()
            total += len(batch)
            batch = []

        if (i + 1) % 50 == 0:
            log.info(f"Processed {i + 1}/{len(meeting_uuids)} meetings, {total + len(batch)} QoS records")

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_participant_qos", QOS_COLUMNS, batch,
                        "qos_id", [c for c in QOS_COLUMNS if c != "qos_id"],
                        "(" + ", ".join(["%s"] * 12 + ["%s::jsonb", "%s::jsonb", "%s"]) + ")")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} QoS records ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "qos_records_synced": dg.MetadataValue.int(total),
        "meetings_processed": dg.MetadataValue.int(len(meeting_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── meeting sharing ──

SHARING_COLUMNS = [
    "sharing_id", "meeting_uuid", "user_id", "user_name", "details", "raw", "_synced_at",
]


def _sharing_row(p: dict, meeting_uuid: str, run_ts: datetime) -> tuple:
    sid = f"{meeting_uuid}:{p.get('id', '')}:{p.get('user_name', '')}"
    return (
        sid,
        meeting_uuid,
        p.get("id"),
        p.get("user_name"),
        clean_json(p.get("details")),
        clean_json(p),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_meetings],
    description="Zoom meeting screen sharing / recording activity per participant",
)
def zoom_meeting_sharing(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_sharing", """
            sharing_id      TEXT PRIMARY KEY,
            meeting_uuid    TEXT NOT NULL,
            user_id         TEXT,
            user_name       TEXT,
            details         JSONB,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_sharing_mtg ON {SCHEMA_NAME}.meeting_sharing (meeting_uuid)",
        ])
    finally:
        conn.close()

    meeting_uuids = _get_meeting_uuids_since(start)
    log.info(f"Fetching sharing data for {len(meeting_uuids)} meetings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(meeting_uuids):
        try:
            for p in zoom.fetch_meeting_sharing(meeting_id=uuid, log=log):
                batch.append(_sharing_row(p, uuid, run_ts))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch sharing for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_sharing", SHARING_COLUMNS, batch,
                            "sharing_id", [c for c in SHARING_COLUMNS if c != "sharing_id"],
                            "(%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_sharing", SHARING_COLUMNS, batch,
                        "sharing_id", [c for c in SHARING_COLUMNS if c != "sharing_id"],
                        "(%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} sharing records ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "sharing_records_synced": dg.MetadataValue.int(total),
        "meetings_processed": dg.MetadataValue.int(len(meeting_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── polls ──

POLL_COLUMNS = [
    "poll_id", "meeting_uuid", "email", "name",
    "question_title", "answer", "raw", "_synced_at",
]


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_meetings],
    description="Zoom meeting poll responses",
)
def zoom_meeting_polls(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_polls", """
            poll_id         TEXT PRIMARY KEY,
            meeting_uuid    TEXT NOT NULL,
            email           TEXT,
            name            TEXT,
            question_title  TEXT,
            answer          TEXT,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_polls_mtg ON {SCHEMA_NAME}.meeting_polls (meeting_uuid)",
        ])
    finally:
        conn.close()

    meeting_uuids = _get_meeting_uuids_since(start)
    log.info(f"Fetching polls for {len(meeting_uuids)} meetings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(meeting_uuids):
        try:
            questions = zoom.fetch_meeting_polls(meeting_id=uuid, log=log)
            for q in questions:
                for detail in q.get("question_details", []):
                    pid = f"{uuid}:{q.get('email', '')}:{q.get('name', '')}:{detail.get('question', '')}"
                    batch.append((
                        pid, uuid,
                        q.get("email"), q.get("name"),
                        detail.get("question"), detail.get("answer"),
                        clean_json(q), run_ts,
                    ))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch polls for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_polls", POLL_COLUMNS, batch,
                            "poll_id", [c for c in POLL_COLUMNS if c != "poll_id"],
                            "(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_polls", POLL_COLUMNS, batch,
                        "poll_id", [c for c in POLL_COLUMNS if c != "poll_id"],
                        "(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} poll responses ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "poll_responses_synced": dg.MetadataValue.int(total),
        "meetings_processed": dg.MetadataValue.int(len(meeting_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── Q&A ──

QA_COLUMNS = [
    "qa_id", "meeting_uuid", "question", "asker_name", "asker_email",
    "answer", "answerer_name", "raw", "_synced_at",
]


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    deps=[zoom_meetings],
    description="Zoom meeting Q&A data",
)
def zoom_meeting_qa(
    context: dg.AssetExecutionContext,
    config: ZoomMeetingSyncConfig,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)
    start = _resolve_start(context, config, log)

    conn = get_db_connection()
    try:
        ensure_table(conn, "meeting_qa", """
            qa_id           TEXT PRIMARY KEY,
            meeting_uuid    TEXT NOT NULL,
            question        TEXT,
            asker_name      TEXT,
            asker_email     TEXT,
            answer          TEXT,
            answerer_name   TEXT,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_qa_mtg ON {SCHEMA_NAME}.meeting_qa (meeting_uuid)",
        ])
    finally:
        conn.close()

    meeting_uuids = _get_meeting_uuids_since(start)
    log.info(f"Fetching Q&A for {len(meeting_uuids)} meetings")

    batch: List[tuple] = []
    total = 0
    errors = 0

    for i, uuid in enumerate(meeting_uuids):
        try:
            questions = zoom.fetch_meeting_qa(meeting_id=uuid, log=log)
            for q in questions:
                qid = f"{uuid}:{q.get('question', '')}:{q.get('email', '')}"
                answer_list = q.get("answer_list", [])
                if answer_list:
                    for a in answer_list:
                        aid = f"{qid}:{a.get('name', '')}"
                        batch.append((
                            aid, uuid,
                            q.get("question"), q.get("name"), q.get("email"),
                            a.get("answer"), a.get("name"),
                            clean_json(q), run_ts,
                        ))
                else:
                    batch.append((
                        qid, uuid,
                        q.get("question"), q.get("name"), q.get("email"),
                        None, None,
                        clean_json(q), run_ts,
                    ))
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Failed to fetch Q&A for {uuid}: {e}")

        if len(batch) >= BATCH_SIZE:
            conn = get_db_connection()
            try:
                upsert_rows(conn, "meeting_qa", QA_COLUMNS, batch,
                            "qa_id", [c for c in QA_COLUMNS if c != "qa_id"],
                            "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
            finally:
                conn.close()
            total += len(batch)
            batch = []

    if batch:
        conn = get_db_connection()
        try:
            upsert_rows(conn, "meeting_qa", QA_COLUMNS, batch,
                        "qa_id", [c for c in QA_COLUMNS if c != "qa_id"],
                        "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)")
        finally:
            conn.close()
        total += len(batch)

    log.info(f"Sync complete: {total} Q&A entries ({errors} errors)")

    return dg.MaterializeResult(metadata={
        "qa_entries_synced": dg.MetadataValue.int(total),
        "meetings_processed": dg.MetadataValue.int(len(meeting_uuids)),
        "errors": dg.MetadataValue.int(errors),
    })


# ── shared helper: get meeting UUIDs from warehouse ──

def _get_meeting_uuids_since(start: datetime) -> List[str]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT uuid FROM {SCHEMA_NAME}.meetings WHERE start_time >= %s ORDER BY start_time",
                (start,),
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
