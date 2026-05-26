from datetime import datetime, timezone
from typing import Any, Dict, List

import dagster as dg

from orpheus_engine.defs.shared.zoom import ZoomResource
from orpheus_engine.defs.zoom.db import SCHEMA_NAME, ensure_table, get_db_connection, upsert_rows


COLUMNS = [
    "date", "new_users", "meetings", "participants", "meeting_minutes", "_synced_at",
]


def _row(day: dict, run_ts: datetime) -> tuple:
    return (
        day.get("date"),
        day.get("new_users"),
        day.get("meetings"),
        day.get("participants"),
        day.get("meeting_minutes"),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="Zoom daily account-level usage stats",
)
def zoom_daily_usage(
    context: dg.AssetExecutionContext,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "daily_usage", """
            date            DATE PRIMARY KEY,
            new_users       INTEGER,
            meetings        INTEGER,
            participants    INTEGER,
            meeting_minutes INTEGER,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """)
    finally:
        conn.close()

    now = datetime.now(timezone.utc)
    seen: dict[str, tuple] = {}

    for months_ago in range(6):
        year = now.year
        month = now.month - months_ago
        if month <= 0:
            month += 12
            year -= 1
        try:
            days = zoom.fetch_daily_usage(year=year, month=month, log=log)
            for day in days:
                row = _row(day, run_ts)
                seen[row[0]] = row
            log.info(f"Fetched {len(days)} days for {year}-{month:02d}")
        except Exception as e:
            log.warning(f"Could not fetch {year}-{month:02d}: {e}")
    rows = list(seen.values())

    conn = get_db_connection()
    try:
        update_cols = [c for c in COLUMNS if c != "date"]
        upsert_rows(conn, "daily_usage", COLUMNS, rows, "date", update_cols)
    finally:
        conn.close()

    log.info(f"Synced {len(rows)} daily usage rows")

    total_meetings = sum(r[2] or 0 for r in rows)
    total_participants = sum(r[3] or 0 for r in rows)

    return dg.MaterializeResult(metadata={
        "days_synced": dg.MetadataValue.int(len(rows)),
        "total_meetings": dg.MetadataValue.int(total_meetings),
        "total_participants": dg.MetadataValue.int(total_participants),
    })
