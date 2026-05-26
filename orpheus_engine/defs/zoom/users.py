from datetime import datetime, timezone
from typing import Any, Dict, List

import dagster as dg

from orpheus_engine.defs.shared.zoom import ZoomResource
from orpheus_engine.defs.zoom.db import SCHEMA_NAME, clean_json, ensure_table, get_db_connection, upsert_rows


COLUMNS = [
    "user_id", "email", "first_name", "last_name", "display_name",
    "type", "status", "role_name", "role_id",
    "pmi", "timezone", "dept", "created_at", "last_login_time",
    "language", "phone_number", "pic_url",
    "verified", "cluster", "user_created_at",
    "raw", "_synced_at",
]


def _user_row(user: dict, run_ts: datetime) -> tuple:
    return (
        user.get("id"),
        user.get("email"),
        user.get("first_name"),
        user.get("last_name"),
        user.get("display_name"),
        user.get("type"),
        user.get("status"),
        user.get("role_name"),
        user.get("role_id"),
        user.get("pmi"),
        user.get("timezone"),
        user.get("dept"),
        user.get("created_at"),
        user.get("last_login_time"),
        user.get("language"),
        user.get("phone_number"),
        user.get("pic_url"),
        user.get("verified"),
        user.get("cluster"),
        user.get("user_created_at"),
        clean_json(user),
        run_ts,
    )


@dg.asset(
    compute_kind="zoom_api",
    group_name="zoom",
    description="All Zoom account users",
)
def zoom_users(
    context: dg.AssetExecutionContext,
    zoom: ZoomResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)

    conn = get_db_connection()
    try:
        ensure_table(conn, "users", """
            user_id         TEXT PRIMARY KEY,
            email           TEXT,
            first_name      TEXT,
            last_name       TEXT,
            display_name    TEXT,
            type            INTEGER,
            status          TEXT,
            role_name       TEXT,
            role_id         TEXT,
            pmi             BIGINT,
            timezone        TEXT,
            dept            TEXT,
            created_at      TIMESTAMPTZ,
            last_login_time TIMESTAMPTZ,
            language        TEXT,
            phone_number    TEXT,
            pic_url         TEXT,
            verified        INTEGER,
            cluster         TEXT,
            user_created_at TIMESTAMPTZ,
            raw             JSONB,
            _synced_at      TIMESTAMPTZ DEFAULT NOW()
        """, [
            f"CREATE INDEX IF NOT EXISTS idx_zoom_users_email ON {SCHEMA_NAME}.users (email)",
            f"CREATE INDEX IF NOT EXISTS idx_zoom_users_status ON {SCHEMA_NAME}.users (status)",
        ])
    finally:
        conn.close()

    seen: dict[str, tuple] = {}
    for status in ("active", "inactive", "pending"):
        for user in zoom.fetch_users(status=status, log=log):
            row = _user_row(user, run_ts)
            seen[row[0]] = row
    rows = list(seen.values())

    conn = get_db_connection()
    try:
        update_cols = [c for c in COLUMNS if c not in ("user_id",)]
        upsert_rows(conn, "users", COLUMNS, rows, "user_id", update_cols,
                     "(" + ", ".join(["%s"] * 20 + ["%s::jsonb", "%s"]) + ")")
    finally:
        conn.close()

    type_counts: Dict[int, int] = {}
    for row in rows:
        t = row[5] or 0
        type_counts[t] = type_counts.get(t, 0) + 1

    log.info(f"Synced {len(rows)} users — types: {type_counts}")

    metadata: Dict[str, Any] = {"users_synced": dg.MetadataValue.int(len(rows))}
    type_labels = {1: "basic", 2: "licensed", 3: "on_prem", 4: "none"}
    for t, count in sorted(type_counts.items()):
        metadata[f"type_{type_labels.get(t, str(t))}"] = dg.MetadataValue.int(count)

    return dg.MaterializeResult(metadata=metadata)
