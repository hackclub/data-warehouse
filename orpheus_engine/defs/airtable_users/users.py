import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

import dagster as dg

from orpheus_engine.defs.shared.airtable_enterprise import AirtableEnterpriseResource


SCHEMA_NAME = "airtable_users"


def _get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def _ensure_users_table(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(SCHEMA_NAME)
        ))
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.users (
                user_id             TEXT PRIMARY KEY,
                email               TEXT,
                name                TEXT,
                state               TEXT,
                license_type        TEXT,
                last_activity_time  TIMESTAMPTZ,
                created_time        TIMESTAMPTZ,
                is_admin            BOOLEAN,
                is_managed          BOOLEAN,
                is_service_account  BOOLEAN,
                is_two_factor_auth  BOOLEAN,
                is_sso_required     BOOLEAN,
                invited_by_user_id  TEXT,
                _synced_at          TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()


def _user_to_row(user: dict, run_ts: datetime) -> tuple:
    return (
        user.get("id"),
        user.get("email"),
        user.get("name"),
        user.get("state"),
        user.get("licenseType"),
        user.get("lastActivityTime"),
        user.get("createdTime"),
        user.get("isAdmin"),
        user.get("isManaged"),
        user.get("isServiceAccount"),
        user.get("isTwoFactorAuthEnabled"),
        user.get("isSsoRequired"),
        user.get("invitedToAirtableByUserId"),
        run_ts,
    )


def _upsert_users(conn, rows: list):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.users
                (user_id, email, name, state, license_type,
                 last_activity_time, created_time,
                 is_admin, is_managed, is_service_account,
                 is_two_factor_auth, is_sso_required,
                 invited_by_user_id, _synced_at)
            VALUES %s
            ON CONFLICT (user_id) DO UPDATE SET
                email = EXCLUDED.email,
                name = EXCLUDED.name,
                state = EXCLUDED.state,
                license_type = EXCLUDED.license_type,
                last_activity_time = EXCLUDED.last_activity_time,
                is_admin = EXCLUDED.is_admin,
                is_managed = EXCLUDED.is_managed,
                is_service_account = EXCLUDED.is_service_account,
                is_two_factor_auth = EXCLUDED.is_two_factor_auth,
                is_sso_required = EXCLUDED.is_sso_required,
                _synced_at = EXCLUDED._synced_at
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=500,
        )
    conn.commit()


@dg.asset(
    compute_kind="airtable_api",
    group_name="airtable_audit_logs",
    description="Syncs Airtable Enterprise user list with seat type and last activity for billing analysis",
)
def airtable_enterprise_users(
    context: dg.AssetExecutionContext,
    airtable_enterprise: AirtableEnterpriseResource,
) -> dg.MaterializeResult:
    log = context.log
    run_ts = datetime.now(timezone.utc)

    conn = _get_db_connection()
    try:
        _ensure_users_table(conn)
    finally:
        conn.close()

    user_ids = airtable_enterprise.fetch_enterprise_user_ids(log=log)

    rows: List[tuple] = []
    for user in airtable_enterprise.fetch_users(user_ids, log=log):
        rows.append(_user_to_row(user, run_ts))

    conn = _get_db_connection()
    try:
        _upsert_users(conn, rows)
    finally:
        conn.close()

    license_counts: Dict[str, int] = {}
    for row in rows:
        lt = row[4] or "unknown"
        license_counts[lt] = license_counts.get(lt, 0) + 1

    log.info(f"Synced {len(rows)} users — {license_counts}")

    metadata: Dict[str, Any] = {
        "users_synced": dg.MetadataValue.int(len(rows)),
    }
    for lt, count in sorted(license_counts.items()):
        metadata[f"license_{lt}"] = dg.MetadataValue.int(count)

    return dg.MaterializeResult(metadata=metadata)
