"""
QuickBooks Online Sync

Syncs the QuickBooks Online company (realm QBO_REALM_ID) into the `quickbooks`
PostgreSQL schema as raw JSONB -- one table per entity type (quickbooks.invoices,
quickbooks.bills, ...). Each row holds the complete API object in `payload` plus
a few hoisted columns (txn_date, doc_number, name, total_amt, timestamps) for
convenient querying; dbt models can dig into the JSONB for the rest.

Auth: OAuth2 refresh-token flow against oauth.platform.intuit.com. Intuit
ROTATES refresh tokens (roughly daily; old values eventually die), so the
current token is persisted in quickbooks._qbo_oauth and QBO_REFRESH_TOKEN is
only the seed for first boot / re-authorization. The token table is excluded
from the DuckLake mirror (see ducklake EXCLUDED_TABLES) -- it must never leave
the warehouse. Access tokens last ~1h; a 401 mid-run triggers one re-refresh.
If every candidate token is rejected, re-authorize with scripts/qbo_auth.py.

API: GET /v3/company/{realm}/query with entity SQL, 1000 rows/page.
Rate limits: 500 req/min per realm, 10 concurrent. Intuit meters read calls
(free Builder tier: 500k/month, hard-blocked above), so the run emits its API
call count as metadata; a full refresh of these books is a few hundred calls.

Architecture mirrors the fillout sync: entities fan out across a thread pool,
every request passes through one shared rate limiter, each fetched page is
upserted immediately (bounded memory), and per-entity stale rows
(_synced_at < run start) are deleted only when that entity fetched completely,
so deletions in QuickBooks propagate without a partial run nuking data.
"""

import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Tuple

import psycopg2
import requests
from psycopg2 import sql
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)


SCHEMA_NAME = "quickbooks"
TOKEN_TABLE = "_qbo_oauth"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
DEFAULT_API_BASE = "https://quickbooks.api.intuit.com"
MINOR_VERSION = "75"
QUERY_PAGE_SIZE = 1000  # QBO query maximum
# QBO allows 500 req/min per realm; stay comfortably under it.
RATE_LIMIT_INTERVAL = 0.15  # seconds between requests (~6.7 req/s)
MAX_WORKERS = 5  # QBO caps concurrency at 10; throughput is gated by the limiter

# API entity name -> warehouse table name. All are queryable via the /query
# endpoint and share the same table shape. Entities missing from a company
# (e.g. no estimates) just return zero rows.
ENTITIES = {
    # Lists
    "Account": "accounts",
    "Class": "classes",
    "CompanyInfo": "company_info",
    "Customer": "customers",
    "Department": "departments",
    "Employee": "employees",
    "Item": "items",
    "PaymentMethod": "payment_methods",
    "Preferences": "preferences",
    "Term": "terms",
    "TaxCode": "tax_codes",
    "TaxRate": "tax_rates",
    "Budget": "budgets",
    # Transactions
    "Bill": "bills",
    "BillPayment": "bill_payments",
    "CreditCardPayment": "credit_card_payments",
    "CreditMemo": "credit_memos",
    "Deposit": "deposits",
    "Estimate": "estimates",
    "Invoice": "invoices",
    "JournalEntry": "journal_entries",
    "Payment": "payments",
    "Purchase": "purchases",
    "PurchaseOrder": "purchase_orders",
    "RefundReceipt": "refund_receipts",
    "SalesReceipt": "sales_receipts",
    "Transfer": "transfers",
    "VendorCredit": "vendor_credits",
    "Vendor": "vendors",
}

TOKEN_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TOKEN_TABLE} (
    client_id     TEXT PRIMARY KEY,
    refresh_token TEXT NOT NULL,
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL
);
"""


def entity_ddl(table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table} (
        id                TEXT PRIMARY KEY,
        txn_date          DATE,
        doc_number        TEXT,
        name              TEXT,
        total_amt         NUMERIC,
        created_time      TIMESTAMP WITH TIME ZONE,
        last_updated_time TIMESTAMP WITH TIME ZONE,
        payload           JSONB,
        _synced_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} environment variable is not set")
    return value


def get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def get_api_base() -> str:
    return os.getenv("QBO_API_BASE", DEFAULT_API_BASE).rstrip("/")


def ensure_schema_and_tables(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(SCHEMA_NAME)
        ))
        cur.execute(TOKEN_DDL)
        for table in ENTITIES.values():
            cur.execute(entity_ddl(table))
    conn.commit()


class QboAuthError(Exception):
    pass


class TokenManager:
    """Holds the current access token; refreshes via OAuth and persists rotated
    refresh tokens to quickbooks._qbo_oauth. Tries the stored token first, then
    the QBO_REFRESH_TOKEN env seed, so re-running scripts/qbo_auth.py after the
    stored token dies is all it takes to heal the connection."""

    def __init__(self, log):
        self._lock = threading.Lock()
        self._log = log
        self._client_id = _require_env("QBO_CLIENT_ID")
        self._client_secret = _require_env("QBO_CLIENT_SECRET")
        self._access_token = None

    def access_token(self) -> str:
        with self._lock:
            if self._access_token is None:
                self._refresh_locked()
            return self._access_token

    def handle_unauthorized(self, stale_token: str) -> str:
        """Refresh after a 401. Only the first thread holding the stale token
        actually refreshes; the rest reuse the replacement."""
        with self._lock:
            if self._access_token == stale_token:
                self._refresh_locked()
            return self._access_token

    def _refresh_locked(self):
        for source, refresh_token in self._candidate_refresh_tokens():
            resp = requests.post(
                TOKEN_URL,
                auth=(self._client_id, self._client_secret),
                headers={"Accept": "application/json"},
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=30,
            )
            if resp.ok:
                tokens = resp.json()
                self._access_token = tokens["access_token"]
                self._persist_refresh_token(tokens.get("refresh_token") or refresh_token)
                self._log.info(f"QBO access token obtained via {source} refresh token")
                return
            self._log.warning(
                f"QBO token refresh via {source} token failed ({resp.status_code}): {resp.text}"
            )
        raise QboAuthError(
            "Every QBO refresh token was rejected. Re-authorize with "
            "scripts/qbo_auth.py and update QBO_REFRESH_TOKEN."
        )

    def _candidate_refresh_tokens(self) -> Iterator[Tuple[str, str]]:
        stored = None
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT refresh_token FROM {SCHEMA_NAME}.{TOKEN_TABLE} WHERE client_id = %s",
                    (self._client_id,),
                )
                row = cur.fetchone()
                stored = row[0] if row else None
        finally:
            conn.close()
        if stored:
            yield "stored", stored
        env_seed = os.getenv("QBO_REFRESH_TOKEN")
        if env_seed and env_seed != stored:
            yield "env-seed", env_seed

    def _persist_refresh_token(self, refresh_token: str):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {SCHEMA_NAME}.{TOKEN_TABLE} (client_id, refresh_token, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (client_id) DO UPDATE SET
                        refresh_token = EXCLUDED.refresh_token,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (self._client_id, refresh_token),
                )
            conn.commit()
        finally:
            conn.close()


class RateLimiter:
    """Spaces requests across all threads to stay under a global req/s cap."""

    def __init__(self, min_interval: float):
        self._lock = threading.Lock()
        self._min_interval = min_interval
        self._next_allowed = 0.0

    def acquire(self):
        with self._lock:
            scheduled = max(time.monotonic(), self._next_allowed)
            self._next_allowed = scheduled + self._min_interval
        wait = scheduled - time.monotonic()
        if wait > 0:
            time.sleep(wait)


class ApiCallCounter:
    """Thread-safe counter so the run can report metered API usage."""

    def __init__(self):
        self._lock = threading.Lock()
        self.value = 0

    def increment(self):
        with self._lock:
            self.value += 1


def build_session() -> requests.Session:
    """Session without auth headers -- the bearer token is set per request so a
    mid-run token refresh takes effect everywhere immediately."""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    retry = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_with_retry(fn, *, max_attempts=8, log=None):
    """Run fn(), retrying on 429 with exponential backoff honoring Retry-After."""
    for attempt in range(max_attempts):
        try:
            return fn()
        except requests.exceptions.HTTPError as e:
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None)
            if status == 429 and attempt < max_attempts - 1:
                retry_after = None
                if resp is not None:
                    try:
                        retry_after = int(resp.headers.get("Retry-After", ""))
                    except (ValueError, TypeError):
                        pass
                wait = (retry_after if retry_after else min(2 * (2 ** attempt), 60)) + random.uniform(0, 1)
                if log:
                    log.warning(f"429 rate limited, waiting {wait:.1f}s (attempt {attempt + 1}/{max_attempts})")
                time.sleep(wait)
                continue
            raise


def qbo_get(session, limiter, tokens, counter, realm_id: str,
            path: str, params: Dict[str, Any]) -> Any:
    """Rate-limited authenticated GET; retries once through a token refresh on 401."""
    limiter.acquire()
    counter.increment()
    token = tokens.access_token()
    url = f"{get_api_base()}/v3/company/{realm_id}{path}"
    resp = session.get(url, params=params, timeout=90,
                       headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 401:
        token = tokens.handle_unauthorized(stale_token=token)
        limiter.acquire()
        counter.increment()
        resp = session.get(url, params=params, timeout=90,
                           headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()


def _j(value: Any) -> str:
    """JSON-encode for a JSONB column, stripping NUL bytes Postgres rejects."""
    return json.dumps(value).replace("\\u0000", "")


def _dedupe_by_pk(rows: List[Tuple], key_len: int) -> List[Tuple]:
    """Collapse rows sharing the same primary key, keeping the last occurrence;
    Postgres rejects an ON CONFLICT batch that touches the same key twice."""
    seen: Dict[Tuple, Tuple] = {}
    for row in rows:
        seen[row[:key_len]] = row
    return list(seen.values())


def _entity_row(obj: Dict[str, Any]) -> Tuple:
    """Build a row tuple (sans _synced_at) from a raw QBO object. Timestamps
    stay as ISO strings; Postgres casts them."""
    meta = obj.get("MetaData") or {}
    return (
        obj.get("Id"),
        obj.get("TxnDate"),
        obj.get("DocNumber"),
        obj.get("DisplayName") or obj.get("FullyQualifiedName") or obj.get("Name"),
        obj.get("TotalAmt"),
        meta.get("CreateTime"),
        meta.get("LastUpdatedTime"),
        _j(obj),
    )


def flush_rows(table: str, rows: List[Tuple], run_ts: datetime) -> int:
    if not rows:
        return 0
    rows = _dedupe_by_pk(rows, key_len=1)
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA_NAME}.{table}
                    (id, txn_date, doc_number, name, total_amt,
                     created_time, last_updated_time, payload, _synced_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    txn_date = EXCLUDED.txn_date,
                    doc_number = EXCLUDED.doc_number,
                    name = EXCLUDED.name,
                    total_amt = EXCLUDED.total_amt,
                    created_time = EXCLUDED.created_time,
                    last_updated_time = EXCLUDED.last_updated_time,
                    payload = EXCLUDED.payload,
                    _synced_at = EXCLUDED._synced_at
                """,
                [r + (run_ts,) for r in rows],
                template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=500,
            )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def sync_entity(entity: str, table: str, realm_id: str, limiter, tokens,
                counter, run_ts: datetime, log) -> int:
    """Page through one entity's full contents, upserting page by page."""
    total = 0
    start_position = 1
    with build_session() as session:
        while True:
            query = (
                f"SELECT * FROM {entity} "
                f"STARTPOSITION {start_position} MAXRESULTS {QUERY_PAGE_SIZE}"
            )
            data = fetch_with_retry(
                lambda q=query: qbo_get(
                    session, limiter, tokens, counter, realm_id,
                    "/query", {"query": q, "minorversion": MINOR_VERSION},
                ),
                log=log,
            )
            items = (data.get("QueryResponse") or {}).get(entity, [])
            total += flush_rows(
                table, [_entity_row(o) for o in items if o.get("Id")], run_ts
            )
            if len(items) < QUERY_PAGE_SIZE:
                break
            start_position += QUERY_PAGE_SIZE
            if start_position > 2_000_000:
                raise RuntimeError(f"{entity}: pagination did not terminate")
    return total


@asset(
    group_name="quickbooks",
    compute_kind="quickbooks",
    description=(
        "Syncs all QuickBooks Online entities into the `quickbooks` schema as "
        "raw JSONB (full refresh with per-entity staleness deletion)."
    ),
)
def quickbooks_online_sync(context: AssetExecutionContext) -> Output[None]:
    """
    1. Refreshes the OAuth access token (persisting the rotated refresh token)
    2. Pages through every entity in ENTITIES via the /query endpoint,
       upserting each page into its quickbooks.<table>, stamped with run_ts
    3. Per fully-synced entity, deletes rows with _synced_at < run_ts
       (objects deleted in QuickBooks disappear here too)
    """
    log = context.log
    run_ts = datetime.now(timezone.utc)
    realm_id = _require_env("QBO_REALM_ID")

    conn = get_db_connection()
    try:
        ensure_schema_and_tables(conn)
    finally:
        conn.close()

    tokens = TokenManager(log)
    tokens.access_token()  # fail fast on auth before fanning out
    limiter = RateLimiter(RATE_LIMIT_INTERVAL)
    counter = ApiCallCounter()

    rows_by_entity: Dict[str, int] = {}
    errors_by_entity: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                sync_entity, entity, table, realm_id, limiter, tokens,
                counter, run_ts, log,
            ): entity
            for entity, table in ENTITIES.items()
        }
        for future in as_completed(futures):
            entity = futures[future]
            try:
                rows_by_entity[entity] = future.result()
                log.info(f"{entity}: {rows_by_entity[entity]} rows")
            except Exception as e:
                errors_by_entity[entity] = str(e)
                log.warning(f"{entity} failed: {e}")

    if errors_by_entity and not rows_by_entity:
        raise RuntimeError(f"Every QBO entity failed; errors: {errors_by_entity}")

    # Stale cleanup only for entities whose fetch completed, so a partial
    # failure never deletes rows it merely failed to re-confirm.
    deleted_by_entity: Dict[str, int] = {}
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for entity, table in ENTITIES.items():
                if entity not in rows_by_entity:
                    continue
                cur.execute(
                    f"DELETE FROM {SCHEMA_NAME}.{table} WHERE _synced_at < %s",
                    (run_ts,),
                )
                if cur.rowcount:
                    deleted_by_entity[entity] = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    total_rows = sum(rows_by_entity.values())
    log.info(
        f"Sync complete: {total_rows} rows across {len(rows_by_entity)} entities, "
        f"{counter.value} API calls, {len(errors_by_entity)} entity errors, "
        f"{sum(deleted_by_entity.values())} stale rows deleted"
    )

    metadata = {
        "rows_synced": MetadataValue.int(total_rows),
        "api_calls": MetadataValue.int(counter.value),
        "entity_errors": MetadataValue.int(len(errors_by_entity)),
        "deleted_stale_rows": MetadataValue.int(sum(deleted_by_entity.values())),
        "rows_by_entity": MetadataValue.json(rows_by_entity),
    }
    if errors_by_entity:
        metadata["errors_by_entity"] = MetadataValue.json(errors_by_entity)
    if deleted_by_entity:
        metadata["deleted_by_entity"] = MetadataValue.json(deleted_by_entity)

    return Output(value=None, metadata=metadata)


defs = Definitions(
    assets=[quickbooks_online_sync],
)
