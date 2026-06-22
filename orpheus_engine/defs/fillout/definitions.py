"""
Fillout Sync

Discovers every form accessible by the Fillout API key and syncs all forms and
their submissions into the `fillout` PostgreSQL schema as raw JSONB.

Two tables:
  - forms: One row per FORM with its question catalog and other metadata as JSONB
    (form_id PK, name, is_published, questions JSONB, ..., _synced_at)
  - submissions: One row per SUBMISSION with the full response object as JSONB
    (form_id + submission_id composite PK, response JSONB, _synced_at)

API: https://www.fillout.com/help/fillout-rest-api
  - GET /forms                          -> list every form
  - GET /forms/{formId}                 -> form metadata (questions, etc.)
  - GET /forms/{formId}/submissions     -> paginated submissions (limit max 150)

Architecture:
  - Forms are processed concurrently via a ThreadPoolExecutor
  - Every HTTP request passes through one shared rate limiter capped below the
    Fillout account-wide limit of 5 req/s (concurrency cannot beat that cap)
  - Fetched rows are pushed into a thread-safe write queue that flushes to
    PostgreSQL in batches; a final flush writes the remainder
  - Stale rows (with _synced_at < run start) are deleted, so forms/submissions
    deleted in Fillout disappear here too
"""

import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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


SCHEMA_NAME = "fillout"
DEFAULT_BASE_URL = "https://api.fillout.com/v1/api"
SUBMISSIONS_PAGE_SIZE = 150  # Fillout max
# Fillout limits every API key to 5 req/s account-wide. Stay just under it.
RATE_LIMIT_INTERVAL = 0.21  # seconds between requests (~4.7 req/s)
MAX_WORKERS = 6  # request throughput is gated by the rate limiter, not this
FORMS_FLUSH_THRESHOLD = 100
SUBMISSIONS_FLUSH_THRESHOLD = 5_000

FORMS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.forms (
    form_id        TEXT PRIMARY KEY,
    numeric_id     BIGINT,
    name           TEXT,
    is_published   BOOLEAN,
    tags           JSONB,
    questions      JSONB,
    calculations   JSONB,
    url_parameters JSONB,
    scheduling     JSONB,
    payments       JSONB,
    documents      JSONB,
    _synced_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

SUBMISSIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.submissions (
    form_id         TEXT NOT NULL,
    submission_id   TEXT NOT NULL,
    submission_time TIMESTAMP WITH TIME ZONE,
    last_updated_at TIMESTAMP WITH TIME ZONE,
    started_at      TIMESTAMP WITH TIME ZONE,
    response        JSONB,
    _synced_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (form_id, submission_id)
);
"""


def get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def ensure_schema_and_tables(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(SCHEMA_NAME)
        ))
        cur.execute(FORMS_DDL)
        cur.execute(SUBMISSIONS_DDL)
    conn.commit()


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


def build_session() -> requests.Session:
    token = os.getenv("FILLOUT_API_KEY")
    if not token:
        raise ValueError("FILLOUT_API_KEY environment variable is not set")
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    # Transient 5xx are retried by urllib3; 429 is handled by fetch_with_retry
    # so it can honor the longer Retry-After wait Fillout asks for.
    retry = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_base_url() -> str:
    """Return the Fillout API base URL, allowing EU/self-hosted endpoints."""
    base_url = os.getenv("FILLOUT_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    if base_url.endswith("/v1/api"):
        return base_url
    return f"{base_url}/v1/api"


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


def fillout_get(session: requests.Session, limiter: RateLimiter, path: str,
                params: Optional[Dict[str, Any]] = None) -> Any:
    """Rate-limited GET against the Fillout API, returning parsed JSON."""
    limiter.acquire()
    resp = session.get(f"{get_base_url()}{path}", params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _j(value: Any) -> str:
    """JSON-encode for a JSONB column, stripping NUL bytes Postgres rejects."""
    return json.dumps(value).replace("\\u0000", "")


def _dedupe_by_pk(rows: List[Tuple], key_len: int) -> List[Tuple]:
    """Collapse rows sharing the same primary key (the first `key_len` elements),
    keeping the last occurrence. Postgres rejects an ON CONFLICT batch that touches
    the same key twice, and Fillout can return duplicate submission ids across
    pages, so we dedupe before every upsert."""
    seen: Dict[Tuple, Tuple] = {}
    for row in rows:
        seen[row[:key_len]] = row
    return list(seen.values())


def _submission_row(form_id: str, sub: Dict[str, Any]) -> Tuple:
    """Build a fillout.submissions row tuple (sans _synced_at) from a raw
    submission object. Timestamps stay as ISO strings; Postgres casts them."""
    return (
        form_id,
        sub.get("submissionId"),
        sub.get("submissionTime"),
        sub.get("lastUpdatedAt"),
        sub.get("startedAt"),
        _j(sub),
    )


def _metadata_json(meta: Optional[Dict[str, Any]], key: str) -> Optional[str]:
    if meta is None:
        return None
    return _j(meta.get(key, []))


def _form_row(form: Dict[str, Any], meta: Optional[Dict[str, Any]]) -> Tuple:
    return (
        form.get("formId"),
        form.get("id"),
        form.get("name"),
        form.get("isPublished"),
        _j(form.get("tags", [])),
        _metadata_json(meta, "questions"),
        _metadata_json(meta, "calculations"),
        _metadata_json(meta, "urlParameters"),
        _metadata_json(meta, "scheduling"),
        _metadata_json(meta, "payments"),
        _metadata_json(meta, "documents"),
    )


def fetch_all_forms(session, limiter, log) -> List[Dict]:
    """Fetch the full form list. /forms is not documented as paginated, but
    page defensively in case a limit/offset surfaces for large accounts."""
    forms = fetch_with_retry(lambda: fillout_get(session, limiter, "/forms"), log=log)
    if not isinstance(forms, list):
        raise ValueError(f"Unexpected /forms response shape: {type(forms)}")
    return forms


class WriteQueue:
    """Thread-safe queue that accumulates rows and flushes to PostgreSQL in batches."""

    def __init__(self, run_ts: datetime, log):
        self._lock = threading.Lock()
        self._forms_buf: List[Tuple] = []
        self._subs_buf: List[Tuple] = []
        self._run_ts = run_ts
        self._log = log
        self._total_forms_flushed = 0
        self._total_subs_flushed = 0

    def push_form(self, row: Tuple):
        with self._lock:
            self._forms_buf.append(row)
            if len(self._forms_buf) >= FORMS_FLUSH_THRESHOLD:
                self._flush_forms()

    def push_submissions(self, rows: List[Tuple]):
        if not rows:
            return
        with self._lock:
            self._subs_buf.extend(rows)
            if len(self._subs_buf) >= SUBMISSIONS_FLUSH_THRESHOLD:
                self._flush_submissions()

    def flush_all(self):
        with self._lock:
            self._flush_forms()
            self._flush_submissions()

    def _flush_forms(self):
        if not self._forms_buf:
            return
        rows = _dedupe_by_pk(self._forms_buf, key_len=1)
        self._forms_buf = []
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {SCHEMA_NAME}.forms AS target
                        (form_id, numeric_id, name, is_published, tags, questions,
                         calculations, url_parameters, scheduling, payments, documents, _synced_at)
                    VALUES %s
                    ON CONFLICT (form_id) DO UPDATE SET
                        numeric_id = EXCLUDED.numeric_id,
                        name = EXCLUDED.name,
                        is_published = EXCLUDED.is_published,
                        tags = EXCLUDED.tags,
                        questions = COALESCE(EXCLUDED.questions, target.questions),
                        calculations = COALESCE(EXCLUDED.calculations, target.calculations),
                        url_parameters = COALESCE(EXCLUDED.url_parameters, target.url_parameters),
                        scheduling = COALESCE(EXCLUDED.scheduling, target.scheduling),
                        payments = COALESCE(EXCLUDED.payments, target.payments),
                        documents = COALESCE(EXCLUDED.documents, target.documents),
                        _synced_at = EXCLUDED._synced_at
                    """,
                    [r + (self._run_ts,) for r in rows],
                    template="(%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s)",
                    page_size=100,
                )
            conn.commit()
            self._total_forms_flushed += len(rows)
            self._log.info(f"Flushed {len(rows)} forms (total: {self._total_forms_flushed})")
        finally:
            conn.close()

    def _flush_submissions(self):
        if not self._subs_buf:
            return
        rows = _dedupe_by_pk(self._subs_buf, key_len=2)
        self._subs_buf = []
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {SCHEMA_NAME}.submissions
                        (form_id, submission_id, submission_time, last_updated_at,
                         started_at, response, _synced_at)
                    VALUES %s
                    ON CONFLICT (form_id, submission_id) DO UPDATE SET
                        submission_time = EXCLUDED.submission_time,
                        last_updated_at = EXCLUDED.last_updated_at,
                        started_at = EXCLUDED.started_at,
                        response = EXCLUDED.response,
                        _synced_at = EXCLUDED._synced_at
                    """,
                    [r + (self._run_ts,) for r in rows],
                    template="(%s, %s, %s, %s, %s, %s::jsonb, %s)",
                    page_size=500,
                )
            conn.commit()
            self._total_subs_flushed += len(rows)
            self._log.info(f"Flushed {len(rows)} submissions (total: {self._total_subs_flushed})")
        finally:
            conn.close()


def _stream_submissions(session, limiter, form_id: str, queue: WriteQueue, log) -> int:
    """Page through a form's submissions into the write queue. A mid-form retry
    may re-push a page, but the (form_id, submission_id) upsert is idempotent."""
    offset = 0
    total = 0
    while True:
        data = fetch_with_retry(
            lambda o=offset: fillout_get(
                session, limiter, f"/forms/{form_id}/submissions",
                {"limit": SUBMISSIONS_PAGE_SIZE, "offset": o},
            ),
            log=log,
        )
        responses = data.get("responses", []) if isinstance(data, dict) else []
        rows = [
            _submission_row(form_id, sub)
            for sub in responses
            if sub.get("submissionId")
        ]
        queue.push_submissions(rows)
        total += len(rows)
        if len(responses) < SUBMISSIONS_PAGE_SIZE:
            break
        offset += SUBMISSIONS_PAGE_SIZE
    return total


def process_form(form: Dict, limiter, queue: WriteQueue, log) -> Dict[str, int]:
    """Fetch one form's metadata + submissions and push everything to the queue."""
    form_id = form.get("formId")
    stats = {"submissions": 0, "metadata_errors": 0}

    # Requests sessions are mutable; keep one session scoped to each worker task.
    with build_session() as session:
        # Metadata (question catalog etc.). On failure still record the form from
        # the list fields so it isn't deleted as stale.
        meta: Optional[Dict[str, Any]] = {}
        try:
            meta = fetch_with_retry(
                lambda: fillout_get(session, limiter, f"/forms/{form_id}"), log=log
            ) or {}
        except Exception as e:
            log.warning(f"[{form.get('name')}] ({form_id}) metadata fetch failed: {e}")
            meta = None
            stats["metadata_errors"] = 1

        queue.push_form(_form_row(form, meta))

        stats["submissions"] = _stream_submissions(session, limiter, form_id, queue, log)
    return stats


def _can_delete_stale_rows(form_errors: int, skipped_forms: int) -> bool:
    return form_errors == 0 and skipped_forms == 0


def delete_stale_rows(conn, run_ts: datetime) -> Tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA_NAME}.submissions WHERE _synced_at < %s", (run_ts,))
        deleted_subs = cur.rowcount
        cur.execute(f"DELETE FROM {SCHEMA_NAME}.forms WHERE _synced_at < %s", (run_ts,))
        deleted_forms = cur.rowcount
    conn.commit()
    return deleted_forms, deleted_subs


@asset(
    group_name="fillout",
    compute_kind="fillout",
    description=(
        "Syncs all Fillout forms and their submissions into the `fillout` schema "
        "as raw JSONB (full refresh with staleness deletion)."
    ),
)
def fillout_sync(context: AssetExecutionContext) -> Output[None]:
    """
    1. Lists every form via GET /forms
    2. For each form, fetches metadata and paginates all submissions
       (all requests funneled through a shared <5 req/s rate limiter)
    3. Upserts into fillout.forms and fillout.submissions, stamped with run_ts
    4. Deletes any rows with _synced_at < run_ts (forms/submissions gone from Fillout)
    """
    log = context.log
    run_ts = datetime.now(timezone.utc)

    limiter = RateLimiter(RATE_LIMIT_INTERVAL)

    conn = get_db_connection()
    try:
        ensure_schema_and_tables(conn)
    finally:
        conn.close()

    log.info("Fetching form list...")
    with build_session() as session:
        forms = fetch_all_forms(session, limiter, log)
    log.info(f"Discovered {len(forms)} forms")
    forms_to_process = [form for form in forms if form.get("formId")]
    skipped_forms = len(forms) - len(forms_to_process)
    if skipped_forms:
        log.warning(f"Skipping {skipped_forms} forms without formId")

    queue = WriteQueue(run_ts, log)
    total_submissions = 0
    form_errors = 0
    metadata_errors = 0
    forms_done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_form, form, limiter, queue, log): form
            for form in forms_to_process
        }
        for future in as_completed(futures):
            form = futures[future]
            forms_done += 1
            try:
                stats = future.result()
                total_submissions += stats["submissions"]
                metadata_errors += stats["metadata_errors"]
            except Exception as e:
                log.warning(f"Form {form.get('name')} ({form.get('formId')}) failed: {e}")
                form_errors += 1

            if forms_done % 100 == 0:
                log.info(
                    f"Progress: {forms_done}/{len(forms)} forms, "
                    f"{total_submissions} submissions"
                )

    log.info("Final flush of remaining queued rows...")
    queue.flush_all()

    deleted_forms = 0
    deleted_subs = 0
    if _can_delete_stale_rows(form_errors, skipped_forms):
        log.info("Cleaning up stale data...")
        conn = get_db_connection()
        try:
            deleted_forms, deleted_subs = delete_stale_rows(conn, run_ts)
        finally:
            conn.close()
        log.info(f"Deleted {deleted_forms} stale forms, {deleted_subs} stale submissions")
    else:
        log.warning(
            "Skipping stale cleanup because the Fillout sync was incomplete "
            f"({form_errors} form errors, {skipped_forms} skipped forms)"
        )

    log.info(
        f"Sync complete: {len(forms)} forms, {total_submissions} submissions, "
        f"{form_errors} form errors, {metadata_errors} metadata errors"
    )

    return Output(
        value=None,
        metadata={
            "forms": MetadataValue.int(len(forms)),
            "submissions": MetadataValue.int(total_submissions),
            "form_errors": MetadataValue.int(form_errors),
            "metadata_errors": MetadataValue.int(metadata_errors),
            "skipped_forms": MetadataValue.int(skipped_forms),
            "deleted_forms": MetadataValue.int(deleted_forms),
            "deleted_submissions": MetadataValue.int(deleted_subs),
        },
    )


defs = Definitions(
    assets=[fillout_sync],
)
