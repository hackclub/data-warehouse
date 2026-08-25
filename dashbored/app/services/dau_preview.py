"""Query program databases, warehouse, or Airtable for DAU preview data."""

from __future__ import annotations

import os
import re
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, timedelta
from urllib.parse import urlparse

import psycopg2
import requests
from psycopg2 import sql as pgsql


MAX_DAYS = 90
MAX_RECORDS = 10000
MAX_CLAIMS = 20000
CONNECT_TIMEOUT = 5
HTTP_TIMEOUT = (5, 15)
AIRTABLE_API = "https://api.airtable.com/v0"
UNCHECKED_URL = (
    "The database connection was not re-checked for this preview. Go back to the "
    "connection step and test the URL again."
)

_ADDRESS_RE = re.compile(
    r"\b\d{1,3}(?:\.\d{1,3}){3}\b|(?<=\s)\([0-9a-f:.]{2,}\)", re.IGNORECASE
)
_DSN_FIELD_RE = re.compile(
    r"(host|hostaddr|user|dbname|password|port)\s*=\s*'?([^'\s]+)", re.IGNORECASE
)
_IDENTITY_FIELDS = {"user", "dbname"}
_HOST_KV_RE = re.compile(r"\b(host|hostaddr|user|dbname)\s*=\s*'?[^'\s,)]+'?", re.IGNORECASE)
_PORT_RE = re.compile(r"\bport[\s=]+'?\d+", re.IGNORECASE)


def _dsn_parts(url: str | None, identity: bool = True) -> list[str]:
    if not url:
        return []
    parsed = urlparse(url)
    parts = {url, parsed.netloc, parsed.hostname, parsed.password}
    if identity:
        parts |= {parsed.username, parsed.path.lstrip("/")}
    parts.update(
        value
        for key, value in _DSN_FIELD_RE.findall(url)
        if identity or key.lower() not in _IDENTITY_FIELDS
    )
    return sorted((p for p in parts if p and len(p) > 2), key=len, reverse=True)


def _scrub(message: str, url: str | None = None, identity: bool = True) -> str:
    """Strip the DSN, its host/user/db and any resolved address out of a driver error.

    identity=False keeps the bare dbname and username, which the wizard user typed
    and needs to read back; the password, host, address and port always go.
    """
    for secret in _dsn_parts(url, identity):
        message = message.replace(secret, "[redacted]")
    message = _HOST_KV_RE.sub(lambda m: f"{m.group(1)}=[redacted]", message)
    message = _PORT_RE.sub("port [redacted]", message)
    return _ADDRESS_RE.sub("[redacted]", message)


def _server_said(e: psycopg2.Error) -> str | None:
    """The server's own complaint; None when the failure predates any answer.

    libpq only fills diag from a result the backend sent, so a populated
    message_primary is proof the error is about the query rather than about the
    connection string — it cannot contain the DSN.
    """
    diag = getattr(e, "diag", None)
    primary = getattr(diag, "message_primary", None)
    if not primary:
        return None
    return " ".join(
        part for part in (primary, diag.message_detail, diag.message_hint) if part
    )


def _user_db_error(prefix: str, e: psycopg2.Error, url: str) -> str:
    """A failure against the wizard user's own database, in the form they can act on.

    What the server said is theirs to read verbatim: it names their table, column
    or missing privilege, and scrubbing it by substring only mangles it — a
    password of `sessions` turns `analytics.sessions` into `analytics.[redacted]`.
    Everything else is libpq quoting the connection string back, so that gets
    scrubbed: host, address and port go, the user and database they typed stay.
    """
    said = _server_said(e)
    if said:
        return f"{prefix}: {said}"
    return f"{prefix}: {_scrub(str(e), url, identity=False)}"


def _warehouse_error(prefix: str, e: psycopg2.Error, url: str) -> str:
    """Same for our warehouse, where nothing about the DSN is the user's to see."""
    return f"{prefix}: {_scrub(_server_said(e) or str(e), url)}"


def _page_past_window(page: list[dict], field: str, start: str) -> bool:
    days = [d for d in (str(r.get("fields", {}).get(field) or "")[:10] for r in page) if d]
    return bool(days) and days[-1] < start


class DauPreviewService:
    def __init__(
        self,
        wizard_state: dict,
        airtable_token: str | None = None,
        dial_url: str | None = None,
    ):
        """dial_url is the caller's already-vetted address for the program database.

        The raw URL in the wizard state is never dialled from here: it was checked
        when it was entered, and a name that resolved somewhere public then can
        resolve somewhere internal by now.
        """
        self.state = wizard_state
        self.dau_config = wizard_state.get("dau_config", {})
        self.airtable_token = airtable_token
        self.dial_url = dial_url
        self._airtable_cache: dict[tuple, list[dict]] = {}
        self._airtable_lock = threading.Lock()
        self._warnings: set[str] = set()

    def preview(self) -> dict:
        is_airtable = self.state.get("source_type") == "airtable"

        jobs = {}
        if self.dau_config.get("has_custom_time") and self.dau_config.get("custom_table"):
            jobs["custom"] = self._query_airtable_dau if is_airtable else self._query_custom_dau
        if self.dau_config.get("uses_hackatime"):
            jobs["hackatime"] = self._query_hackatime_dau

        results = {}
        if jobs:
            with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
                futures = {name: pool.submit(fn) for name, fn in jobs.items()}
                results = {name: f.result() for name, f in futures.items()}

        blended = self._blend(results)
        if self._warnings:
            blended["truncated"] = True
            blended["warning"] = " ".join(sorted(self._warnings))
        return blended

    def _table_ident(self, key: str) -> tuple[str, str]:
        """(schema, table) for a bare or "schema.table" key from the DAU config.

        Resolved exactly the way code_generator._ident resolves it, so the chart
        measures the table the generated model will measure. A bare name is only
        split when no introspected table is spelled that way, because an Airtable
        table is allowed a dot in its name.
        """
        tables = self.state.get("schema") or []
        info = next((t for t in tables if t["name"] == key), None)
        if not info and "." in key:
            schema, name = key.split(".", 1)
            info = next(
                (
                    t for t in tables
                    if t["name"] == name and (t.get("schema") or "public") == schema
                ),
                None,
            )
            if not info:
                return schema, name
        if info:
            return info.get("schema") or "public", info["name"]
        return "public", key

    def _qualified(self, key: str) -> str:
        return "{}.{}".format(*self._table_ident(key))

    @staticmethod
    @contextmanager
    def _connect(url: str, statement_timeout: str, utc: bool = False):
        conn = psycopg2.connect(url, connect_timeout=CONNECT_TIMEOUT)
        try:
            conn.set_session(readonly=True, autocommit=True)
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s", (statement_timeout,))
                if utc:
                    cur.execute("SET TIME ZONE 'UTC'")
                yield cur
        finally:
            conn.close()

    def _query_custom_dau(self) -> list[dict]:
        url = self.dial_url
        if not url:
            return [{"error": UNCHECKED_URL}]

        table = self.dau_config["custom_table"]
        user_col = self.dau_config.get("user_column", "user_email")
        ts_col = self.dau_config.get("timestamp_column", "created_at")

        try:
            with self._connect(url, "15s") as cur:
                query = pgsql.SQL("""
                    SELECT
                        ({ts} AT TIME ZONE 'America/New_York')::date AS day,
                        COUNT(DISTINCT {user}) AS dau
                    FROM {table}
                    WHERE {ts} >= %s AND {ts} < NOW()
                    GROUP BY 1 ORDER BY 1
                """).format(
                    ts=pgsql.Identifier(ts_col),
                    user=pgsql.Identifier(user_col),
                    table=pgsql.Identifier(*self._table_ident(table)),
                )
                cur.execute(query, (self._start_date(),))
                return [{"date": str(r[0]), "dau": r[1]} for r in cur.fetchall()]
        except psycopg2.Error as e:
            return [{"error": _user_db_error("Query failed", e, url)}]

    def _query_airtable_dau(self) -> list[dict]:
        if not self.airtable_token:
            return [{"error": "No Airtable token available"}]

        base_id = self.state.get("airtable_base_id")
        if not base_id:
            return [{"error": "No Airtable base selected"}]

        table_name = self.dau_config.get("custom_table", "")
        user_col = self.dau_config.get("user_column", "")
        ts_col = self.dau_config.get("timestamp_column", "")

        if not all([table_name, user_col, ts_col]):
            return [{"error": "DAU config incomplete — need table, user column, and timestamp column"}]

        table_id = self._resolve_airtable_table_id(table_name)
        if not table_id:
            return [{"error": f"Could not find table '{table_name}' in base"}]

        try:
            records = self._airtable_records(
                base_id, table_name, table_id, [user_col, ts_col], since_field=ts_col,
            )
        except Exception as e:
            return [{"error": _scrub(f"Airtable query failed: {e}")}]

        counts = defaultdict(set)
        start = self._start_date()
        for rec in records:
            fields = rec.get("fields", {})
            user = fields.get(user_col)
            ts = fields.get(ts_col)
            if not user or not ts:
                continue
            day = str(ts)[:10]
            if day >= start:
                counts[day].add(str(user))

        return sorted(
            [{"date": day, "dau": len(users)} for day, users in counts.items()],
            key=lambda x: x["date"],
        )

    def _resolve_airtable_table_id(self, table_name: str) -> str | None:
        name = self._table_ident(table_name)[1]
        for t in self.state.get("schema", []):
            if t["name"] == name and t.get("table_id"):
                return t["table_id"]
        return None

    def _airtable_sources(self) -> list[tuple[str, list[str], str | None]]:
        """(table, fields, window column) for every Airtable read this preview needs."""
        dc = self.dau_config
        sources = []
        if dc.get("has_custom_time") and dc.get("custom_table"):
            ts_col = dc.get("timestamp_column", "")
            sources.append((dc["custom_table"], [dc.get("user_column", ""), ts_col], ts_col or None))
        if dc.get("uses_hackatime") and dc.get("ht_mapping_table") and dc.get("ht_alias_column"):
            fields = [dc["ht_alias_column"]]
            if dc.get("claim_start_source") != "fixed_date":
                fields.append(dc.get("claim_start_column", "created_at"))
            sources.append((dc["ht_mapping_table"], fields, None))
        return sources

    def _airtable_records(
        self, base_id: str, table_name: str, table_id: str, fields: list[str],
        since_field: str | None = None,
    ) -> list[dict]:
        """One pass per table, covering every source that reads it.

        The field list is built from _airtable_sources rather than from the
        caller's, so the two threads that can land here first agree on it — and
        so on the sort that decides which records survive MAX_RECORDS.
        """
        wanted = []
        best_ts = None
        for other_table, other_fields, other_since in self._airtable_sources():
            if self._table_ident(other_table)[1] == self._table_ident(table_name)[1]:
                wanted += other_fields
                if other_since is None:
                    since_field = None
                elif other_since:
                    best_ts = other_since
        wanted = [f for f in dict.fromkeys(wanted + list(fields)) if f]
        sort_field = since_field or best_ts or (wanted[0] if wanted else None)

        key = (table_id, since_field, sort_field)
        with self._airtable_lock:
            if key not in self._airtable_cache:
                self._airtable_cache[key] = self._fetch_airtable_records(
                    base_id, table_name, table_id, wanted, since_field, sort_field,
                )
            return self._airtable_cache[key]

    def _fetch_airtable_records(
        self, base_id: str, table_name: str, table_id: str, fields: list[str],
        since_field: str | None = None, sort_field: str | None = None,
    ) -> list[dict]:
        headers = {
            "Authorization": f"Bearer {self.airtable_token}",
        }
        records = []
        params = {"fields[]": fields, "pageSize": 100}
        if sort_field:
            params["sort[0][field]"] = sort_field
            params["sort[0][direction]"] = "desc" if since_field else "asc"
        url = f"{AIRTABLE_API}/{base_id}/{table_id}"
        start = self._start_date()

        while True:
            try:
                r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            except requests.RequestException as e:
                raise RuntimeError(f"Airtable request failed: {e}")
            if r.status_code != 200:
                raise RuntimeError(f"Airtable API {r.status_code}: {r.text[:200]}")
            data = r.json()
            page = data.get("records", [])
            records.extend(page)
            offset = data.get("offset")
            if not offset:
                break
            if since_field and _page_past_window(page, since_field, start):
                break
            if len(records) >= MAX_RECORDS:
                order = f"{'newest' if since_field else 'first'} by '{sort_field}'"
                self._warnings.add(
                    f"Only the first {MAX_RECORDS} records of '{table_name}' were read "
                    f"({order}) — the chart undercounts activity."
                )
                break
            params["offset"] = offset

        return records

    def _query_hackatime_dau(self) -> list[dict]:
        warehouse_url = os.environ.get("WAREHOUSE_READONLY_URL")
        if not warehouse_url:
            return [{"error": "Warehouse connection not configured (WAREHOUSE_READONLY_URL)"}]

        claims = self._fetch_alias_claims()
        if isinstance(claims, dict) and "error" in claims:
            return [claims]
        if not claims:
            return [{"error": "No hackatime alias claims found in the mapping table"}]

        aliases = [c[0] for c in claims]
        start_times = [c[1] for c in claims]

        # Claim starts arrive as UTC wall times with no zone on them — from
        # `AT TIME ZONE 'UTC'` on the program side, or bare ISO from Airtable.
        # ::timestamptz reads a zoneless literal in the session zone, so pin it.
        try:
            with self._connect(warehouse_url, "15s", utc=True) as cur:
                cur.execute("""
                    SELECT
                        (h.activity_time AT TIME ZONE 'America/New_York')::date AS day,
                        COUNT(DISTINCT h.hackatime_first_email) AS dau
                    FROM public_hackatime_analytics.hourly_project_activity h
                    JOIN unnest(%s::text[], %s::timestamptz[]) AS claims(alias, start_ts)
                        ON LOWER(h.project_name) = claims.alias
                        AND h.activity_time >= claims.start_ts
                    WHERE h.hackatime_hours > 0
                        AND h.activity_time >= %s
                        AND h.activity_time < NOW()
                    GROUP BY 1 ORDER BY 1
                """, (aliases, start_times, self._start_date()))
                return [{"date": str(r[0]), "dau": r[1]} for r in cur.fetchall()]
        except psycopg2.Error as e:
            return [{"error": _warehouse_error("Warehouse query failed", e, warehouse_url)}]

    def _fetch_alias_claims(self) -> list[tuple[str, str]] | dict:
        """Fetch (alias, claim_start_ts) pairs from the program's mapping table."""
        mapping_table = self.dau_config.get("ht_mapping_table", "")
        alias_col = self.dau_config.get("ht_alias_column", "")
        alias_format = self.dau_config.get("ht_alias_format", "single")
        claim_source = self.dau_config.get("claim_start_source", "column")
        claim_col = self.dau_config.get("claim_start_column", "created_at")
        claim_date = self.dau_config.get("claim_start_date", "")

        if not mapping_table or not alias_col:
            return {"error": "Hackatime config incomplete — need mapping table and alias column"}

        is_airtable = self.state.get("source_type") == "airtable"
        if is_airtable:
            return self._fetch_airtable_claims(
                mapping_table, alias_col, alias_format, claim_source, claim_col, claim_date,
            )
        return self._fetch_postgres_claims(
            mapping_table, alias_col, alias_format, claim_source, claim_col, claim_date,
        )

    def _fetch_postgres_claims(
        self, table, alias_col, alias_format, claim_source, claim_col, claim_date,
    ) -> list[tuple[str, str]] | dict:
        url = self.dial_url
        if not url:
            return {"error": UNCHECKED_URL}

        fixed_ts = f"{claim_date} 00:00:00+00" if claim_source == "fixed_date" and claim_date else None

        try:
            with self._connect(url, "10s") as cur:
                if fixed_ts:
                    query = pgsql.SQL("""
                        SELECT {alias}, %s::timestamptz
                        FROM {table}
                        WHERE {alias} IS NOT NULL AND {alias}::text <> ''
                        GROUP BY 1 ORDER BY 1
                        LIMIT %s
                    """).format(
                        alias=pgsql.Identifier(alias_col),
                        table=pgsql.Identifier(*self._table_ident(table)),
                    )
                    cur.execute(query, (fixed_ts, MAX_CLAIMS))
                else:
                    query = pgsql.SQL("""
                        SELECT {alias}, MIN({ts} AT TIME ZONE 'UTC')
                        FROM {table}
                        WHERE {alias} IS NOT NULL AND {alias}::text <> ''
                            AND {ts} IS NOT NULL
                        GROUP BY 1 ORDER BY 1
                        LIMIT %s
                    """).format(
                        alias=pgsql.Identifier(alias_col),
                        ts=pgsql.Identifier(claim_col),
                        table=pgsql.Identifier(*self._table_ident(table)),
                    )
                    cur.execute(query, (MAX_CLAIMS,))

                rows = cur.fetchall()
                if len(rows) >= MAX_CLAIMS:
                    self._warnings.add(
                        f"Only the first {MAX_CLAIMS} hackatime aliases from "
                        f"{self._qualified(table)}, ordered by alias, were used — the "
                        "hackatime series undercounts activity."
                    )
                return self._expand_aliases(rows, alias_format)
        except psycopg2.Error as e:
            return {"error": _user_db_error("Failed to fetch alias claims", e, url)}

    def _fetch_airtable_claims(
        self, table_name, alias_col, alias_format, claim_source, claim_col, claim_date,
    ) -> list[tuple[str, str]] | dict:
        if not self.airtable_token:
            return {"error": "No Airtable token available"}

        base_id = self.state.get("airtable_base_id")
        if not base_id:
            return {"error": "No Airtable base selected"}

        table_id = self._resolve_airtable_table_id(table_name)
        if not table_id:
            return {"error": f"Could not find table '{table_name}' in base"}

        fields = [alias_col]
        if claim_source != "fixed_date":
            fields.append(claim_col)

        try:
            records = self._airtable_records(base_id, table_name, table_id, fields)
        except Exception as e:
            return {"error": _scrub(f"Airtable query failed: {e}")}

        fixed_ts = f"{claim_date}T00:00:00Z" if claim_source == "fixed_date" and claim_date else None
        raw_pairs = [
            (rec.get("fields", {}).get(alias_col), fixed_ts or rec.get("fields", {}).get(claim_col))
            for rec in records
        ]

        return self._expand_aliases(raw_pairs, alias_format)

    @staticmethod
    def _expand_aliases(
        raw_pairs: list[tuple], alias_format: str,
    ) -> list[tuple[str, str]]:
        claims: dict[str, str] = {}
        for alias_val, ts in raw_pairs:
            if alias_val is None:
                continue
            ts_str = "" if ts is None else str(ts).strip()
            if not ts_str:
                continue

            if alias_format == "single":
                parts = [alias_val]
            elif alias_format == "csv":
                parts = str(alias_val).split(",")
            else:
                parts = alias_val if isinstance(alias_val, list) else [alias_val]

            for part in parts:
                cleaned = str(part).strip().lower()
                if not cleaned:
                    continue
                if cleaned not in claims or ts_str < claims[cleaned]:
                    claims[cleaned] = ts_str
        return list(claims.items())

    def _blend(self, results: dict) -> dict:
        if not results:
            return {"data": [], "source": "none"}

        custom = results.get("custom", [])
        hackatime = results.get("hackatime", [])

        has_custom_error = custom and isinstance(custom[0], dict) and "error" in custom[0]
        has_ht_error = hackatime and isinstance(hackatime[0], dict) and "error" in hackatime[0]

        if "custom" in results and "hackatime" in results:
            if has_custom_error:
                return {"data": hackatime, "source": "hackatime", "error": custom[0]["error"]}
            if has_ht_error:
                return {"data": custom, "source": "custom", "error": hackatime[0]["error"]}

            blended = self._merge_by_date(custom, hackatime)
            return {"data": blended, "source": "blended"}

        if "custom" in results:
            if has_custom_error:
                return {"data": [], "source": "custom", "error": custom[0]["error"]}
            return {"data": custom, "source": "custom"}

        if has_ht_error:
            return {"data": [], "source": "hackatime", "error": hackatime[0]["error"]}
        return {"data": hackatime, "source": "hackatime"}

    def _merge_by_date(self, custom: list, hackatime: list) -> list:
        by_date: dict[str, dict] = {}
        for r in custom:
            by_date[r["date"]] = {"date": r["date"], "custom": r["dau"], "hackatime": 0}
        for r in hackatime:
            if r["date"] not in by_date:
                by_date[r["date"]] = {"date": r["date"], "custom": 0, "hackatime": 0}
            by_date[r["date"]]["hackatime"] = r["dau"]

        return sorted(
            [
                {
                    "date": v["date"],
                    "dau": max(v["custom"], v["hackatime"]),
                    "custom": v["custom"],
                    "hackatime": v["hackatime"],
                }
                for v in by_date.values()
            ],
            key=lambda x: x["date"],
        )

    def _start_date(self) -> str:
        sd = self.dau_config.get("start_date")
        if sd:
            try:
                parsed = date.fromisoformat(sd)
                cutoff = date.today() - timedelta(days=MAX_DAYS)
                return str(max(parsed, cutoff))
            except ValueError:
                pass
        return str(date.today() - timedelta(days=30))
