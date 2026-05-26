import random
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional
from urllib.parse import quote

import requests
from pydantic import PrivateAttr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import dagster as dg


REPORT_PAGE_SIZE = 300
METRICS_PAGE_SIZE = 300
PAGE_DELAY = 3.5  # report endpoints are 10-20 req/min


class ZoomResource(dg.ConfigurableResource):
    account_id: str = dg.EnvVar("ZOOM_ACCOUNT_ID")
    client_id: str = dg.EnvVar("ZOOM_CLIENT_ID")
    client_secret: str = dg.EnvVar("ZOOM_CLIENT_SECRET")

    _session: Optional[requests.Session] = PrivateAttr(default=None)
    _token: Optional[str] = PrivateAttr(default=None)
    _token_expires_at: Optional[datetime] = PrivateAttr(default=None)

    @contextmanager
    def yield_for_execution(
        self, context: dg.InitResourceContext
    ) -> Iterator["ZoomResource"]:
        session = requests.Session()
        retry = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
        session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session = session
        self._token = None
        self._token_expires_at = None
        try:
            yield self
        finally:
            session.close()
            self._session = None
            self._token = None
            self._token_expires_at = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            raise RuntimeError(
                "ZoomResource used outside execution context — "
                "session is only available during asset materialization"
            )
        return self._session

    def _ensure_token(self):
        now = datetime.now(timezone.utc)
        if self._token and self._token_expires_at and now < self._token_expires_at:
            return

        resp = requests.post(
            "https://zoom.us/oauth/token",
            params={"grant_type": "account_credentials", "account_id": self.account_id},
            auth=(self.client_id, self.client_secret),
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires_at = now + timedelta(seconds=data.get("expires_in", 3600) - 60)
        self._get_session().headers["Authorization"] = f"Bearer {self._token}"

    def _request(self, method: str, path: str, *, params=None, max_attempts: int = 15, log=None) -> dict:
        url = f"https://api.zoom.us/v2{path}"
        for attempt in range(max_attempts):
            self._ensure_token()
            resp = self._get_session().request(method, url, params=params)
            if resp.status_code == 429:
                retry_after = None
                try:
                    retry_after = int(resp.headers.get("Retry-After", ""))
                except (ValueError, TypeError):
                    pass
                wait = (retry_after if retry_after else min(30 * (2 ** attempt), 300)) + random.uniform(0, 5)
                if log:
                    log.warning(f"Rate limited on {path}, waiting {wait:.1f}s (attempt {attempt + 1}/{max_attempts})")
                time.sleep(wait)
                continue
            if resp.status_code == 401:
                self._token = None
                continue
            if not resp.ok:
                if log:
                    log.error(f"{resp.status_code} on {path}: {resp.text[:500]}")
                resp.raise_for_status()
            return resp.json()
        raise requests.exceptions.HTTPError(f"Exhausted {max_attempts} retries on {url}")

    def _get(self, path: str, *, params=None, log=None) -> dict:
        return self._request("GET", path, params=params, log=log)

    @staticmethod
    def _encode_meeting_uuid(uuid: str) -> str:
        if "/" in uuid or "+" in uuid:
            return quote(quote(uuid, safe=""), safe="")
        return uuid

    @staticmethod
    def _month_chunks(start: datetime, end: datetime):
        current = start
        while current < end:
            chunk_end = min(current + timedelta(days=30), end)
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    # ── audit & activity ──

    def fetch_operation_logs(self, *, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "page_size": REPORT_PAGE_SIZE,
            }
            while True:
                data = self._get("/report/operationlogs", params=params, log=log)
                for entry in data.get("operation_logs", []):
                    yield entry
                token = data.get("next_page_token")
                if not token:
                    break
                params["next_page_token"] = token
                time.sleep(PAGE_DELAY)
            time.sleep(PAGE_DELAY)

    def fetch_sign_in_activities(self, *, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "page_size": REPORT_PAGE_SIZE,
            }
            while True:
                data = self._get("/report/activities", params=params, log=log)
                for entry in data.get("activity_logs", []):
                    yield entry
                token = data.get("next_page_token")
                if not token:
                    break
                params["next_page_token"] = token
                time.sleep(PAGE_DELAY)
            time.sleep(PAGE_DELAY)

    # ── users ──

    def fetch_users(self, *, status: str = "active", log=None):
        params = {"status": status, "page_size": REPORT_PAGE_SIZE}
        while True:
            data = self._get("/users", params=params, log=log)
            for user in data.get("users", []):
                yield user
            token = data.get("next_page_token")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(PAGE_DELAY)

    # ── daily usage ──

    def fetch_daily_usage(self, *, year: int, month: int, log=None) -> list:
        data = self._get("/report/daily", params={"year": year, "month": month}, log=log)
        return data.get("dates", [])

    # ── meetings ──

    def fetch_user_meetings(self, *, user_id: str, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "page_size": REPORT_PAGE_SIZE,
                "type": "past",
            }
            while True:
                data = self._get(f"/report/users/{user_id}/meetings", params=params, log=log)
                for meeting in data.get("meetings", []):
                    yield meeting
                token = data.get("next_page_token")
                if not token:
                    break
                params["next_page_token"] = token
                time.sleep(PAGE_DELAY)
            time.sleep(PAGE_DELAY)

    def fetch_meeting_participants(self, *, meeting_id: str, log=None):
        encoded = self._encode_meeting_uuid(meeting_id)
        params = {"page_size": REPORT_PAGE_SIZE}
        while True:
            data = self._get(f"/report/meetings/{encoded}/participants", params=params, log=log)
            for p in data.get("participants", []):
                yield p
            token = data.get("next_page_token")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(PAGE_DELAY)

    def fetch_meeting_participant_details(self, *, meeting_id: str, log=None):
        encoded = self._encode_meeting_uuid(meeting_id)
        params = {"page_size": METRICS_PAGE_SIZE, "type": "past"}
        while True:
            data = self._get(f"/metrics/meetings/{encoded}/participants", params=params, log=log)
            for p in data.get("participants", []):
                yield p
            token = data.get("next_page_token")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(PAGE_DELAY)

    def fetch_meeting_participant_qos(self, *, meeting_id: str, log=None):
        encoded = self._encode_meeting_uuid(meeting_id)
        params = {"page_size": METRICS_PAGE_SIZE, "type": "past"}
        while True:
            data = self._get(f"/metrics/meetings/{encoded}/participants/qos", params=params, log=log)
            for p in data.get("participants", []):
                yield p
            token = data.get("next_page_token")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(PAGE_DELAY)

    def fetch_meeting_sharing(self, *, meeting_id: str, log=None):
        encoded = self._encode_meeting_uuid(meeting_id)
        params = {"page_size": METRICS_PAGE_SIZE, "type": "past"}
        while True:
            data = self._get(f"/metrics/meetings/{encoded}/participants/sharing", params=params, log=log)
            for p in data.get("participants", []):
                yield p
            token = data.get("next_page_token")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(PAGE_DELAY)

    def fetch_meeting_polls(self, *, meeting_id: str, log=None) -> list:
        encoded = self._encode_meeting_uuid(meeting_id)
        data = self._get(f"/report/meetings/{encoded}/polls", log=log)
        return data.get("questions", [])

    def fetch_meeting_qa(self, *, meeting_id: str, log=None) -> list:
        encoded = self._encode_meeting_uuid(meeting_id)
        data = self._get(f"/report/meetings/{encoded}/qa", log=log)
        return data.get("questions", [])

    # ── recordings ──

    def fetch_user_recordings(self, *, user_id: str, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "page_size": REPORT_PAGE_SIZE,
            }
            while True:
                data = self._get(f"/users/{user_id}/recordings", params=params, log=log)
                for meeting in data.get("meetings", []):
                    yield meeting
                token = data.get("next_page_token")
                if not token:
                    break
                params["next_page_token"] = token
                time.sleep(PAGE_DELAY)
            time.sleep(PAGE_DELAY)

    def fetch_recording_analytics(self, *, meeting_id: str, log=None):
        encoded = self._encode_meeting_uuid(meeting_id)
        params = {"page_size": REPORT_PAGE_SIZE}
        while True:
            data = self._get(f"/meetings/{encoded}/recordings/analytics_details", params=params, log=log)
            for entry in data.get("analytics_details", []):
                yield entry
            token = data.get("next_page_token")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(PAGE_DELAY)

    def fetch_meeting_summaries(self, *, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "page_size": REPORT_PAGE_SIZE,
            }
            while True:
                data = self._get("/meetings/meeting_summaries", params=params, log=log)
                for summary in data.get("summaries", []):
                    yield summary
                token = data.get("next_page_token")
                if not token:
                    break
                params["next_page_token"] = token
                time.sleep(PAGE_DELAY)
            time.sleep(PAGE_DELAY)

    # ── usage reports ──

    def fetch_cloud_recording_usage(self, *, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
            }
            data = self._get("/report/cloud_recording", params=params, log=log)
            for user in data.get("cloud_recording_storage", []):
                yield user
            time.sleep(PAGE_DELAY)

    def fetch_telephone_report(self, *, start: datetime, end: datetime, log=None):
        for chunk_start, chunk_end in self._month_chunks(start, end):
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "page_size": REPORT_PAGE_SIZE,
            }
            while True:
                data = self._get("/report/telephone", params=params, log=log)
                for entry in data.get("telephony_usage", []):
                    yield entry
                token = data.get("next_page_token")
                if not token:
                    break
                params["next_page_token"] = token
                time.sleep(PAGE_DELAY)
            time.sleep(PAGE_DELAY)
