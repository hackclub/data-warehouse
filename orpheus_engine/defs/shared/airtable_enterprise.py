import random
import time
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional

import requests
from pydantic import PrivateAttr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import dagster as dg


PAGE_SIZE = 1000
PAGE_DELAY = 0.5


class AirtableEnterpriseResource(dg.ConfigurableResource):
    api_key: str = dg.EnvVar("AIRTABLE_ENTERPRISE_PAT")
    enterprise_account_id: str = dg.EnvVar("AIRTABLE_ENTERPRISE_ACCOUNT_ID")

    _session: Optional[requests.Session] = PrivateAttr(default=None)

    @contextmanager
    def yield_for_execution(
        self, context: dg.InitResourceContext
    ) -> Iterator["AirtableEnterpriseResource"]:
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {self.api_key}"
        retry = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
        session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session = session
        try:
            yield self
        finally:
            session.close()
            self._session = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            raise RuntimeError(
                "AirtableEnterpriseResource used outside execution context — "
                "session is only available during asset materialization"
            )
        return self._session

    def _base_url(self, path: str = "") -> str:
        return f"https://api.airtable.com/v0/meta/enterpriseAccounts/{self.enterprise_account_id}{path}"

    def _request_with_retry(self, url: str, params, *, max_attempts: int = 30, log=None) -> dict:
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
                    )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        raise requests.exceptions.HTTPError(f"Exhausted {max_attempts} retries on {url}")

    def fetch_events(self, *, start_time: Optional[str] = None, log=None):
        url = self._base_url("/auditLogEvents")
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

    def fetch_enterprise_user_ids(self, *, log=None) -> List[str]:
        if log:
            log.info("Fetching enterprise account user IDs...")
        data = self._request_with_retry(self._base_url(), {}, log=log)
        user_ids = data.get("userIds", [])
        if log:
            log.info(f"Found {len(user_ids)} user IDs")
        return user_ids

    def fetch_users(self, user_ids: List[str], *, batch_size: int = 100, log=None):
        url = self._base_url("/users")
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i:i + batch_size]
            params = [("id", uid) for uid in batch]
            if log:
                log.info(f"Fetching users {i+1}-{i+len(batch)} of {len(user_ids)}...")
            data = self._request_with_retry(url, params, log=log)
            yield from data.get("users", [])
            if i + batch_size < len(user_ids):
                time.sleep(PAGE_DELAY)
