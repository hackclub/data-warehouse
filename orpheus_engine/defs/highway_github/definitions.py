"""
Highway GitHub Commit Scrape

Highway (the Summer 2025 hardware/PCB program, ran ~May-Oct 2025) kept its
build journals as JOURNAL.md files inside each participant's GitHub repo, so
the only daily-grained activity trace for the program is the commit history of
the submitted repos. The program's app DB and Airtable mirror are rsvps /
final-submission totals only.

This asset:
  1. Reads the distinct GitHub repos from Highway project submissions already
     mirrored in airtable_raw_all_bases.records (base appuDQSHCdCHyOrxw,
     table tbl9QnZ320NTGJHJj).
  2. Fetches each repo's default-branch commit history via gh-proxy
     (same methodology as the unified_ysws_db stars fetcher), bounded to the
     program era (2025-03-01 .. 2026-01-01).
  3. Upserts into the `highway_github` schema:
       - repos:   one row per repo with scrape status (ok/not_found/empty/error)
       - commits: one row per (repo, sha) with author timestamp/identity

The program has ended, so this is a one-time backfill: the asset is excluded
from materialize_all_assets_job and only runs on demand.
"""

import asyncio
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dagster import (
    AssetExecutionContext,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

SCHEMA_NAME = "highway_github"

HIGHWAY_BASE_ID = "appuDQSHCdCHyOrxw"
HIGHWAY_PROJECTS_TABLE_ID = "tbl9QnZ320NTGJHJj"

# Generous bounds around the program (first submissions 2025-05, last
# stragglers 2025-11); the dbt model applies the real program window.
COMMITS_SINCE = "2025-03-01T00:00:00Z"
COMMITS_UNTIL = "2026-01-01T00:00:00Z"

GH_PROXY_BASE = "https://gh-proxy.hackclub.com/gh"
PER_PAGE = 100
MAX_PAGES_PER_REPO = 50  # 5k commits in-window — far above any real project
CONCURRENT_REPOS = 20
REQUEST_RETRIES = 5

REPOS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.repos (
    repo_key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    scrape_status TEXT NOT NULL,
    http_status INTEGER,
    commit_count INTEGER,
    _synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.commits (
    repo_key TEXT NOT NULL,
    sha TEXT NOT NULL,
    authored_at TIMESTAMP WITH TIME ZONE,
    committed_at TIMESTAMP WITH TIME ZONE,
    author_login TEXT,
    author_email TEXT,
    message_head TEXT,
    _synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (repo_key, sha)
);
"""

GITHUB_REPO_RE = re.compile(r"github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)", re.IGNORECASE)


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
        cur.execute(REPOS_DDL)
        cur.execute(COMMITS_DDL)
    conn.commit()


def parse_repo_key(github_url: str) -> Optional[Tuple[str, str]]:
    """Extract (owner, repo) from a GitHub URL, dropping .git and deep paths."""
    match = GITHUB_REPO_RE.search(github_url or "")
    if not match:
        return None
    owner = match.group(1).lower()
    repo = match.group(2).lower()
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not repo or owner in ("orgs", "topics", "search"):
        return None
    return owner, repo


def load_highway_repos(conn) -> List[Tuple[str, str]]:
    """Distinct (owner, repo) from Highway project submissions in the
    airtable_raw_all_bases mirror. All statuses included — inclusion rules
    (for example excluding purged submissions) belong to the dbt layer."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT fields->>'Github_Url'
            FROM airtable_raw_all_bases.records
            WHERE base_id = %s AND table_id = %s
              AND COALESCE(fields->>'Github_Url', '') <> ''
            """,
            (HIGHWAY_BASE_ID, HIGHWAY_PROJECTS_TABLE_ID),
        )
        urls = [row[0] for row in cur.fetchall()]
    repos = {parse_repo_key(u) for u in urls}
    repos.discard(None)
    return sorted(repos)


async def fetch_repo_commits(
    session: aiohttp.ClientSession,
    owner: str,
    repo: str,
    api_key: str,
    log,
) -> Dict:
    """Fetch one repo's in-window default-branch commit history, paginated."""
    repo_key = f"{owner}/{repo}"
    commits: List[Dict] = []

    for page in range(1, MAX_PAGES_PER_REPO + 1):
        url = (
            f"{GH_PROXY_BASE}/repos/{owner}/{repo}/commits"
            f"?per_page={PER_PAGE}&page={page}"
            f"&since={COMMITS_SINCE}&until={COMMITS_UNTIL}"
        )

        data = None
        for attempt in range(REQUEST_RETRIES):
            try:
                async with session.get(
                    url,
                    headers={"X-API-Key": api_key, "Accept": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        break
                    if response.status == 404:
                        return {"repo_key": repo_key, "status": "not_found",
                                "http_status": 404, "commits": []}
                    # 409 = empty git repository
                    if response.status == 409:
                        return {"repo_key": repo_key, "status": "empty",
                                "http_status": 409, "commits": []}
                    if response.status in (403, 429, 500, 502, 503, 504):
                        wait = min(2 ** attempt * 2, 30)
                        log.warning(f"{repo_key} page {page}: HTTP {response.status}, retrying in {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    return {"repo_key": repo_key, "status": "error",
                            "http_status": response.status, "commits": []}
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait = min(2 ** attempt * 2, 30)
                log.warning(f"{repo_key} page {page}: {e}, retrying in {wait}s")
                await asyncio.sleep(wait)
        else:
            return {"repo_key": repo_key, "status": "error",
                    "http_status": None, "commits": commits}

        for item in data:
            commit = item.get("commit", {}) or {}
            author = commit.get("author", {}) or {}
            committer = commit.get("committer", {}) or {}
            gh_author = item.get("author") or {}
            message = (commit.get("message") or "").split("\n", 1)[0][:200]
            commits.append({
                "sha": item.get("sha"),
                "authored_at": author.get("date"),
                "committed_at": committer.get("date"),
                "author_login": gh_author.get("login"),
                "author_email": author.get("email"),
                "message_head": message,
            })

        if len(data) < PER_PAGE:
            break
    else:
        log.warning(f"{repo_key}: hit MAX_PAGES_PER_REPO ({MAX_PAGES_PER_REPO}), history truncated")

    return {"repo_key": repo_key, "status": "ok", "http_status": 200, "commits": commits}


async def fetch_all_repos(repos: List[Tuple[str, str]], api_key: str, log) -> List[Dict]:
    results: List[Dict] = []
    semaphore = asyncio.Semaphore(CONCURRENT_REPOS)

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=CONCURRENT_REPOS * 2),
    ) as session:

        async def bounded_fetch(owner: str, repo: str) -> Dict:
            async with semaphore:
                return await fetch_repo_commits(session, owner, repo, api_key, log)

        tasks = [bounded_fetch(owner, repo) for owner, repo in repos]
        done = 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            done += 1
            if done % 100 == 0:
                log.info(f"Scraped {done}/{len(repos)} repos...")

    return results


def upsert_results(conn, results: List[Dict], run_ts: datetime) -> Tuple[int, int]:
    repo_rows = []
    commit_rows = []
    refreshed_commit_repos = []
    for r in results:
        owner, repo = r["repo_key"].split("/", 1)
        repo_rows.append((r["repo_key"], owner, repo, r["status"],
                          r["http_status"], len(r["commits"]), run_ts))
        if r["status"] in ("ok", "not_found", "empty"):
            refreshed_commit_repos.append((r["repo_key"],))
        for c in r["commits"]:
            if not c["sha"]:
                continue
            commit_rows.append((r["repo_key"], c["sha"], c["authored_at"],
                                c["committed_at"], c["author_login"],
                                c["author_email"], c["message_head"], run_ts))

    with conn.cursor() as cur:
        if refreshed_commit_repos:
            execute_values(
                cur,
                f"""
                DELETE FROM {SCHEMA_NAME}.commits
                WHERE repo_key IN (
                    SELECT repo_key FROM (VALUES %s) AS refreshed(repo_key)
                )
                """,
                refreshed_commit_repos,
                page_size=500,
            )
        if repo_rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA_NAME}.repos
                    (repo_key, owner, repo, scrape_status, http_status, commit_count, _synced_at)
                VALUES %s
                ON CONFLICT (repo_key) DO UPDATE SET
                    scrape_status = EXCLUDED.scrape_status,
                    http_status = EXCLUDED.http_status,
                    commit_count = EXCLUDED.commit_count,
                    _synced_at = EXCLUDED._synced_at
                """,
                repo_rows,
                page_size=500,
            )
        if commit_rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA_NAME}.commits
                    (repo_key, sha, authored_at, committed_at, author_login,
                     author_email, message_head, _synced_at)
                VALUES %s
                ON CONFLICT (repo_key, sha) DO UPDATE SET
                    authored_at = EXCLUDED.authored_at,
                    committed_at = EXCLUDED.committed_at,
                    author_login = EXCLUDED.author_login,
                    author_email = EXCLUDED.author_email,
                    message_head = EXCLUDED.message_head,
                    _synced_at = EXCLUDED._synced_at
                """,
                commit_rows,
                page_size=1000,
            )
    conn.commit()
    return len(repo_rows), len(commit_rows)


@asset(
    compute_kind="github_api",
    group_name="highway_github",
    description=(
        "One-time backfill of commit history for Highway-submitted GitHub repos "
        "via gh-proxy, into the highway_github schema (repos + commits). "
        "Highway ended Oct 2025 — run manually, not on the schedule."
    ),
)
def highway_github_commits(context: AssetExecutionContext) -> Output[None]:
    log = context.log

    api_key = os.getenv("GH_PROXY_API_KEY")
    if not api_key:
        raise ValueError("GH_PROXY_API_KEY environment variable is required")

    conn = get_db_connection()
    try:
        ensure_schema_and_tables(conn)
        repos = load_highway_repos(conn)
    finally:
        conn.close()
    log.info(f"Found {len(repos)} distinct Highway repos to scrape")

    run_ts = datetime.now(timezone.utc)
    start = time.time()
    results = asyncio.run(fetch_all_repos(repos, api_key, log))
    log.info(f"Scrape finished in {time.time() - start:.0f}s")

    status_counts: Dict[str, int] = {}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    log.info(f"Scrape statuses: {status_counts}")

    conn = get_db_connection()
    try:
        n_repos, n_commits = upsert_results(conn, results, run_ts)
    finally:
        conn.close()
    log.info(f"Upserted {n_repos} repos, {n_commits} commits")

    return Output(
        value=None,
        metadata={
            "repos": MetadataValue.int(n_repos),
            "commits": MetadataValue.int(n_commits),
            "ok": MetadataValue.int(status_counts.get("ok", 0)),
            "not_found": MetadataValue.int(status_counts.get("not_found", 0)),
            "empty": MetadataValue.int(status_counts.get("empty", 0)),
            "errors": MetadataValue.int(status_counts.get("error", 0)),
        },
    )


defs = Definitions(
    assets=[highway_github_commits],
)
