"""
YSWS True Spend static site.

Reads the true-spend dbt models out of the warehouse, renders a plain static
site (see site.py), and commits the result onto the `main` branch of
https://github.com/hackclub/ysws-true-spend, which serves it on GitHub Pages.

Nothing in the target repo is hand-maintained: every run replaces the entire
tracked tree with what the renderer emitted, and a commit only happens when the
rendered bytes actually changed, so a 6-hourly cadence does not spam empty
commits. Anything added to the repo by hand -- including the GitHub Pages CNAME
-- is deleted on the next run, so it has to be emitted by the renderer instead.

Requires a GitHub token with push access to the target repo in
YSWS_TRUE_SPEND_GITHUB_TOKEN (fine-grained: Contents read/write on
hackclub/ysws-true-spend).
"""

import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    Definitions,
    MetadataValue,
    Output,
    asset,
)

from .data import fetch_site_data
from .freshness import dagster_times_from_db, dagster_times_from_instance
from .site import render_site

DEFAULT_REPO = "hackclub/ysws-true-spend"
DEFAULT_BRANCH = "main"
TOKEN_ENV_VAR = "YSWS_TRUE_SPEND_GITHUB_TOKEN"
# The custom domain the renderer publishes in CNAME (see site.CUSTOM_DOMAIN).
# While the repo is private, Pages also serves the same tree at a random
# *.pages.github.io host.
PAGES_URL = "https://ysws-true-spend.hackclub.com/"

# Identity on every commit this asset pushes to the site repo.
COMMIT_NAME = "Hack Club Data Warehouse"
COMMIT_EMAIL = "data-warehouse@hackclub.com"

# dbt asset keys carry the model's custom schema as a prefix
# (hcb_ysws_true_spend_analytics/ysws_spend_ledger), so spell them out rather
# than relying on the bare model name, which would silently create a phantom
# upstream asset instead of wiring the real dependency.
UPSTREAM_ASSETS = [
    AssetKey(["hcb_ysws_true_spend_analytics", "ysws_spend_by_program"]),
    AssetKey(["hcb_ysws_true_spend_analytics", "ysws_spend_org_tree"]),
    AssetKey(["hcb_ysws_true_spend_analytics", "ysws_spend_ledger"]),
    AssetKey(["hcb_analytics", "ledger"]),
    AssetKey(["hcb_analytics", "orgs"]),
]


class YswsTrueSpendSiteConfig(Config):
    """Set publish=False to render and report without touching the repo."""

    publish: bool = True


def _get_db_connection():
    conn_string = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not conn_string:
        raise ValueError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return psycopg2.connect(conn_string)


def _redact(text: str, secrets: List[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def _git(
    args: List[str],
    cwd: Path,
    secrets: Optional[List[str]] = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Run git, keeping the token out of logs and exceptions."""
    secrets = secrets or []
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            "git "
            + " ".join(_redact(a, secrets) for a in args)
            + f" failed ({result.returncode}): "
            + _redact((result.stderr or result.stdout).strip(), secrets)
        )
    return result


def _write_files(root: Path, files: Dict[str, Any]) -> None:
    """Text files as UTF-8; the DuckDB database is bytes."""
    for rel_path, content in files.items():
        target = root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding="utf-8")


def _clear_worktree(root: Path) -> None:
    """Drop every tracked/untracked file except .git, so removals propagate."""
    for entry in root.iterdir():
        if entry.name == ".git":
            continue
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def publish_site(
    files: Dict[str, str],
    commit_message: str,
    repo: str = DEFAULT_REPO,
    branch: str = DEFAULT_BRANCH,
    token: Optional[str] = None,
    log=None,
) -> Dict[str, Any]:
    """Commit `files` as the entire content of `repo`@`branch`. Returns a summary."""
    token = token or os.getenv(TOKEN_ENV_VAR)
    if not token:
        raise ValueError(
            f"{TOKEN_ENV_VAR} is not set. Create a fine-grained GitHub token with "
            f"Contents: read and write on {repo} and add it to the Dagster "
            "environment (Coolify) to publish the site."
        )

    remote = f"https://x-access-token:{token}@github.com/{repo}.git"
    secrets = [token, remote]

    with tempfile.TemporaryDirectory(prefix="ysws-true-spend-") as tmp:
        root = Path(tmp)
        _git(["init", "--quiet", "--initial-branch", branch], root, secrets)
        _git(["remote", "add", "origin", remote], root, secrets)
        _git(["config", "user.name", COMMIT_NAME], root, secrets)
        _git(["config", "user.email", COMMIT_EMAIL], root, secrets)

        # An empty repo has no branch to fetch; that is a valid first run.
        fetched = _git(
            ["fetch", "--depth", "1", "origin", branch], root, secrets, check=False
        )
        if fetched.returncode == 0:
            _git(["reset", "--hard", "--quiet", "FETCH_HEAD"], root, secrets)
        elif log:
            log.info(
                f"No existing {branch} on {repo} (or it is empty) - creating the "
                "first commit."
            )

        _clear_worktree(root)
        _write_files(root, files)
        _git(["add", "--all"], root, secrets)

        status = _git(["status", "--porcelain"], root, secrets)
        changed = [line for line in status.stdout.splitlines() if line.strip()]
        if not changed:
            head = _git(["rev-parse", "HEAD"], root, secrets).stdout.strip()
            return {
                "committed": False,
                "commit_sha": head,
                "changed_files": 0,
                "file_count": len(files),
            }

        _git(["commit", "--quiet", "-m", commit_message], root, secrets)
        _git(["push", "--quiet", "origin", f"HEAD:refs/heads/{branch}"], root, secrets)
        head = _git(["rev-parse", "HEAD"], root, secrets).stdout.strip()
        return {
            "committed": True,
            "commit_sha": head,
            "changed_files": len(changed),
            "file_count": len(files),
        }


def build_site_files(
    generated_at: Optional[datetime] = None,
    dagster_instance: Any = None,
    dagster_db_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query the warehouse and render the site. Returns files + counts.

    The two freshness timestamps the site shows are Dagster materialization
    facts, so pass the running instance (in the asset) or a Dagster database URL
    (when rendering a preview outside Dagster). With neither, the site says the
    pull and recalculation times are unknown rather than inventing them.
    """
    generated_at = generated_at or datetime.now(timezone.utc)
    conn = _get_db_connection()
    try:
        data = fetch_site_data(conn)
    finally:
        conn.close()

    if data.freshness is not None:
        if dagster_instance is not None:
            pulled, recalculated = dagster_times_from_instance(dagster_instance)
        elif dagster_db_url:
            pulled, recalculated = dagster_times_from_db(dagster_db_url)
        else:
            pulled, recalculated = None, None
        data.freshness.hcb_pulled_at = pulled
        data.freshness.recalculated_at = recalculated

    files = render_site(data, generated_at)
    total_spend = sum(
        (Decimal(str(p["true_spend_dollars"] or 0)) for p in data.programs), Decimal(0)
    )
    total_revenue = sum(
        (Decimal(str(p["external_revenue_dollars"] or 0)) for p in data.programs),
        Decimal(0),
    )
    return {
        "files": files,
        "generated_at": generated_at,
        "program_count": len(data.programs),
        "org_count": sum(len(v) for v in data.orgs_by_program.values()),
        "spend_transaction_count": sum(len(v) for v in data.spend_by_program.values()),
        "revenue_transaction_count": sum(
            len(v) for v in data.revenue_by_program.values()
        ),
        "total_true_spend_dollars": total_spend,
        "total_external_revenue_dollars": total_revenue,
        "bytes": sum(
            len(c) if isinstance(c, bytes) else len(c.encode("utf-8"))
            for c in files.values()
        ),
        "unmatched_orgs": len(data.unmatched_orgs),
        "unlinked_programs": len(data.unlinked_programs),
        "freshness": data.freshness,
    }


@asset(
    name="ysws_true_spend_site",
    compute_kind="static_site",
    group_name="ysws_true_spend_site",
    deps=UPSTREAM_ASSETS,
    description=(
        "Renders the YSWS true-spend static site (program tree, per-program "
        "transaction pages, JSON) from the true-spend dbt models and pushes it to "
        f"the {DEFAULT_BRANCH} branch of {DEFAULT_REPO}."
    ),
)
def ysws_true_spend_site(
    context: AssetExecutionContext, config: YswsTrueSpendSiteConfig
) -> Output[None]:
    built = build_site_files(dagster_instance=context.instance)
    files = built["files"]
    context.log.info(
        f"Rendered {len(files)} files ({built['bytes'] / 1_000_000:.1f} MB) for "
        f"{built['program_count']} programs, {built['org_count']} orgs, "
        f"{built['spend_transaction_count']} spend and "
        f"{built['revenue_transaction_count']} revenue transactions."
    )

    fresh = built["freshness"]
    if fresh is not None and fresh.hcb_pulled_at is not None:
        stale_hours = (built["generated_at"] - fresh.hcb_pulled_at).total_seconds() / 3600
        if stale_hours > 36:
            context.log.warning(
                f"HCB mirror last succeeded {stale_hours / 24:.1f} days ago "
                f"({fresh.hcb_pulled_at:%Y-%m-%d %H:%M UTC}); every published number "
                "is that old. The site says so on its front page."
            )

    metadata: Dict[str, Any] = {
        "programs": MetadataValue.int(built["program_count"]),
        "orgs": MetadataValue.int(built["org_count"]),
        "spend_transactions": MetadataValue.int(built["spend_transaction_count"]),
        "revenue_transactions": MetadataValue.int(built["revenue_transaction_count"]),
        "total_true_spend": MetadataValue.float(float(built["total_true_spend_dollars"])),
        "total_external_revenue": MetadataValue.float(
            float(built["total_external_revenue_dollars"])
        ),
        "unmatched_orgs": MetadataValue.int(built["unmatched_orgs"]),
        "unlinked_programs": MetadataValue.int(built["unlinked_programs"]),
        "files": MetadataValue.int(len(files)),
        "size_mb": MetadataValue.float(round(built["bytes"] / 1_000_000, 2)),
        "site_url": MetadataValue.url(PAGES_URL),
    }

    if not config.publish:
        context.log.info("publish=False: rendered only, nothing pushed.")
        metadata["published"] = MetadataValue.bool(False)
        return Output(value=None, metadata=metadata)

    message = (
        "Update YSWS true spend site\n\n"
        f"{built['program_count']} programs, "
        f"{built['spend_transaction_count']} spend transactions. "
        f"Generated {built['generated_at'].strftime('%Y-%m-%d %H:%M UTC')} by the "
        "ysws_true_spend_site Dagster asset."
    )
    result = publish_site(files, message, log=context.log)
    if result["committed"]:
        context.log.info(
            f"Pushed {result['commit_sha'][:8]} to {DEFAULT_REPO}@{DEFAULT_BRANCH} "
            f"({result['changed_files']} changed paths)."
        )
    else:
        context.log.info("Site unchanged since the last run; nothing committed.")

    metadata.update(
        {
            "published": MetadataValue.bool(bool(result["committed"])),
            "commit_sha": MetadataValue.text(result["commit_sha"]),
            "changed_files": MetadataValue.int(result["changed_files"]),
            "repo": MetadataValue.url(f"https://github.com/{DEFAULT_REPO}"),
        }
    )
    return Output(value=None, metadata=metadata)


defs = Definitions(
    assets=[ysws_true_spend_site],
    resources={},
)
