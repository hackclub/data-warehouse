"""Shared GitHub URL helpers."""

import re
from typing import Optional, Tuple

_GITHUB_REPO_RE = re.compile(
    r"^(?:https?://)?(?:www\.)?github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)(?:[/?#].*)?$",
    re.IGNORECASE,
)

# First path segments on github.com that are site pages, not users/orgs
_NON_OWNER_SEGMENTS = {"orgs", "topics", "search"}


def parse_github_repo(url: Optional[str]) -> Optional[Tuple[str, str]]:
    """Extract (owner, repo) from a GitHub URL, or None if it isn't a repo URL.

    Accepts http/https, optional www., and strips a trailing .git plus any
    extra path, query or fragment. Case is preserved (GitHub is
    case-insensitive; callers that key on the result should lowercase).

        https://github.com/shreyansh881/tank.git       -> ("shreyansh881", "tank")
        http://www.github.com/hackclub/sprig/tree/main -> ("hackclub", "sprig")
    """
    match = _GITHUB_REPO_RE.match((url or "").strip())
    if not match:
        return None
    owner, repo = match.groups()
    if repo.lower().endswith(".git"):
        repo = repo[:-4]
    if not repo.strip(".") or owner.lower() in _NON_OWNER_SEGMENTS:
        return None
    return owner, repo
