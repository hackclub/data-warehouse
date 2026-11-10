"""Agent scratch work must never be committed to this public repository."""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRATCH_PREFIXES = ("work/", ".factory-scratch/")


def test_no_agent_scratch_files_are_tracked():
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split("\0")
    offenders = [
        path
        for path in tracked
        if path.startswith(SCRATCH_PREFIXES) or "/scratch/" in f"/{path}"
    ]
    assert not offenders, (
        "Agent scratch files are tracked in Git; remove them (see AGENTS.md "
        f"'Never commit agent scratch work'): {offenders[:20]}"
    )
