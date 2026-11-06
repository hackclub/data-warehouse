"""Shared by bin/warehouse-admin-psql and bin/warehouse-dbt. Standard library only.

Finds settings, reaches the warehouse host over SSH and picks one running
container. The relay and the container names stay out of this public repo: they
come from the environment or the parent repo's .env (see
runbooks/prod-health-monitoring.md §10).
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

RELAY_VAR = "WAREHOUSE_SSH_RELAY"
POSTGRES_CONTAINER_VAR = "WAREHOUSE_POSTGRES_CONTAINER"
APP_CONTAINER_VAR = "WAREHOUSE_APP_CONTAINER"

REASON_MAX_CHARS = 40


class Refused(Exception):
    """A clear reason not to run; the tools print it and exit 2."""


def _dotenv_candidates() -> list[Path]:
    # WAREHOUSE_DOTENV replaces the lookup entirely, so tests can never reach a
    # real .env by falling through to it.
    override = os.environ.get("WAREHOUSE_DOTENV")
    if override:
        return [Path(override)]
    return [
        REPO_ROOT / ".env",
        Path.home() / "dev" / "hackclub" / "data-warehouse" / ".env",
    ]


def _read_dotenv_value(path: Path, name: str) -> str | None:
    if not path.is_file():
        return None
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line.startswith(f"{name}="):
            continue
        value = line.split("=", 1)[1].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        return value or None
    return None


def setting(name: str) -> str | None:
    """A value from the environment, otherwise from the first .env that has it."""
    value = os.environ.get(name)
    if value:
        return value
    for path in _dotenv_candidates():
        value = _read_dotenv_value(path, name)
        if value:
            return value
    return None


def required_setting(name: str) -> str:
    value = setting(name)
    if not value:
        raise Refused(f"{name} is not set in the environment or the parent repo's .env")
    return value


def clean_reason(reason: str | None) -> str:
    """Make a reason safe for application_name, where it shows in pg_stat_activity."""
    cleaned = re.sub(r"[^A-Za-z0-9_.:-]+", "-", (reason or "").strip()).strip("-")
    if not cleaned:
        raise Refused("--reason is required: say why this needs elevated access")
    return cleaned[:REASON_MAX_CHARS].rstrip("-")


def relay_argv(remote_argv: list[str]) -> list[str]:
    """Build the local command that runs remote_argv on the warehouse host.

    Every `ssh` in the relay hands its command to one more remote shell, so the
    remote command is quoted once per hop. Each hop then sees exactly one word,
    and nothing in the command can be expanded on the way.
    """
    relay = shlex.split(required_setting(RELAY_VAR))
    hops = relay.count("ssh")
    if hops < 1:
        raise Refused(f"{RELAY_VAR} must be an ssh command")
    remote = " ".join(shlex.quote(word) for word in remote_argv)
    for _ in range(hops - 1):
        remote = shlex.quote(remote)
    return [*relay, remote]


def find_container(setting_name: str) -> str:
    """The one running container named <value> or <value>-<suffix>.

    Refuses on zero or several, such as in the middle of a deploy.
    """
    base = required_setting(setting_name)
    result = subprocess.run(
        relay_argv(["docker", "ps", "--format", "{{.Names}}"]),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Refused(
            f"could not list containers through {RELAY_VAR} "
            f"(exit {result.returncode}): {result.stderr.strip()[-300:]}"
        )
    matches = [
        name
        for name in result.stdout.split()
        if name == base or name.startswith(f"{base}-")
    ]
    if len(matches) != 1:
        raise Refused(
            f"expected one running container for {setting_name}, found {len(matches)}"
            + (": " + ", ".join(matches) if matches else "")
            + ". If a deploy is running, wait for it to finish."
        )
    return matches[0]


def fail(message: str) -> int:
    print(f"{Path(sys.argv[0]).name}: {message}", file=sys.stderr)
    return 2
