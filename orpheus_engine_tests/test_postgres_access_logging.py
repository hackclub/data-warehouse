"""Guard PostgreSQL direct-access logging configuration and runbook docs."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PG_DOCKER_DIR = REPO_ROOT / "warehouse-postgres-docker"
LOGGING_CONF = PG_DOCKER_DIR / "logging.conf"
DOCKERFILE = PG_DOCKER_DIR / "Dockerfile"
CONF_SH = PG_DOCKER_DIR / "conf.sh"
RUNBOOK = REPO_ROOT / "runbooks" / "prod-health-monitoring.md"


def _parse_pg_conf(text: str) -> dict[str, str]:
    settings: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            value = value[1:-1]
        settings[key] = value
    return settings


def test_logging_conf_enables_connection_and_disconnection_logging():
    assert LOGGING_CONF.is_file(), "warehouse-postgres-docker/logging.conf must exist"
    settings = _parse_pg_conf(LOGGING_CONF.read_text(encoding="utf-8"))

    assert settings.get("log_connections") == "on"
    assert settings.get("log_disconnections") == "on"
    # Container stderr must remain enabled so Docker's log drain ships logs off-host.
    assert settings.get("log_destination") == "stderr"
    assert settings.get("logging_collector") == "off"

    prefix = settings.get("log_line_prefix", "")
    for escape, meaning in (
        ("%m", "timestamp"),
        ("%p", "backend pid"),
        ("%u", "user/role"),
        ("%d", "database"),
        ("%a", "application_name"),
        ("%r", "client host and port"),
    ):
        assert escape in prefix, (
            f"log_line_prefix ({prefix!r}) must include {escape} ({meaning})"
        )


def test_dockerfile_installs_logging_conf_and_init_script():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert re.search(
        r"^\s*COPY\s+logging\.conf\s+/etc/postgresql/logging\.conf\s*$",
        dockerfile,
        flags=re.MULTILINE,
    ), "Dockerfile must copy logging.conf to /etc/postgresql/logging.conf"
    assert re.search(
        r"^\s*COPY\s+conf\.sh\s+/docker-entrypoint-initdb\.d/conf\.sh\s*$",
        dockerfile,
        flags=re.MULTILINE,
    ), "Dockerfile must copy conf.sh to /docker-entrypoint-initdb.d/conf.sh"


def test_conf_sh_includes_logging_conf(tmp_path):
    pgdata = tmp_path / "pgdata"
    pgdata.mkdir()
    pg_conf = pgdata / "postgresql.conf"
    pg_conf.write_text("# base config\n", encoding="utf-8")

    env = {
        **os.environ,
        "PGDATA": str(pgdata),
        "PGBACKREST_STANZA": "warehouse",
    }
    result = subprocess.run(
        ["bash", str(CONF_SH)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"conf.sh failed: {result.stderr}"

    generated = _parse_pg_conf(pg_conf.read_text(encoding="utf-8"))
    assert generated.get("include") == "/etc/postgresql/logging.conf"
    assert generated.get("archive_mode") == "on"


def test_runbook_settings_match_logging_conf():
    # Operators paste the runbook's copy into Coolify for the Dagster database,
    # which does not use this image, so it must not drift from logging.conf.
    text = RUNBOOK.read_text(encoding="utf-8")
    section = text.split("## 12. Direct database access logging", 1)
    assert len(section) == 2, "runbook §12 heading not found"
    block = re.search(r"```ini\n(.*?)```", section[1], flags=re.DOTALL)
    assert block, "runbook §12 must contain an ini block of the settings"

    assert _parse_pg_conf(block.group(1)) == _parse_pg_conf(
        LOGGING_CONF.read_text(encoding="utf-8")
    )
