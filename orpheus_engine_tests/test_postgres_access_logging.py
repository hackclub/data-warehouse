"""Guard PostgreSQL direct-access & statement audit logging config, script, and runbook docs."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
PG_DOCKER_DIR = REPO_ROOT / "warehouse-postgres-docker"
LOGGING_CONF = PG_DOCKER_DIR / "logging.conf"
DOCKERFILE = PG_DOCKER_DIR / "Dockerfile"
CONF_SH = PG_DOCKER_DIR / "conf.sh"
RUNBOOK = REPO_ROOT / "runbooks" / "prod-health-monitoring.md"
SCRIPTS_DIR = REPO_ROOT / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import postgres_audit_logging as pal  # noqa: E402


def _parse_pg_conf(text: str) -> dict[str, str]:
    return pal.parse_pg_conf(text)


def test_logging_conf_enables_connection_disconnection_and_statement_logging():
    assert LOGGING_CONF.is_file(), "warehouse-postgres-docker/logging.conf must exist"
    settings = _parse_pg_conf(LOGGING_CONF.read_text(encoding="utf-8"))

    assert settings.get("log_connections") == "on"
    assert settings.get("log_disconnections") == "on"
    assert settings.get("log_statement") == "all"
    # Suppress bind parameter values in extended-protocol logs to avoid leaking
    # credentials, tokens, or PII into the central log store.
    assert settings.get("log_parameter_max_length") == "0"
    assert settings.get("log_parameter_max_length_on_error") == "0"
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


def test_runbook_settings_match_logging_conf_for_pg17_and_pg18():
    # Operators paste the runbook's copies into Coolify for the Dagster DB (PG 17)
    # and MCP auth DB (PG 18), so both blocks must stay in sync with logging.conf.
    text = RUNBOOK.read_text(encoding="utf-8")
    section = text.split("## 12. Direct database access logging", 1)
    assert len(section) == 2, "runbook §12 heading not found"
    blocks = re.findall(r"```ini\n(.*?)```", section[1], flags=re.DOTALL)
    assert len(blocks) >= 2, "runbook §12 must contain PG 17 and PG 18 ini blocks"

    pg17_expected = pal.expected_settings(17)
    pg18_expected = pal.expected_settings(18)

    assert _parse_pg_conf(blocks[0]) == pg17_expected
    assert _parse_pg_conf(blocks[1]) == pg18_expected
    assert pg18_expected["log_connections"] == "all"

    # Approved per-role exclusion for warehouse sync bulk loads must be documented.
    assert "hc_warehouse_dagster" in section[1]
    assert "log_statement = 'ddl'" in section[1]


def test_render_apply_sql_pg17_with_role_exclusion():
    sql = pal.render_apply_sql(17, role_exclusions=[("hc_warehouse_dagster", "ddl")])
    assert "ALTER SYSTEM SET log_connections = 'on';" in sql
    assert "ALTER SYSTEM SET log_disconnections = 'on';" in sql
    assert "ALTER SYSTEM SET log_statement = 'all';" in sql
    assert "ALTER SYSTEM SET log_parameter_max_length = '0';" in sql
    assert "ALTER SYSTEM SET log_parameter_max_length_on_error = '0';" in sql
    assert 'ALTER ROLE "hc_warehouse_dagster" SET log_statement = \'ddl\';' in sql
    assert sql.rstrip().endswith("SELECT pg_reload_conf();")


def test_render_apply_sql_pg18_uses_all_for_log_connections():
    sql = pal.render_apply_sql(18)
    assert "ALTER SYSTEM SET log_connections = 'all';" in sql
    assert "ALTER SYSTEM SET log_statement = 'all';" in sql
    assert sql.rstrip().endswith("SELECT pg_reload_conf();")


def test_render_rollback_sql_defaults_to_statement_only_and_supports_full_reset():
    stmt_only = pal.render_rollback_sql(role_exclusions=[("hc_warehouse_dagster", "ddl")])
    assert "ALTER SYSTEM RESET log_statement;" in stmt_only
    assert "ALTER SYSTEM RESET log_parameter_max_length;" in stmt_only
    assert "ALTER SYSTEM RESET log_connections;" not in stmt_only
    assert 'ALTER ROLE "hc_warehouse_dagster" RESET log_statement;' in stmt_only
    assert stmt_only.rstrip().endswith("SELECT pg_reload_conf();")

    full = pal.render_rollback_sql(reset_connection_logging=True)
    assert "ALTER SYSTEM RESET log_connections;" in full
    assert "ALTER SYSTEM RESET log_disconnections;" in full
    assert "ALTER SYSTEM RESET log_line_prefix;" in full


def test_parse_role_exclusions_rejects_bad_inputs():
    with pytest.raises(ValueError):
        pal.parse_role_exclusions(["no_equals_sign"])
    with pytest.raises(ValueError):
        pal.parse_role_exclusions(["bad;role=ddl"])
    with pytest.raises(ValueError):
        pal.parse_role_exclusions(["hc_warehouse_dagster=verbose"])
    with pytest.raises(ValueError):
        pal.parse_role_exclusions(["hc_warehouse_dagster=ddl", "hc_warehouse_dagster=mod"])


def test_evaluate_settings_detects_drift_and_unexpected_role_overrides():
    pg17 = pal.expected_settings(17)
    assert (
        pal.evaluate_settings(
            pg17,
            pg_major=17,
            live_role_overrides={"hc_warehouse_dagster": {"log_statement": "ddl"}},
            expected_exclusions=[("hc_warehouse_dagster", "ddl")],
        )
        == []
    )

    drifted = {**pg17, "log_statement": "none"}
    errors = pal.evaluate_settings(
        drifted,
        pg_major=17,
        live_role_overrides={"rogue_role": {"log_statement": "none"}},
        expected_exclusions=[("hc_warehouse_dagster", "ddl")],
    )
    assert any("log_statement" in e for e in errors)
    assert any("hc_warehouse_dagster" in e for e in errors)
    assert any("rogue_role" in e for e in errors)
