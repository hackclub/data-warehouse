"""Guard PostgreSQL direct-access & statement audit logging config, script, and runbook docs."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from scripts import postgres_audit_logging as pal
from scripts import provision_warehouse_agent_login as pwal
from scripts import provision_warehouse_reader as pwr

REPO_ROOT = Path(__file__).resolve().parent.parent
PG_DOCKER_DIR = REPO_ROOT / "warehouse-postgres-docker"
LOGGING_CONF = PG_DOCKER_DIR / "logging.conf"
DOCKERFILE = PG_DOCKER_DIR / "Dockerfile"
CONF_SH = PG_DOCKER_DIR / "conf.sh"
RUNBOOK = REPO_ROOT / "runbooks" / "prod-health-monitoring.md"
MCP_DIR = REPO_ROOT / "warehouse-mcp"

# Postmaster-context settings that need a restart; the reload-only apply SQL skips them.
RESTART_ONLY_KEYS = {"logging_collector"}


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


def test_runbook_settings_match_logging_conf_and_document_limits():
    # Operators paste the runbook's copies into Coolify for the Dagster DB (PG 17)
    # and MCP auth DB (PG 18), so both blocks must stay in sync with logging.conf.
    text = RUNBOOK.read_text(encoding="utf-8")
    section = text.split("## 12. Direct database access logging", 1)
    assert len(section) == 2, "runbook §12 heading not found"
    body = section[1]
    blocks = re.findall(r"```ini\n(.*?)```", body, flags=re.DOTALL)
    assert len(blocks) >= 2, "runbook §12 must contain PG 17 and PG 18 ini blocks"

    pg17_expected = pal.expected_settings(17)
    pg18_expected = pal.expected_settings(18)

    assert _parse_pg_conf(blocks[0]) == pg17_expected
    assert _parse_pg_conf(blocks[1]) == pg18_expected
    assert pg18_expected["log_connections"] == "all"

    # The exclusions were approved on 2026-11-25; no "pending" wording may remain.
    assert "Approved per-role exclusions" in body
    assert "pending approval" not in body
    assert "hc_warehouse_dagster" in body
    assert "dagster_app" in body
    assert "fivetran" in body
    # Extended protocol, superuser bypass limit, and multi-line limit documented.
    assert "execute <name>:" in body
    assert "PGOPTIONS" in body
    assert "auditd" in body
    assert "Multi-line SQL statements" in body


def test_render_apply_sql_pg17_places_role_exclusion_before_cluster_reload():
    sql = pal.render_apply_sql(
        17,
        role_exclusions=[("hc_warehouse_dagster", "ddl")],
        suppress_error_roles=["hc_warehouse_dagster"],
    )
    alter_role_pos = sql.index('ALTER ROLE "hc_warehouse_dagster" SET log_statement = \'ddl\';')
    alter_err_pos = sql.index('ALTER ROLE "hc_warehouse_dagster" SET log_min_error_statement = \'panic\';')
    alter_verb_pos = sql.index('ALTER ROLE "hc_warehouse_dagster" SET log_error_verbosity = \'terse\';')
    alter_sys_pos = sql.index("ALTER SYSTEM SET log_statement = 'all';")
    assert alter_role_pos < alter_err_pos < alter_verb_pos < alter_sys_pos
    assert "ALTER SYSTEM SET log_connections = 'on';" in sql
    assert "ALTER SYSTEM SET log_disconnections = 'on';" in sql
    assert "ALTER SYSTEM SET log_parameter_max_length = '0';" in sql
    assert "ALTER SYSTEM SET log_parameter_max_length_on_error = '0';" in sql
    assert sql.rstrip().endswith("SELECT pg_reload_conf();")


def test_apply_sql_covers_every_reloadable_logging_conf_setting():
    # A setting added to logging.conf must reach live clusters through the
    # reload-only apply SQL too, not only through `check`.
    conf_keys = set(pal.parse_pg_conf(LOGGING_CONF.read_text(encoding="utf-8")))
    assert set(pal.RELOADABLE_KEYS) == conf_keys - RESTART_ONLY_KEYS
    sql = pal.render_apply_sql(17)
    applied = set(re.findall(r"^ALTER SYSTEM SET (\w+) = ", sql, flags=re.MULTILINE))
    assert applied == conf_keys - RESTART_ONLY_KEYS


def test_render_apply_sql_pg18_uses_all_for_log_connections():
    sql = pal.render_apply_sql(18)
    assert "ALTER SYSTEM SET log_connections = 'all';" in sql
    assert "ALTER SYSTEM SET log_statement = 'all';" in sql
    assert sql.rstrip().endswith("SELECT pg_reload_conf();")


def test_render_rollback_sql_resets_cluster_and_reloads_before_role_reset():
    stmt_only = pal.render_rollback_sql(
        role_exclusions=[("dagster_app", "ddl")],
        suppress_error_roles=["dagster_app"],
    )
    reset_sys = stmt_only.index("ALTER SYSTEM RESET log_statement;")
    reload_pos = stmt_only.index("SELECT pg_reload_conf();")
    source_guard = stmt_only.index("v_source NOT IN ('default', 'configuration file')")
    val_guard = stmt_only.index("v_setting <> 'none'")
    reset_role = stmt_only.index('ALTER ROLE "dagster_app" RESET log_statement;')
    reset_err_role = stmt_only.index('ALTER ROLE "dagster_app" RESET log_min_error_statement;')
    reset_verb_role = stmt_only.index('ALTER ROLE "dagster_app" RESET log_error_verbosity;')
    assert reset_sys < reload_pos < source_guard < val_guard < reset_role < reset_err_role < reset_verb_role
    assert "ALTER SYSTEM RESET log_parameter_max_length;" in stmt_only
    assert "ALTER SYSTEM RESET log_connections;" not in stmt_only

    file_managed = pal.render_rollback_sql(
        role_exclusions=[("hc_warehouse_dagster", "ddl")],
        suppress_error_roles=["hc_warehouse_dagster"],
        cluster_managed_by_file=True,
    )
    assert "ALTER SYSTEM RESET" not in file_managed
    assert file_managed.index("SELECT pg_reload_conf();") < file_managed.index(
        'ALTER ROLE "hc_warehouse_dagster" RESET log_statement;'
    )

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


def test_evaluate_settings_detects_drift_unexpected_overrides_and_file_errors():
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
        file_errors=["/etc/postgresql/postgresql.conf:99 (bad_param): syntax error"],
    )
    assert any("log_statement" in e for e in errors)
    assert any("hc_warehouse_dagster" in e for e in errors)
    assert any("rogue_role" in e for e in errors)
    assert any("pg_file_settings error" in e for e in errors)


def test_cli_check_subcommand_exists_and_handles_missing_env():
    rc = pal.main(["check", "--dsn-env", "NONEXISTENT_TEST_DSN_VAR_AE7057C6", "--pg-major", "17"])
    assert rc == 2


def test_provisioning_scripts_suppress_statement_logging_for_scram_ddl():
    verifier = pwr._scram_sha256_verifier("dummy-test-password-only")

    for login in pwal.LOGINS.values():
        agent_sql = pwal.render_setup_sql(login, verifier)
        assert "SET LOCAL log_statement = 'none';" in agent_sql
        assert "SET LOCAL log_min_duration_statement = -1;" in agent_sql
        assert "SET LOCAL log_min_error_statement = 'panic';" in agent_sql
        assert agent_sql.index("SET LOCAL log_statement = 'none';") < agent_sql.index(verifier)
        assert agent_sql.index("SET LOCAL log_min_error_statement = 'panic';") < agent_sql.index(verifier)

    reader_sql = pwr._render_reader_setup_sql(
        role_name="hc_warehouse_reader",
        db_name="app_db",
        scram_verifier=verifier,
        grants={"public.items": None},
        conn_limit=4,
    )
    assert "SET LOCAL log_statement = 'none';" in reader_sql
    assert "SET LOCAL log_min_duration_statement = -1;" in reader_sql
    assert "SET LOCAL log_min_error_statement = 'panic';" in reader_sql
    assert reader_sql.index("SET LOCAL log_statement = 'none';") < reader_sql.index(verifier)
    assert reader_sql.index("SET LOCAL log_min_error_statement = 'panic';") < reader_sql.index(verifier)


def test_warehouse_mcp_attribution_comment_is_single_line():
    # Static check so root pytest verifies B2/R1 even when psycopg_pool isn't installed.
    db_src = (MCP_DIR / "db.py").read_text(encoding="utf-8")
    assert "-- warehouse-mcp" not in db_src, (
        "warehouse-mcp/db.py must not use '-- warehouse-mcp ...\\n' which splits SQL onto an unprefixed log line"
    )
    assert "/* warehouse-mcp " in db_src
    assert "_SAFE_ATTRIBUTION_EXTRA" in db_src
    assert "MAX_ATTRIBUTION_NAME_LENGTH" in db_src
