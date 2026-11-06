import importlib
import json
import shutil
import socket
import subprocess
from pathlib import Path

import psycopg2
import pytest
from dagster_sling import SlingResource

from orpheus_engine.defs.sling.assets import _isolate_missing_program_connection
from orpheus_engine_tests.test_sling_program_exclusions import (
    CUSTOM_EXCLUDED_COLUMNS,
    EXCLUDED_COLUMNS,
)
from scripts.provision_warehouse_reader import (
    PROGRAM_ENV_VARS,
    _build_reader_url,
    _check_row_count_parity,
    _quote_ident,
    _quote_literal,
    _render_reader_setup_sql,
    _replication_reader_grants,
    _scram_sha256_verifier,
    _update_env_file_content,
    main as provision_main,
)


@pytest.mark.parametrize(
    "program",
    [
        "attend", "fallen", "horizons", "midnight",
        "macondo", "theseus", "haven", "haven_booking", "atlantis", "pixl",
        "cubed", "mipmap", "contd", "coral", "grandma", "stern", "out_to_c",
        "scraps", "trmnt", "three_am", "canvas", "club_starter_grant",
        "dyad", "embassy", "jame_gam", "save_point", "scare",
    ],
)
@pytest.mark.parametrize("configured", [False, True])
def test_orchard_connection(monkeypatch, program, configured):
    env = PROGRAM_ENV_VARS.get(program, f"{program.upper()}_K8S_URL")
    monkeypatch.delenv(env, raising=False)
    monkeypatch.delenv(f"{env}_SLING", raising=False)
    # An invalid Auth key must have no effect on either direct connection.
    monkeypatch.setenv("AUTH_SSH_PRIVATE_KEY_B64", "invalid-base64")
    if configured:
        monkeypatch.setenv(
            env,
            "postgresql://reader:fake@db.example.com:5432/test"
            "?sslmode=verify-full&sslrootcert=system&application_name=test",
        )
    module = importlib.import_module(f"orpheus_engine.defs.sling.programs.{program}")
    try:
        importlib.reload(module)
        conn = _isolate_missing_program_connection(getattr(module, f"{program}_db_connection"))
        initialized = SlingResource(connections=[conn]).process_config_and_initialize()
        resolved = initialized.connections[0]
        if configured:
            assert resolved.connection_string == (
                "postgresql://reader:fake@db.example.com:5432/test"
                "?sslmode=verify-full&application_name=test"
            )
        else:
            assert resolved.connection_string == (
                f"postgres://missing-env-{env.lower().replace('_', '-')}.invalid:5432/unset"
            )
        assert not getattr(resolved, "ssh_tunnel", None)
        assert not getattr(resolved, "ssh_private_key", None)
    finally:
        # Do not leave a derived test env reference in the imported module.
        monkeypatch.delenv(env, raising=False)
        importlib.reload(module)


def _synthetic_catalog_for_program(program: str) -> dict[str, list[str]]:
    """Build a synthetic information_schema catalog covering every mirrored
    stream, its kept columns, its excluded columns, and an unmirrored table."""
    mod = importlib.import_module(f"orpheus_engine.defs.sling.programs.{program}")
    config = getattr(mod, f"{program}_replication_config")
    excluded_map = (
        EXCLUDED_COLUMNS.get(program)
        or CUSTOM_EXCLUDED_COLUMNS.get(program)
        or {}
    )
    catalog: dict[str, list[str]] = {
        "public.unmirrored_secret_table": ["id", "secret_token", "created_at"],
    }
    for stream_name, opts in config["streams"].items():
        opts = opts or {}
        if opts.get("disabled"):
            continue
        select = opts.get("select") or []
        positives = [c for c in select if not c.startswith("-")]
        negatives = [c[1:] for c in select if c.startswith("-")]
        extra_excluded = excluded_map.get(stream_name, [])
        base_cols = positives or ["id", "user_id", "created_at", "updated_at", "expires_at"]
        all_cols = list(dict.fromkeys(base_cols + negatives + list(extra_excluded)))
        catalog[stream_name] = all_cols
    return catalog


def test_offline_sql_quoting_and_scram_verifier():
    assert _quote_ident("users") == '"users"'
    assert _quote_ident('odd"name') == '"odd""name"'
    with pytest.raises(ValueError):
        _quote_ident("")
    with pytest.raises(ValueError):
        _quote_ident("bad\x00ident")

    assert _quote_literal("plain") == "'plain'"
    assert _quote_literal("o'hare") == "'o''hare'"
    assert _quote_literal(r"back\slash") == r"E'back\\slash'"
    with pytest.raises(ValueError):
        _quote_literal("bad\x00lit")

    verifier = _scram_sha256_verifier("test-password", salt=b"0123456789abcdef", iterations=4096)
    assert verifier.startswith("SCRAM-SHA-256$4096:MDEyMzQ1Njc4OWFiY2RlZg==$")
    assert verifier == _scram_sha256_verifier(
        "test-password", salt=b"0123456789abcdef", iterations=4096
    )
    with pytest.raises(ValueError):
        _scram_sha256_verifier("")


@pytest.mark.parametrize(
    ("program", "expected_total", "expected_table_grants", "expected_col_grants"),
    [
        ("attend", 37, 22, 15),
        ("fallen", 18, 14, 4),
        ("horizons", 26, 21, 5),
        ("midnight", 4, 0, 4),
    ],
)
def test_reader_grants_and_sql_for_four_programs(
    program, expected_total, expected_table_grants, expected_col_grants
):
    mod = importlib.import_module(f"orpheus_engine.defs.sling.programs.{program}")
    config = getattr(mod, f"{program}_replication_config")
    catalog = _synthetic_catalog_for_program(program)

    grants = _replication_reader_grants(config, catalog)
    assert len(grants) == expected_total
    assert "public.unmirrored_secret_table" not in grants

    table_grants = sum(1 for cols in grants.values() if cols is None)
    col_grants = sum(1 for cols in grants.values() if cols is not None)
    assert table_grants == expected_table_grants
    assert col_grants == expected_col_grants

    excluded_map = (
        EXCLUDED_COLUMNS.get(program)
        or CUSTOM_EXCLUDED_COLUMNS.get(program)
        or {}
    )
    for stream_name, excluded_cols in excluded_map.items():
        granted_cols = grants[stream_name]
        assert granted_cols is not None, (program, stream_name)
        if program == "horizons" and stream_name == "public.user_sessions":
            # Horizons hashes user_sessions.id on the source server, so the
            # database login needs SELECT on the 4 source columns while Sling
            # writes only id_sha256 to the warehouse.
            assert granted_cols == ("id", "user_id", "expires_at", "created_at")
            continue
        for excluded_col in excluded_cols:
            assert excluded_col not in granted_cols, (program, stream_name, excluded_col)

    verifier = _scram_sha256_verifier("secret-pw", salt=b"fedcba9876543210")
    sql_text = _render_reader_setup_sql(f"ysws_{program}_db", verifier, grants)
    assert sql_text.startswith("BEGIN;\n")
    assert sql_text.endswith("COMMIT;\n")
    assert "CONNECTION LIMIT 4" in sql_text
    assert 'ALTER ROLE "hc_warehouse_reader" SET default_transaction_read_only = on;' in sql_text
    assert "unmirrored_secret_table" not in sql_text
    assert "secret-pw" not in sql_text


def test_reader_grants_reject_wildcards_and_schema_drift():
    catalog = {"public.users": ["id", "email", "secret_token"]}

    with pytest.raises(ValueError, match="Wildcard"):
        _replication_reader_grants({"streams": {"public.*": None}}, catalog)

    with pytest.raises(ValueError, match="does not exist in source catalog"):
        _replication_reader_grants({"streams": {"public.missing": None}}, catalog)

    with pytest.raises(ValueError, match="excluded column 'typo_col'"):
        _replication_reader_grants(
            {"streams": {"public.users": {"select": ["-typo_col"]}}}, catalog
        )

    with pytest.raises(ValueError, match="selected column 'typo_col'"):
        _replication_reader_grants(
            {"streams": {"public.users": {"select": ["id", "typo_col"]}}}, catalog
        )

    with pytest.raises(ValueError, match="unknown column 'typo_col'"):
        _replication_reader_grants(
            {
                "streams": {
                    "public.users": {
                        "sql": "select id, typo_col from public.users",
                    }
                }
            },
            catalog,
        )

    with pytest.raises(ValueError, match="SCRAM-SHA-256"):
        _render_reader_setup_sql("db", "plaintext-password", {"public.users": None})


def test_row_count_parity_rules():
    full_refresh_cfg = {
        "defaults": {"mode": "full-refresh"},
        "streams": {
            "public.events": None,
            "public.empty_tbl": None,
        },
    }
    pre = {"public.events": (100, 100), "public.empty_tbl": (0, None)}
    post_ok = {"public.events": (102, 100), "public.empty_tbl": (0, None)}
    assert _check_row_count_parity(full_refresh_cfg, pre, post_ok) == []

    post_bad = {"public.events": (100, 50), "public.empty_tbl": (0, None)}
    failures = _check_row_count_parity(full_refresh_cfg, pre, post_bad)
    assert len(failures) == 2

    # When SLING_ALLOW_EMPTY is set (Horizons), an empty table must still exist in the warehouse.
    allow_empty_cfg = {
        "env": {"SLING_ALLOW_EMPTY": True},
        "defaults": {"mode": "full-refresh"},
        "streams": {"public.empty_tbl": None},
    }
    assert _check_row_count_parity(
        allow_empty_cfg, {"public.empty_tbl": (0, 0)}, {"public.empty_tbl": (0, None)}
    )

    # Incremental streams allow a stable positive warehouse-minus-source gap (hard deletes upstream)
    # but fail if warehouse rows regress or the gap jumps.
    inc_cfg = {
        "defaults": {"mode": "incremental"},
        "streams": {"public.projects": {"mode": "incremental"}},
    }
    assert (
        _check_row_count_parity(
            inc_cfg,
            {"public.projects": (3200, 3206)},
            {"public.projects": (3202, 3208)},
        )
        == []
    )
    assert _check_row_count_parity(
        inc_cfg,
        {"public.projects": (3206, 3206)},
        {"public.projects": (3206, 3190)},
    )


def test_provision_cli_writes_sql_and_env_without_printing_secrets(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "db_name": "test_db",
                "columns": _synthetic_catalog_for_program("midnight"),
            }
        ),
        encoding="utf-8",
    )
    env_path = tmp_path / ".env"
    env_path.write_text(
        "OTHER_VAR=keep\n"
        "MIDNIGHT_K8S_URL='postgresql://reader:fake@db.example.com:5432/test"
        "?sslmode=verify-full'\n",
        encoding="utf-8",
    )
    sql_path = tmp_path / "setup.sql"

    rc = provision_main(
        [
            "--program",
            "midnight",
            "--catalog-json",
            str(catalog_path),
            "--env-file",
            str(env_path),
            "--check",
            "--render-sql-file",
            str(sql_path),
            "--write-env-file",
            str(env_path),
        ]
    )
    assert rc == 0
    captured = capsys.readouterr()
    assert "midnight: 4 streams (0 table SELECT, 4 column SELECT), 0 drift errors" in captured.out
    assert "fake" not in captured.out
    assert "SCRAM-SHA-256" not in captured.out
    assert "postgresql://" not in captured.out

    updated_env = env_path.read_text(encoding="utf-8")
    assert "OTHER_VAR=keep" in updated_env
    assert "hc_warehouse_reader:" in updated_env
    assert "reader:fake" not in updated_env
    assert "sslrootcert=system" in updated_env
    assert sql_path.read_text(encoding="utf-8").startswith("BEGIN;\n")


def test_provision_cli_flag_guards_and_offline_count_comparison(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "db_name": "test_db",
                "columns": _synthetic_catalog_for_program("midnight"),
            }
        ),
        encoding="utf-8",
    )
    env_path = tmp_path / ".env"
    env_path.write_text(
        "MIDNIGHT_K8S_URL='postgresql://reader:fake@db.example.com:5432/test?sslmode=verify-full'\n",
        encoding="utf-8",
    )
    sql_path = tmp_path / "setup.sql"

    # 1. No action flag -> fails.
    with pytest.raises(SystemExit):
        provision_main(
            [
                "--program",
                "midnight",
                "--catalog-json",
                str(catalog_path),
                "--env-file",
                str(env_path),
            ]
        )

    # 2. --render-sql-file without --write-env-file -> fails without writing SQL.
    with pytest.raises(SystemExit):
        provision_main(
            [
                "--program",
                "midnight",
                "--catalog-json",
                str(catalog_path),
                "--env-file",
                str(env_path),
                "--render-sql-file",
                str(sql_path),
            ]
        )
    assert not sql_path.exists()

    # 3. --write-env-file without --render-sql-file -> fails.
    with pytest.raises(SystemExit):
        provision_main(
            [
                "--program",
                "midnight",
                "--catalog-json",
                str(catalog_path),
                "--env-file",
                str(env_path),
                "--write-env-file",
                str(env_path),
            ]
        )

    # 4. --write-env-file pointing to a missing file -> fails before writing SQL.
    missing_env = tmp_path / "does_not_exist.env"
    with pytest.raises(SystemExit, match="does not exist"):
        provision_main(
            [
                "--program",
                "midnight",
                "--catalog-json",
                str(catalog_path),
                "--env-file",
                str(env_path),
                "--render-sql-file",
                str(sql_path),
                "--write-env-file",
                str(missing_env),
            ]
        )
    assert not sql_path.exists()

    # 5. Offline --compare-counts-json + --post-counts-json passes on matching counts and fails on regression.
    pre_path = tmp_path / "pre_counts.json"
    post_ok_path = tmp_path / "post_ok.json"
    post_bad_path = tmp_path / "post_bad.json"
    pre_counts = {
        "public.projects": [100, 100],
        "public.submissions": [50, 50],
        "public.users": [200, 200],
        "public.user_sessions": [200, 200],
    }
    post_ok = {
        "public.projects": [102, 102],
        "public.submissions": [50, 50],
        "public.users": [201, 201],
        "public.user_sessions": [201, 201],
    }
    post_bad = {
        "public.projects": [100, 80],
        "public.submissions": [50, 50],
        "public.users": [200, 200],
        "public.user_sessions": [200, 200],
    }
    pre_path.write_text(json.dumps({"counts": pre_counts}), encoding="utf-8")
    post_ok_path.write_text(json.dumps({"counts": post_ok}), encoding="utf-8")
    post_bad_path.write_text(json.dumps({"counts": post_bad}), encoding="utf-8")

    capsys.readouterr()
    rc = provision_main(
        [
            "--program",
            "midnight",
            "--compare-counts-json",
            str(pre_path),
            "--post-counts-json",
            str(post_ok_path),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "midnight: row-count parity passed for 4 streams" in out

    with pytest.raises(SystemExit, match="row-count parity failed"):
        provision_main(
            [
                "--program",
                "midnight",
                "--compare-counts-json",
                str(pre_path),
                "--post-counts-json",
                str(post_bad_path),
            ]
        )


def _find_pg_bin(binary_name: str) -> str | None:
    found = shutil.which(binary_name)
    if found:
        return found
    candidates = sorted(
        Path("/usr/lib/postgresql").glob(f"*/bin/{binary_name}"), reverse=True
    )
    return str(candidates[0]) if candidates else None


def test_live_loopback_postgres_end_to_end_provision_verify_and_counts(tmp_path, capsys):
    initdb_bin = _find_pg_bin("initdb")
    pg_ctl_bin = _find_pg_bin("pg_ctl")
    if not initdb_bin or not pg_ctl_bin:
        pytest.skip("local PostgreSQL binaries (initdb/pg_ctl) not installed")

    pgdata = tmp_path / "pgdata"
    sock_dir = tmp_path / "pgsock"
    sock_dir.mkdir()
    log_file = tmp_path / "postgres.log"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    subprocess.run(
        [
            initdb_bin,
            "-D",
            str(pgdata),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-instructions",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    with (pgdata / "pg_hba.conf").open("a", encoding="utf-8") as hba:
        hba.write("host all hc_warehouse_reader 127.0.0.1/32 scram-sha-256\n")

    pg_opts = (
        f"-F -p {port} -h 127.0.0.1 -k {sock_dir} "
        "-c ssl=off -c password_encryption=scram-sha-256"
    )
    subprocess.run(
        [pg_ctl_bin, "-D", str(pgdata), "-l", str(log_file), "-o", pg_opts, "-w", "start"],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        admin_conn = psycopg2.connect(
            host="127.0.0.1", port=port, user="postgres", dbname="postgres"
        )
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute("CREATE ROLE app_owner LOGIN PASSWORD 'owner-pass'")
            cur.execute("CREATE DATABASE fallen_src OWNER app_owner")
            cur.execute("CREATE DATABASE warehouse_dst OWNER postgres")
        admin_conn.close()

        catalog = _synthetic_catalog_for_program("fallen")
        src_admin = psycopg2.connect(
            host="127.0.0.1", port=port, user="postgres", dbname="fallen_src"
        )
        src_admin.autocommit = True
        with src_admin.cursor() as cur:
            for table_key, cols in catalog.items():
                s_name, t_name = table_key.split(".", 1)
                col_defs = ", ".join(f"{_quote_ident(c)} text" for c in cols)
                q_tbl = f"{_quote_ident(s_name)}.{_quote_ident(t_name)}"
                cur.execute(f"CREATE TABLE {q_tbl} ({col_defs})")
                cur.execute(f"ALTER TABLE {q_tbl} OWNER TO app_owner")
                col_names = ", ".join(_quote_ident(c) for c in cols)
                placeholders = ", ".join(["'row1'"] * len(cols))
                cur.execute(f"INSERT INTO {q_tbl} ({col_names}) VALUES ({placeholders})")
                cur.execute(f"INSERT INTO {q_tbl} ({col_names}) VALUES ({placeholders})")

        wh_admin = psycopg2.connect(
            host="127.0.0.1", port=port, user="postgres", dbname="warehouse_dst"
        )
        wh_admin.autocommit = True
        with wh_admin.cursor() as cur:
            cur.execute('CREATE SCHEMA "fallen"')
            for table_key, cols in catalog.items():
                if table_key == "public.unmirrored_secret_table":
                    continue
                _, t_name = table_key.split(".", 1)
                col_defs = ", ".join(f"{_quote_ident(c)} text" for c in cols)
                q_tbl = f'"fallen".{_quote_ident(t_name)}'
                cur.execute(f"CREATE TABLE {q_tbl} ({col_defs})")
                col_names = ", ".join(_quote_ident(c) for c in cols)
                placeholders = ", ".join(["'row1'"] * len(cols))
                cur.execute(f"INSERT INTO {q_tbl} ({col_names}) VALUES ({placeholders})")
                cur.execute(f"INSERT INTO {q_tbl} ({col_names}) VALUES ({placeholders})")

        env_path = tmp_path / ".env"
        env_path.write_text(
            f"FALLEN_K8S_URL='postgresql://app_owner:owner-pass@127.0.0.1:{port}/fallen_src?sslmode=disable'\n"
            f"WAREHOUSE_COOLIFY_URL='postgresql://postgres@127.0.0.1:{port}/warehouse_dst?sslmode=disable'\n",
            encoding="utf-8",
        )
        sql_path = tmp_path / "fallen_setup.sql"
        pre_counts_path = tmp_path / "pre_counts.json"
        post_counts_path = tmp_path / "post_counts.json"

        # 1. Provision using live catalog fetch (no --catalog-json) and record baseline counts.
        rc = provision_main(
            [
                "--program",
                "fallen",
                "--env-file",
                str(env_path),
                "--check",
                "--render-sql-file",
                str(sql_path),
                "--write-env-file",
                str(env_path),
                "--record-counts-json",
                str(pre_counts_path),
            ]
        )
        assert rc == 0
        with src_admin.cursor() as cur:
            cur.execute(sql_path.read_text(encoding="utf-8"))

        # 2. Verify live reader when env_path now points to hc_warehouse_reader (Finding 1).
        assert "hc_warehouse_reader:" in env_path.read_text(encoding="utf-8")
        rc = provision_main(
            [
                "--program",
                "fallen",
                "--env-file",
                str(env_path),
                "--check",
                "--verify-reader",
            ]
        )
        assert rc == 0

        # 3. Simulate an app migration adding a column to a -col table while env_path
        # already uses hc_warehouse_reader (Runbook §8.3 + Finding 1).
        with src_admin.cursor() as cur:
            cur.execute('ALTER TABLE "public"."users" ADD COLUMN "new_migrated_col" text')
        with pytest.raises(RuntimeError, match="Column privilege mismatch on public.users.new_migrated_col"):
            provision_main(
                [
                    "--program",
                    "fallen",
                    "--env-file",
                    str(env_path),
                    "--verify-reader",
                ]
            )
        rc = provision_main(
            [
                "--program",
                "fallen",
                "--env-file",
                str(env_path),
                "--check",
                "--render-sql-file",
                str(sql_path),
                "--write-env-file",
                str(env_path),
            ]
        )
        assert rc == 0
        with src_admin.cursor() as cur:
            cur.execute(sql_path.read_text(encoding="utf-8"))
        rc = provision_main(
            [
                "--program",
                "fallen",
                "--env-file",
                str(env_path),
                "--verify-reader",
            ]
        )
        assert rc == 0

        # 4. Negative cases for Finding 2: read-only flag off, role memberships,
        # predefined write roles, table write grants, excluded column grants, and unmirrored table grants.
        with src_admin.cursor() as cur:
            cur.execute('ALTER ROLE "hc_warehouse_reader" SET default_transaction_read_only = off')
        with pytest.raises(RuntimeError, match="Expected transaction_read_only='on'"):
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])
        with src_admin.cursor() as cur:
            cur.execute('ALTER ROLE "hc_warehouse_reader" SET default_transaction_read_only = on')

        with src_admin.cursor() as cur:
            cur.execute('GRANT "app_owner" TO "hc_warehouse_reader"')
        with pytest.raises(RuntimeError, match="Unexpected role memberships"):
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])

        # Re-applying setup SQL automatically revokes stray role memberships.
        with src_admin.cursor() as cur:
            cur.execute(sql_path.read_text(encoding="utf-8"))
        assert (
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])
            == 0
        )

        with src_admin.cursor() as cur:
            cur.execute('GRANT "pg_write_all_data" TO "hc_warehouse_reader"')
        with pytest.raises(RuntimeError, match="Unexpected role memberships"):
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])
        with src_admin.cursor() as cur:
            cur.execute('REVOKE "pg_write_all_data" FROM "hc_warehouse_reader"')

        with src_admin.cursor() as cur:
            cur.execute('GRANT INSERT ON TABLE "public"."users" TO PUBLIC')
        with pytest.raises(RuntimeError, match="Unexpected write privilege on public.users"):
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])
        with src_admin.cursor() as cur:
            cur.execute('REVOKE INSERT ON TABLE "public"."users" FROM PUBLIC')

        with src_admin.cursor() as cur:
            cur.execute('GRANT SELECT ("hca_token") ON TABLE "public"."users" TO "hc_warehouse_reader"')
        with pytest.raises(RuntimeError, match="Column privilege mismatch on public.users.hca_token"):
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])
        with src_admin.cursor() as cur:
            cur.execute(sql_path.read_text(encoding="utf-8"))

        with src_admin.cursor() as cur:
            cur.execute(
                'GRANT SELECT ON TABLE "public"."unmirrored_secret_table" TO "hc_warehouse_reader"'
            )
        with pytest.raises(
            RuntimeError,
            match="Unexpected SELECT privilege on unmirrored public.unmirrored_secret_table",
        ):
            provision_main(["--program", "fallen", "--env-file", str(env_path), "--verify-reader"])
        with src_admin.cursor() as cur:
            cur.execute(
                'REVOKE ALL ON TABLE "public"."unmirrored_secret_table" FROM "hc_warehouse_reader"'
            )

        # 5. Live row-count comparison via CLI as hc_warehouse_reader (Finding 3).
        rc = provision_main(
            [
                "--program",
                "fallen",
                "--env-file",
                str(env_path),
                "--compare-counts-json",
                str(pre_counts_path),
                "--record-counts-json",
                str(post_counts_path),
            ]
        )
        assert rc == 0
        assert "owner-pass" not in pre_counts_path.read_text(encoding="utf-8")
        assert "hc_warehouse_reader" not in post_counts_path.read_text(encoding="utf-8")

        # Introduce a count mismatch beyond tolerance and confirm --compare-counts-json fails.
        with src_admin.cursor() as cur:
            for _ in range(10):
                cur.execute('INSERT INTO "public"."users" ("id") VALUES (\'extra\')')
        with pytest.raises(SystemExit, match="row-count parity failed"):
            provision_main(
                [
                    "--program",
                    "fallen",
                    "--env-file",
                    str(env_path),
                    "--compare-counts-json",
                    str(pre_counts_path),
                ]
            )

        src_admin.close()
        wh_admin.close()
    finally:
        subprocess.run(
            [pg_ctl_bin, "-D", str(pgdata), "-m", "immediate", "stop"],
            check=False,
            capture_output=True,
            text=True,
        )


