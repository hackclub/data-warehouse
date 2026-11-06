"""The agents' warehouse login and the two elevated routes.

Everything here is local: a temporary Postgres on loopback with fake data, removed
afterwards, and stub ssh and docker commands. WAREHOUSE_DOTENV points the tools at a
fake .env, so they cannot read a real one, and DO_NOT_TRACK stops dbt's usage data.

The tests run on whichever Postgres is installed (18 on the build machine; prod is
17). The privilege checks they rely on are the same on both.
"""

from __future__ import annotations

import ast
import importlib.machinery
import importlib.util
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
from contextlib import closing
from pathlib import Path
from unittest import mock

import psycopg2
import pytest
from psycopg2 import errors

from scripts import provision_warehouse_agent_login as provision
from scripts.provision_warehouse_reader import _scram_sha256_verifier

REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_PSQL = REPO_ROOT / "bin" / "warehouse-admin-psql"
WAREHOUSE_DBT = REPO_ROOT / "bin" / "warehouse-dbt"
REAL_DBT = Path(sys.executable).parent / "dbt"

AGENT_PASSWORD = "agent-test-password"
SUPERUSER_PASSWORD = "superuser-test-password"
SALT = "fake-salt"
PG_CONTAINER = "fake-postgres"
APP_CONTAINER = "fake-app"


def _pg_bin(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return str(candidates[0]) if candidates else None


class Server:
    def __init__(self, port: int):
        self.port = port

    def admin_url(self) -> str:
        return f"postgresql://postgres:{SUPERUSER_PASSWORD}@127.0.0.1:{self.port}/warehouse"

    def agent_url(self) -> str:
        return f"postgresql://{provision.AGENT_ROLE}:{AGENT_PASSWORD}@127.0.0.1:{self.port}/warehouse"

    def admin_connection(self, autocommit: bool = True):
        conn = psycopg2.connect(self.admin_url())
        conn.autocommit = autocommit
        return conn

    # `with` on a psycopg2 connection opens a transaction and leaves it open, so
    # these close instead: each statement stands alone and no session lingers.
    def admin(self):
        return closing(self.admin_connection())

    def agent(self, read_only_default: bool = False):
        conn = psycopg2.connect(self.agent_url())
        conn.autocommit = True
        if not read_only_default:
            with conn.cursor() as cur:
                cur.execute("SET default_transaction_read_only = off")
        return closing(conn)


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    initdb, pg_ctl = _pg_bin("initdb"), _pg_bin("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("local PostgreSQL binaries (initdb/pg_ctl) not installed")
    root = tmp_path_factory.mktemp("pg")
    pgdata = root / "pgdata"
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    subprocess.run(
        [initdb, "-D", str(pgdata), "-U", "postgres", "-A", "trust", "--no-instructions"],
        check=True, capture_output=True,
    )
    hba = pgdata / "pg_hba.conf"
    hba.write_text(
        f"host all {provision.AGENT_ROLE} 127.0.0.1/32 scram-sha-256\n" + hba.read_text()
    )
    options = f"-F -p {port} -h 127.0.0.1 -k '' -c ssl=off -c password_encryption=scram-sha-256"
    subprocess.run(
        [pg_ctl, "-D", str(pgdata), "-l", str(root / "log"), "-o", options, "-w", "start"],
        check=True, capture_output=True,
    )
    try:
        running = Server(port)
        conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres", dbname="postgres")
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE warehouse")
        conn.close()
        with running.admin() as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA synced")
            cur.execute("CREATE TABLE synced.before_role (id int, name text)")
            cur.execute("INSERT INTO synced.before_role VALUES (1, 'row')")
            cur.execute("CREATE ROLE fake_reader")
            cur.execute(provision.render_setup_sql(_scram_sha256_verifier(AGENT_PASSWORD)))
            cur.execute("CREATE SCHEMA synced_later")
            cur.execute("CREATE TABLE synced_later.after_role (id int)")
            cur.execute("INSERT INTO synced_later.after_role VALUES (7)")
        yield running
    finally:
        subprocess.run([pg_ctl, "-D", str(pgdata), "-m", "immediate", "-w", "stop"], capture_output=True)


# The login


def test_setup_sql_has_only_a_verifier_and_the_exact_role_design():
    password = "plaintext-must-not-appear"
    sql = provision.render_setup_sql(_scram_sha256_verifier(password))
    assert password not in sql
    assert "SCRAM-SHA-256$" in sql
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" in sql
    assert "GRANT pg_read_all_data, pg_read_all_stats TO" in sql
    assert "pg_signal_backend" not in sql
    assert sql.startswith("BEGIN;") and sql.endswith("COMMIT;\n")
    with pytest.raises(ValueError):
        provision.render_setup_sql(password)


def test_agent_url_keeps_server_and_options_and_changes_only_the_login():
    url = provision.build_agent_url("postgres://postgres:old@db.example:8432/postgres?sslmode=require", "n/w")
    assert url == "postgres://hc_warehouse_agent:n%2Fw@db.example:8432/postgres?sslmode=require"


def test_reads_every_schema_including_one_created_after_the_role(server):
    with server.agent(read_only_default=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT name FROM synced.before_role")
        assert cur.fetchone() == ("row",)
        cur.execute("SELECT id FROM synced_later.after_role")
        assert cur.fetchone() == (7,)


def test_predefined_roles_are_exactly_the_two_and_settings_apply_at_login(server):
    with server.agent(read_only_default=True) as conn, conn.cursor() as cur:
        assert provision._attribute_failures(cur) == []
        cur.execute("SHOW default_transaction_read_only")
        assert cur.fetchone() == ("on",)
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone() == ("30s",)
        cur.execute("SHOW temp_file_limit")
        assert cur.fetchone() == ("50GB",)
        with pytest.raises(errors.InsufficientPrivilege):
            cur.execute("SET temp_file_limit = -1")


def test_setup_sql_runs_again_with_a_new_password(server):
    new_password = "second-password"
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute(provision.render_setup_sql(_scram_sha256_verifier(new_password)))
    url = server.agent_url().replace(AGENT_PASSWORD, new_password)
    psycopg2.connect(url).close()
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute(provision.render_setup_sql(_scram_sha256_verifier(AGENT_PASSWORD)))


@pytest.fixture
def blocked_superuser_session(server):
    """A superuser holding a lock, and a second superuser session waiting on it."""
    blocker = server.admin_connection(autocommit=False)
    with blocker.cursor() as cur:
        cur.execute("LOCK TABLE synced.before_role IN ACCESS EXCLUSIVE MODE")
        cur.execute("SELECT pg_backend_pid()")
        blocker_pid = cur.fetchone()[0]
    waiter = server.admin_connection()

    def wait():
        with waiter.cursor() as cur:
            cur.execute("SELECT 'waiting-marker', count(*) FROM synced.before_role")

    thread = threading.Thread(target=wait)
    thread.start()
    try:
        yield blocker_pid
    finally:
        blocker.rollback()
        thread.join(timeout=30)
        waiter.close()
        blocker.close()


def test_sees_query_text_and_the_blocker_of_superuser_sessions(server, blocked_superuser_session):
    with server.agent(read_only_default=True) as conn, conn.cursor() as cur:
        for _ in range(100):
            cur.execute(
                "SELECT query, pg_blocking_pids(pid) FROM pg_stat_activity "
                "WHERE wait_event_type = 'Lock' AND query LIKE '%waiting-marker%'"
            )
            row = cur.fetchone()
            if row:
                break
            time.sleep(0.05)
        assert row and "waiting-marker" in row[0]
        assert row[1] == [blocked_superuser_session]


def test_cannot_end_or_cancel_a_superuser_session(server, blocked_superuser_session):
    with server.agent() as conn, conn.cursor() as cur:
        for function in ("pg_terminate_backend", "pg_cancel_backend"):
            with pytest.raises(errors.InsufficientPrivilege):
                cur.execute(f"SELECT {function}(%s)", (blocked_superuser_session,))


def test_can_end_its_own_other_session(server):
    with server.agent() as first, server.agent() as second:
        with second.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            pid = cur.fetchone()[0]
        with first.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            assert cur.fetchone() == (True,)


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO synced.before_role VALUES (2, 'x')",
        "UPDATE synced.before_role SET name = 'x'",
        "DELETE FROM synced.before_role",
        "TRUNCATE synced.before_role",
        "DROP TABLE synced.before_role",
        "ALTER TABLE synced.before_role ADD COLUMN c int",
        "CREATE TABLE synced.new_table (id int)",
        "CREATE TABLE public.new_table (id int)",
        "CREATE SCHEMA new_schema",
        "CREATE ROLE new_role",
        "CREATE DATABASE new_database",
        "COPY (SELECT 1) TO PROGRAM 'true'",
        "COPY synced.before_role FROM PROGRAM 'true'",
        "COPY (SELECT 1) TO '/tmp/hc-agent-login-test'",
        "SELECT pg_read_file('postgresql.conf', 0, 10)",
        "SELECT pg_ls_dir('.')",
    ],
)
def test_refused_even_with_the_read_only_default_off(server, statement):
    with server.agent() as conn, conn.cursor() as cur:
        with pytest.raises(errors.InsufficientPrivilege):
            cur.execute(statement)


def test_grant_with_the_read_only_default_off_changes_nothing(server):
    with server.agent() as conn, conn.cursor() as cur:
        cur.execute("GRANT USAGE ON SCHEMA synced TO fake_reader")
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("SELECT has_schema_privilege('fake_reader', 'synced', 'USAGE')")
        assert cur.fetchone() == (False,)


def test_pg_read_all_data_can_read_password_verifiers_in_pg_authid(server):
    """Pins a limit of the role: Postgres has no way to deny this to one login.

    These are SCRAM verifiers, not passwords: they cannot log in, but they allow
    offline guessing, so every login needs a long random password. See §10.
    """
    with server.agent(read_only_default=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT rolpassword FROM pg_authid WHERE rolname = %s", (provision.AGENT_ROLE,))
        assert cur.fetchone()[0].startswith("SCRAM-SHA-256$")


def test_the_writes_that_remain_are_exactly_temp_tables_and_large_objects(server):
    """Pins what an exposed URL can still store on the server. If this changes, update §10."""
    with server.agent() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE scratch (id int)")
        cur.execute("INSERT INTO scratch VALUES (1)")
        cur.execute("SELECT lo_create(0)")
        large_object = cur.fetchone()[0]
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_largeobject_metadata WHERE oid = %s", (large_object,))
        assert cur.fetchone() == (1,), "the large object should outlive the session"
        cur.execute("SELECT lo_unlink(%s)", (large_object,))


@pytest.mark.parametrize("statement", ["CREATE TEMP TABLE scratch (id int)", "SELECT lo_create(0)"])
def test_the_read_only_default_blocks_those_until_it_is_turned_off(server, statement):
    with server.agent(read_only_default=True) as conn, conn.cursor() as cur:
        with pytest.raises(errors.ReadOnlySqlTransaction):
            cur.execute(statement)


def test_connection_limit_stops_the_twenty_first_session(server):
    open_connections = []
    try:
        for _ in range(provision.CONNECTION_LIMIT):
            open_connections.append(psycopg2.connect(server.agent_url()))
        with pytest.raises(psycopg2.OperationalError, match="too many connections"):
            psycopg2.connect(server.agent_url())
    finally:
        for conn in open_connections:
            conn.close()


def test_verify_passes_on_the_real_login_with_another_login_active(server):
    with server.admin() as other, other.cursor() as cur:
        cur.execute("SELECT 1")
        assert provision.verify_agent_login(server.agent_url()) == []


def test_verify_stops_before_any_probe_when_the_login_is_a_superuser(server):
    with mock.patch.object(provision, "_probe_refused") as probe:
        failures = provision.verify_agent_login(server.admin_url())
    probe.assert_not_called()
    assert failures == [f"logged in as another role, not {provision.AGENT_ROLE}"]


def test_verify_flags_a_login_given_a_power(server):
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER ROLE {provision.AGENT_ROLE} SUPERUSER")
    try:
        with mock.patch.object(provision, "_probe_refused") as probe:
            failures = provision.verify_agent_login(server.agent_url())
        probe.assert_not_called()
        assert failures == ["the login has a power attribute such as SUPERUSER"]
    finally:
        with server.admin() as conn, conn.cursor() as cur:
            cur.execute(f"ALTER ROLE {provision.AGENT_ROLE} NOSUPERUSER")


def test_cli_writes_a_private_url_and_sql_with_no_plaintext(tmp_path, capsys):
    source = tmp_path / "source.env"
    source.write_text("OTHER=keep\nWAREHOUSE_COOLIFY_URL=postgres://postgres:old@db.example:8432/postgres?sslmode=require\n")
    sql_file, staged = tmp_path / "role.sql", tmp_path / "staged.env"
    staged.write_text(source.read_text())

    assert provision.main(
        ["--env-file", str(source), "--render-sql-file", str(sql_file), "--write-env-file", str(staged)]
    ) == 0

    written = staged.read_text()
    assert "OTHER=keep\n" in written and "postgres:old" not in written
    password = written.split("hc_warehouse_agent:")[1].split("@")[0]
    assert password not in sql_file.read_text()
    assert password not in capsys.readouterr().out
    for path in (sql_file, staged):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "postgres:old" in source.read_text()


def test_cli_requires_the_env_file_with_the_sql_file(tmp_path):
    source = tmp_path / "source.env"
    source.write_text("WAREHOUSE_COOLIFY_URL=postgres://u:p@h/db\n")
    with pytest.raises(SystemExit):
        provision.main(["--env-file", str(source), "--render-sql-file", str(tmp_path / "x.sql")])


# Stub host: real shell parsing at each ssh hop, a docker that runs locally


STUB_SSH = """#!/bin/sh
# Like real ssh: drop the host, join the rest and hand it to a shell.
printf '%s\\n' "ssh $*" >> "$STUB_LOG"
shift
exec sh -c "$*"
"""

STUB_DOCKER = """#!{python}
import json, os, shutil, subprocess, sys
args = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as log:
    log.write("docker " + json.dumps(args) + "\\n")
if args[0] == "ps":
    print(os.environ["STUB_CONTAINERS"].replace(",", "\\n"))
    sys.exit(0)
assert args[0] == "exec" and args[1] == "-i"
args = args[2:]
env = dict(os.environ)
while args[0] == "-e":
    key, value = args[1].split("=", 1)
    env[key] = value
    args = args[2:]
args = args[1:]
if args[0] == "psql":
    args += ["-h", "127.0.0.1", "-p", env["STUB_PG_PORT"], "-d", "warehouse"]
    args[0] = env["STUB_PSQL"]
    sys.exit(subprocess.run(args, env=env).returncode)
assert args == ["/app/.venv/bin/python", "-"], args
# Point the program at this test's fake /app, and give it the container's environment.
program = sys.stdin.read()
assert 'APP_ROOT = "/app"' in program
program = program.replace('APP_ROOT = "/app"', 'APP_ROOT = ' + repr(env["STUB_APP_ROOT"]), 1)
env["TMPDIR"] = env["STUB_CONTAINER_TMP"]
env["WAREHOUSE_COOLIFY_URL"] = env["STUB_CONTAINER_URL"]
sys.exit(subprocess.run([sys.executable, "-"], input=program, text=True, env=env).returncode)
"""

class Host:
    """A stub path, a fake container and the environment for running the tools."""

    def __init__(self, root: Path, server: Server):
        self.root = root
        self.log = root / "stub.log"
        self.argv_log = root / "dbt-argv.log"
        self.profile_log = root / "dbt-profile.log"
        self.container_tmp = root / "container-tmp"
        self.container_tmp.mkdir()
        self.app_root = root / "app"
        (self.app_root / ".venv" / "bin").mkdir(parents=True)
        stub_bin = root / "stub-bin"
        stub_bin.mkdir()
        self._executable(stub_bin / "ssh", STUB_SSH)
        self._executable(stub_bin / "docker", STUB_DOCKER.format(python=sys.executable))
        self._executable(self.app_root / ".venv" / "bin" / "dbt", self._dbt_wrapper())
        self.dotenv = root / "fake.env"
        self.dotenv.write_text("")
        self.env = {
            "PATH": f"{stub_bin}:{os.environ['PATH']}",
            "HOME": str(root),
            "DO_NOT_TRACK": "1",
            "WAREHOUSE_DOTENV": str(self.dotenv),
            "WAREHOUSE_SSH_RELAY": "ssh fake-relay ssh fake-host",
            "WAREHOUSE_POSTGRES_CONTAINER": PG_CONTAINER,
            "WAREHOUSE_APP_CONTAINER": APP_CONTAINER,
            "STUB_LOG": str(self.log),
            "STUB_CONTAINERS": f"{PG_CONTAINER},{APP_CONTAINER}-20261106,unrelated",
            "STUB_PG_PORT": str(server.port),
            "STUB_PSQL": _pg_bin("psql") or "psql",
            "STUB_APP_ROOT": str(self.app_root),
            "STUB_CONTAINER_TMP": str(self.container_tmp),
            "STUB_CONTAINER_URL": server.admin_url(),
            "STUB_DBT_ARGV": str(self.argv_log),
            "STUB_DBT_PROFILE": str(self.profile_log),
        }

    def _dbt_wrapper(self) -> str:
        # Records what the container's dbt was given, then runs the real dbt.
        return (
            "#!/bin/sh\n"
            'printf \'%s\\n\' "$*" >> "$STUB_DBT_ARGV"\n'
            'previous=""\n'
            'for arg in "$@"; do\n'
            '  if [ "$previous" = "--profiles-dir" ]; then cat "$arg/profiles.yml" >> "$STUB_DBT_PROFILE"; fi\n'
            '  previous="$arg"\n'
            "done\n"
            f'exec {REAL_DBT} "$@"\n'
        )

    @staticmethod
    def _executable(path: Path, content: str) -> None:
        path.write_text(content)
        path.chmod(0o755)

    def set_salt_file(self, salt: str | None = SALT) -> None:
        path = self.app_root / ".env"
        if salt is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(f"OTHER=1\nHACK_CLUBBERS_LOCATION_SALT={salt}\n")

    def run(self, tool: Path, *args: str, **env) -> subprocess.CompletedProcess:
        return subprocess.run(
            [str(tool), *args],
            env={**self.env, **env},
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=180,
        )

    def logged(self) -> str:
        return self.log.read_text() if self.log.exists() else ""


@pytest.fixture
def host(tmp_path, server):
    made = Host(tmp_path, server)
    made.set_salt_file()
    return made


# bin/warehouse-admin-psql


def test_admin_psql_requires_a_reason_before_touching_the_host(host):
    for args in (["-c", "SELECT 1"], ["--reason", " !! ", "-c", "SELECT 1"]):
        result = host.run(ADMIN_PSQL, *args)
        assert result.returncode == 2
        assert "--reason is required" in result.stderr
    assert host.logged() == ""


def test_admin_psql_runs_as_postgres_and_names_the_cleaned_reason(host):
    result = host.run(
        ADMIN_PSQL,
        "--reason", "fix $(touch pwned) 'stuck' dump; rm",
        "-c", "SELECT current_user, current_setting('application_name')",
    )
    assert result.returncode == 0, result.stderr
    assert "postgres" in result.stdout
    assert "elevated:fix-touch-pwned-stuck-dump-rm" in result.stdout
    assert not (host.root / "pwned").exists()


def test_admin_psql_sends_sql_on_stdin_and_never_in_argv(host, server):
    marker = "marker-'quoted' \"double\" $HOME `tick`"
    sql = "SELECT $sql$" + marker + "$sql$;\n"
    result = host.run(ADMIN_PSQL, "--reason", "test", "-c", sql)
    assert result.returncode == 0, result.stderr
    assert marker in result.stdout
    assert "marker" not in host.logged()
    assert "[\"exec\", \"-i\", \"-e\", \"PGAPPNAME=elevated:test\", \"fake-postgres\", \"psql\", \"-U\", \"postgres\", \"-X\", \"-v\", \"ON_ERROR_STOP=1\", \"-f\", \"-\"]" in host.logged()


def test_admin_psql_reads_a_file_and_stops_at_the_first_error(host, server):
    sql_file = host.root / "fix.sql"
    sql_file.write_text(
        "CREATE TABLE synced.admin_first (id int);\n"
        "SELECT broken FROM nowhere;\n"
        "CREATE TABLE synced.admin_second (id int);\n"
    )
    result = host.run(ADMIN_PSQL, "--reason", "test", "-f", str(sql_file))
    assert result.returncode != 0
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('synced.admin_first'), to_regclass('synced.admin_second')")
        assert cur.fetchone() == ("synced.admin_first", None)
        cur.execute("DROP TABLE synced.admin_first")


@pytest.mark.parametrize(
    "containers, expected",
    [("unrelated", "found 0"), (f"{PG_CONTAINER},{PG_CONTAINER}-2", "found 2")],
)
def test_admin_psql_refuses_unless_one_container_matches(host, containers, expected):
    result = host.run(ADMIN_PSQL, "--reason", "test", "-c", "SELECT 1", STUB_CONTAINERS=containers)
    assert result.returncode == 2
    assert expected in result.stderr
    assert "exec" not in host.logged()


def test_admin_psql_names_a_missing_relay_setting(host):
    result = host.run(ADMIN_PSQL, "--reason", "test", "-c", "SELECT 1", WAREHOUSE_SSH_RELAY="")
    assert result.returncode == 2
    assert "WAREHOUSE_SSH_RELAY is not set" in result.stderr


def test_settings_come_from_the_fake_env_and_never_the_real_one(tmp_path, monkeypatch):
    relay = _load_tool("_warehouse_relay", REPO_ROOT / "bin" / "_warehouse_relay.py")
    fake = tmp_path / "fake.env"
    fake.write_text("A_SETTING='from file'\n")
    monkeypatch.setenv("WAREHOUSE_DOTENV", str(fake))
    monkeypatch.delenv("A_SETTING", raising=False)
    assert relay._dotenv_candidates() == [fake]
    assert relay.setting("A_SETTING") == "from file"
    monkeypatch.setenv("A_SETTING", "from env")
    assert relay.setting("A_SETTING") == "from env"


def _load_tool(name: str, path: Path):
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        loader.exec_module(module)
    finally:
        sys.path.remove(str(path.parent))
    return module


# bin/warehouse-dbt


@pytest.fixture
def project(host):
    """A small project: a model whose SQL needs the salt, a hook, and tests."""
    if not REAL_DBT.exists():
        pytest.skip("dbt is not installed in this environment")
    root = host.root / "fake_dbt_project"
    (root / "models").mkdir(parents=True)
    (root / "tests").mkdir()
    (root / "dbt_project.yml").write_text(
        "name: fake_project\n"
        "version: '1.0.0'\n"
        "config-version: 2\n"
        "profile: orpheus_engine_dbt\n"
        "model-paths: ['models']\n"
        "test-paths: ['tests']\n"
        "on-run-end:\n"
        "  - 'GRANT USAGE ON SCHEMA public TO fake_reader'\n"
        "models:\n"
        "  fake_project:\n"
        "    +materialized: table\n"
    )
    (root / "models" / "only_in_upload.sql").write_text(
        "{% set salt = env_var('HACK_CLUBBERS_LOCATION_SALT') %}\n"
        "select '{{ salt }}' as salt, current_user as who,\n"
        "current_setting('application_name') as app\n"
    )
    (root / "tests" / "passes.sql").write_text(
        "select 1 from {{ ref('only_in_upload') }} where salt is null\n"
    )
    (root / "tests" / "fails.sql").write_text("select 1 as always_fails\n")
    host.env["WAREHOUSE_DBT_PROJECT_DIR"] = str(root)
    return root


def test_profile_matches_what_the_scheduled_job_uses():
    """A wrong target or schema would build superuser tables in a stray schema."""
    container = _load_tool("_warehouse_dbt_container", REPO_ROOT / "bin" / "_warehouse_dbt_container.py")
    import yaml

    source = ast.parse((REPO_ROOT / "orpheus_engine" / "defs" / "dbt" / "definitions.py").read_text())
    constants = {
        node.targets[0].id: ast.literal_eval(node.value)
        for node in source.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in ("DBT_PROFILE_NAME", "DBT_TARGET_NAME")
    }
    schemas = [
        ast.literal_eval(value)
        for node in ast.walk(source)
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values)
        if isinstance(key, ast.Constant) and key.value == "schema"
    ]
    profile = yaml.safe_load(container.PROFILE_YAML)
    assert constants == {"DBT_PROFILE_NAME": "orpheus_engine_dbt", "DBT_TARGET_NAME": "prod"}
    assert schemas == ["public"]
    assert list(profile) == [constants["DBT_PROFILE_NAME"]]
    entry = profile[constants["DBT_PROFILE_NAME"]]
    assert entry["target"] == constants["DBT_TARGET_NAME"]
    assert list(entry["outputs"]) == [constants["DBT_TARGET_NAME"]]
    assert entry["outputs"]["prod"]["schema"] == schemas[0]
    project_yaml = yaml.safe_load((REPO_ROOT / "orpheus_engine_dbt" / "dbt_project.yml").read_text())
    assert project_yaml["profile"] == constants["DBT_PROFILE_NAME"]


def test_profile_holds_only_references_and_usage_data_is_off():
    container = _load_tool("_warehouse_dbt_container", REPO_ROOT / "bin" / "_warehouse_dbt_container.py")
    assert "password: \"{{ env_var('DBT_ENV_SECRET_WAREHOUSE_PASSWORD') }}\"" in container.PROFILE_YAML
    env = container.dbt_env("postgres://u:p%40ss@h:8432/db?sslmode=require", "salt", "app")
    assert env[container.PASSWORD_VAR] == "p@ss"
    assert env["WAREHOUSE_DBT_PORT"] == "8432"
    assert env["WAREHOUSE_DBT_SSLMODE"] == "require"
    assert env["DO_NOT_TRACK"] == "1"
    assert env["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"


def test_local_compile_succeeds_under_the_new_role_and_needs_the_salt(host, server, project):
    host.dotenv.write_text(
        f"WAREHOUSE_COOLIFY_URL={server.agent_url()}\nHACK_CLUBBERS_LOCATION_SALT={SALT}\n"
    )
    result = host.run(WAREHOUSE_DBT, "compile", "-s", "only_in_upload")
    assert result.returncode == 0, result.stdout + result.stderr
    compiled = next((project / "target" / "compiled").rglob("only_in_upload.sql")).read_text()
    assert SALT in compiled
    assert SUPERUSER_PASSWORD not in result.stdout + result.stderr
    assert AGENT_PASSWORD not in result.stdout + result.stderr


def test_local_route_names_a_missing_salt_or_url(host, project):
    host.dotenv.write_text("WAREHOUSE_COOLIFY_URL=postgres://u:p@h/db\n")
    result = host.run(WAREHOUSE_DBT, "compile")
    assert result.returncode == 2
    assert "HACK_CLUBBERS_LOCATION_SALT is not set" in result.stderr
    host.dotenv.write_text("")
    result = host.run(WAREHOUSE_DBT, "compile")
    assert result.returncode == 2
    assert "WAREHOUSE_COOLIFY_URL is not set" in result.stderr


@pytest.mark.parametrize("command", ["run", "test", "build", "seed", "snapshot"])
def test_local_route_refuses_writes_and_points_to_the_elevated_form(host, command):
    result = host.run(WAREHOUSE_DBT, command)
    assert result.returncode == 2
    assert f'--elevated --reason "<why>" {command}' in result.stderr


@pytest.mark.parametrize("flag", ["--target", "--target=dev", "--profiles-dir", "--project-dir", "--profile", "-t"])
def test_both_routes_refuse_flags_the_tool_pins(host, flag):
    for prefix in ([], ["--elevated", "--reason", "test"]):
        result = host.run(WAREHOUSE_DBT, *prefix, "compile", flag, "x")
        assert result.returncode == 2
        assert "is set by this tool" in result.stderr
    assert host.logged() == ""


def test_elevated_requires_a_reason_and_rejects_other_commands(host, project):
    result = host.run(WAREHOUSE_DBT, "--elevated", "run")
    assert result.returncode == 2 and "--reason is required" in result.stderr
    result = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "x", "seed")
    assert result.returncode == 2 and "--elevated runs only" in result.stderr
    assert host.logged() == ""


def test_dbt_test_fails_under_the_new_role_because_of_the_hook(host, server, project):
    """Pins why `dbt test` is elevated even though its tests only read."""
    profiles = host.root / "profiles"
    profiles.mkdir()
    container = _load_tool("_warehouse_dbt_container", REPO_ROOT / "bin" / "_warehouse_dbt_container.py")
    container.write_profile(str(profiles))
    env = {**container.dbt_env(server.agent_url(), SALT, "test"), "HOME": str(host.root)}
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE public.only_in_upload (salt text)")
    try:
        result = subprocess.run(
            [str(REAL_DBT), "test", "-s", "passes",
             "--project-dir", str(project), "--profiles-dir", str(profiles)],
            env=env, capture_output=True, text=True, timeout=180,
        )
    finally:
        with server.admin() as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE public.only_in_upload")
    assert result.returncode != 0, result.stdout
    assert "cannot execute GRANT in a read-only transaction" in result.stdout, result.stdout


def test_elevated_builds_the_uploaded_tree_tests_it_and_cleans_up(host, server, project):
    (project / "target").mkdir()
    (project / "target" / "stale.txt").write_text("must not upload")

    built = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "fix the model", "run")
    assert built.returncode == 0, built.stdout + built.stderr
    assert "elevated dbt run from not a git checkout; reason: fix-the-model" in built.stderr

    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("SELECT salt, who, app FROM public.only_in_upload")
        assert cur.fetchone() == (SALT, "postgres", "elevated-dbt:fix-the-model")
        cur.execute(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'public_%'"
        )
        assert cur.fetchone() == (0,), "tables must land in the profile's schema"

    tested = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "fix the model", "test", "-s", "passes")
    assert tested.returncode == 0, tested.stdout + tested.stderr

    assert list(host.container_tmp.iterdir()) == []
    for text in (host.logged(), host.argv_log.read_text(), host.profile_log.read_text(),
                 built.stdout, built.stderr, tested.stdout):
        assert SUPERUSER_PASSWORD not in text
    assert "{{ env_var('DBT_ENV_SECRET_WAREHOUSE_PASSWORD') }}" in host.profile_log.read_text()

    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE public.only_in_upload")


def test_elevated_upload_leaves_out_target_and_logs(host, project, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_DOTENV", str(host.dotenv))
    tool = _load_tool("warehouse_dbt", WAREHOUSE_DBT)
    (project / "target").mkdir(exist_ok=True)
    (project / "target" / "stale.txt").write_text("x")
    (project / "logs").mkdir()
    (project / "logs" / "dbt.log").write_text("x")
    import base64, io, tarfile

    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(tool._project_tar_b64(project)))) as tar:
        names = [Path(name).as_posix() for name in tar.getnames()]
    assert "dbt_project.yml" in names and "models/only_in_upload.sql" in names
    assert not [name for name in names if name.startswith(("target", "logs"))]


def test_elevated_returns_dbt_failure_and_still_cleans_up(host, server, project):
    built = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "x", "run")
    assert built.returncode == 0, built.stdout + built.stderr
    failed = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "x", "test", "-s", "fails")
    assert failed.returncode == 1, failed.stdout + failed.stderr
    assert list(host.container_tmp.iterdir()) == []
    with server.admin() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE public.only_in_upload")


def test_elevated_prefers_the_container_env_salt_and_refuses_with_none(host, server, project):
    host.set_salt_file(None)
    refused = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "x", "compile")
    assert refused.returncode == 2
    assert "this container has no HACK_CLUBBERS_LOCATION_SALT" in refused.stderr
    assert list(host.container_tmp.iterdir()) == []

    allowed = host.run(
        WAREHOUSE_DBT, "--elevated", "--reason", "x", "compile",
        HACK_CLUBBERS_LOCATION_SALT="salt-from-container-env",
    )
    assert allowed.returncode == 0, allowed.stdout + allowed.stderr


@pytest.mark.parametrize(
    "containers, expected",
    [("unrelated", "found 0"), (f"{APP_CONTAINER}-1,{APP_CONTAINER}-2", "found 2")],
)
def test_elevated_refuses_unless_one_app_container_matches(host, project, containers, expected):
    result = host.run(WAREHOUSE_DBT, "--elevated", "--reason", "x", "run", STUB_CONTAINERS=containers)
    assert result.returncode == 2
    assert expected in result.stderr
    assert "exec" not in host.logged()


def test_elevated_commands_carry_no_arguments_on_the_command_line(host, server, project):
    secret_looking = "select-arg-only-in-payload"
    host.run(WAREHOUSE_DBT, "--elevated", "--reason", "x", "compile", "-s", secret_looking)
    logged = host.logged()
    assert secret_looking not in logged
    exec_lines = [line for line in logged.splitlines() if line.startswith("docker") and "exec" in line]
    assert [json.loads(line.split(" ", 1)[1]) for line in exec_lines] == [
        ["exec", "-i", f"{APP_CONTAINER}-20261106", "/app/.venv/bin/python", "-"]
    ]


# scripts/ysws_true_spend_preview.py


def test_spend_preview_dbt_uses_the_elevated_route(monkeypatch):
    import scripts.ysws_true_spend_preview as preview

    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="1 of 1 OK created\n", stderr="")

    monkeypatch.setattr(preview.subprocess, "run", fake_run)
    preview.run_dbt(["model_a", "model_b"])
    assert calls == [
        [
            str(REPO_ROOT / "bin" / "warehouse-dbt"),
            "--elevated", "--reason", preview.DBT_REASON,
            "run", "--select", "model_a", "model_b",
        ]
    ]


def test_spend_preview_stops_when_dbt_fails(monkeypatch):
    import scripts.ysws_true_spend_preview as preview

    monkeypatch.setattr(
        preview.subprocess, "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 1, stdout="boom\n", stderr=""),
    )
    with pytest.raises(SystemExit, match="dbt run failed"):
        preview.run_dbt(["model_a"])
