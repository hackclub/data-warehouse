"""Tests for scripts/dagster_db_reader.py.

A fake world stands in for the vault, the database and git, while the real host
program runs in a local python3 with a fake docker on its PATH. All values are made
up. Two tests use a throwaway Postgres container with no network.
"""

from __future__ import annotations

import base64
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlsplit

import psycopg2
import pytest

from scripts import dagster_db_reader as reader

SUPER = "sup3r/p@ss+w0rd-Kx7"
NOTE_1 = "note-one"
NOTE_2 = "note-two"
GOOD_CHECKS = {
    "has_power_attribute": False,
    "connection_limit": reader.CONNECTION_LIMIT,
    "settings": dict(reader.ROLE_SETTINGS),
    "memberships": list(reader.ROLE_MEMBERSHIPS),
    "writable_relations": [],
    "can_create_in_public": False,
    "can_create_in_database": False,
    "read_only_by_default": True,
    "runbook_reads": {name: True for name in reader.READ_CHECKS},
    "real_insert": "refused",
}


def superuser_url() -> str:
    return f"postgres://postgres:{quote(SUPER, safe='')}@db.example:5432/dagster?sslmode=require"


FAKE_DOCKER = r"""#!/bin/sh
case "$1 $2" in
  "ps --filter") [ "${FAKE_APP_COUNT:-1}" = 1 ] && echo app-container; exit 0 ;;
  "inspect --format") echo '{"Image":"app:1","Env":["PATH=/bin","POSTGRES_PASSWORD=x","IN_STORE=1","HAND_SET=2","COOLIFY_FQDN=x","SOURCE_COMMIT=y"'"${FAKE_EXTRA_ENV:+,\"$FAKE_EXTRA_ENV\"}"']}'; exit 0 ;;
  "image inspect") echo '["PATH=/bin"]'; exit 0 ;;
esac
exit 9
"""


class World:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        self.checkout = tmp_path / "checkout"
        self.checkout.mkdir()
        self.parent = self.checkout / ".env"
        self.parent.write_text(f"B=1\nPROD_DAGSTER_DB_URL={superuser_url()}\nC=2\n")
        self.parent.chmod(0o600)
        self.app_dir = tmp_path / "app"
        self.app_dir.mkdir()
        (self.app_dir / ".env").write_text(
            f"POSTGRES_PASSWORD={SUPER}\nPOSTGRES_DB=dagster\nPOSTGRES_USERNAME=postgres\n"
        )
        self.worktree = tmp_path / "wt"
        self.worktree.mkdir()
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
        self.notes = {
            NOTE_1: {"id": NOTE_1, "name": "part 1", "fields": [], "notes": f"B=1\nPROD_DAGSTER_DB_URL={superuser_url()}\nC=2\n"},
            NOTE_2: {"id": NOTE_2, "name": "part 2", "fields": [], "notes": "D=3\n"},
        }
        self.sql: list[str] = []
        self.argvs: list[list[str]] = []
        self.inputs: list[bytes] = []
        self.events: list[str] = []
        self.logged_in_as = reader.READER_ROLE
        self.login_urls: list[str] = []
        self.checks = json.loads(json.dumps(GOOD_CHECKS))
        self.drop_note_edit = False
        self.after_note_edit = None

    def args(self, command: str, *extra: str) -> list[str]:
        common = ["--ssh=env", "--app-id=app-id", f"--app-dir={self.app_dir}"]
        if command == "push-gate":
            return [command, *common, *extra]
        copies = [f"--parent-env={self.parent}", f"--vault-note={NOTE_1}", f"--vault-note={NOTE_2}"]
        db = ["--db-container=db"] if command == "provision" else []
        return [command, *common, *copies, *db, *extra]

    def deps(self):
        world = self

        class FakeDeps:
            run = staticmethod(world.run)

            @staticmethod
            def login(url):
                world.login_urls.append(url)
                return {"user": world.logged_in_as, "database": "dagster"}

            @staticmethod
            def checks(url):
                world.events.append("checks")
                return world.checks

        return FakeDeps()

    def main(self, command: str, *extra: str) -> int:
        return reader.main(self.args(command, *extra), self.deps())

    def run(self, argv, *, step, input_bytes=None, timeout=120):
        self.argvs.append(list(argv))
        self.inputs.append(input_bytes or b"")
        if argv[0] == "env" and argv[1:] == ["python3", "-"]:
            return reader.safe_run([sys.executable, "-"], step=step, input_bytes=input_bytes)
        if argv[:2] == ["env", "docker"]:
            self.sql.append(input_bytes.decode())
            self.events.append("role_sql")
            return ""
        if argv[0] == "vaultwarden-agent":
            return self.vault(argv[1:], input_bytes)
        if argv[0] == "git":
            return f"worktree {self.checkout}\nHEAD a\n\nworktree {self.worktree}\nHEAD b\n"
        raise AssertionError(f"unexpected command {argv[0]}")

    def vault(self, argv, stdin) -> str:
        if argv[:2] == ["get", "item"]:
            return json.dumps(self.notes[argv[2]])
        if argv[:2] == ["edit", "item"]:
            self.events.append("vault_edit")
            if not self.drop_note_edit:
                self.notes[argv[2]] = json.loads(base64.b64decode(stdin))
                if self.after_note_edit:
                    self.after_note_edit(self.notes[argv[2]])
            return "{}"
        raise AssertionError(f"unexpected vault command {argv}")

    def parent_url(self) -> str:
        return reader._parse_env_text(self.parent.read_text())["PROD_DAGSTER_DB_URL"]

    def note_url(self) -> str:
        return reader._parse_env_text(self.notes[NOTE_1]["notes"])["PROD_DAGSTER_DB_URL"]


@pytest.fixture
def world(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def forms_of(value: str) -> list[str]:
    return [value, quote(value, safe=""), base64.b64encode(value.encode()).decode()]


def provisioned(world: World, capsys) -> dict:
    assert world.main("provision") == 0
    return json.loads(capsys.readouterr().out)


# Pure helpers


def test_reader_url_keeps_the_database_and_options():
    assert (
        reader.reader_url(superuser_url(), "p", None, None)
        == "postgres://dagster_reader:p@db.example:5432/dagster?sslmode=require"
    )
    assert (
        reader.reader_url(superuser_url(), "p", "fd7a::1", 6)
        == "postgres://dagster_reader:p@[fd7a::1]:6/dagster?sslmode=require"
    )


def test_names_holding_finds_plain_encoded_and_commented_values():
    text = f"A={SUPER}\nB=postgres://u:{quote(SUPER, safe='')}@h/d\n# C={SUPER}\nD=other\n"
    assert reader.env_names_holding(text, SUPER) == ["A", "B", "# C"]


@pytest.mark.parametrize(
    "line",
    [
        f"# old: postgres://postgres:{SUPER}@h/db?sslmode=require",
        f"# old: postgres://postgres:{quote(SUPER, safe='')}@h/db?sslmode=require",
        f"postgres://postgres:{SUPER}@h/db?sslmode=require",
        SUPER,
        f"# {SUPER}",
    ],
)
def test_names_holding_never_returns_a_line_with_no_real_name(line):
    assert reader.env_names_holding(f"{line}\n", SUPER) == [reader.UNNAMED_LINE]


@pytest.mark.parametrize("line", ["TOKENVALUE123", "# TOKENVALUE123", "TOKENVALUE123=", "TOKENVALUE123=x"])
def test_names_holding_never_returns_a_value_shaped_like_a_name(line):
    assert reader.env_names_holding(f"{line}\n", "TOKENVALUE123") == [reader.UNNAMED_LINE]


@pytest.mark.parametrize(
    "key, shown",
    [
        ("PROD_DAGSTER_DB_URL", "PROD_DAGSTER_DB_URL"),
        ("# OLD_URL", "# OLD_URL"),
        ("postgres://postgres:s3cret@h/db?sslmode", reader.UNNAMED_LINE),
        ("# old: postgres://postgres:s3cret@h/db?sslmode", reader.UNNAMED_LINE),
        ("aGVsbG8gd29ybGQ", reader.UNNAMED_LINE),
        ("two words", reader.UNNAMED_LINE),
        ("", reader.UNNAMED_LINE),
    ],
)
def test_printable_name_shows_only_real_env_names(key, shown):
    assert reader.printable_name(key) == shown


@pytest.mark.parametrize(
    "after",
    ["A=2\nPROD_DAGSTER_DB_URL=new\n", "PROD_DAGSTER_DB_URL=new\n", "A=1\nZ=9\nPROD_DAGSTER_DB_URL=new\n"],
)
def test_others_unchanged_catches_a_changed_lost_or_added_name(after):
    assert not reader.others_unchanged("A=1\nPROD_DAGSTER_DB_URL=old\n", after)


def test_others_unchanged_ignores_the_url_and_comments():
    assert reader.others_unchanged("A=1\nPROD_DAGSTER_DB_URL=old\n", "# note\nA=1\nPROD_DAGSTER_DB_URL=new\n")


def test_replace_url_line_refuses_an_edit_that_changes_another_name(monkeypatch):
    monkeypatch.setattr(reader, "_update_env_file_content", lambda text, updates: "A=2\nPROD_DAGSTER_DB_URL=new\n")
    with pytest.raises(reader.StepFailure) as caught:
        reader.replace_url_line("A=1\nPROD_DAGSTER_DB_URL=old\n", "new")
    assert caught.value.step == "env_line" and "another name" in caught.value.detail


@pytest.mark.parametrize("text", ["A=1\n", "PROD_DAGSTER_DB_URL=1\nPROD_DAGSTER_DB_URL=2\n"])
def test_replace_url_line_needs_exactly_one_line(text):
    with pytest.raises(reader.StepFailure):
        reader.replace_url_line(text, "x")


def test_replace_url_line_keeps_quotes_and_other_lines():
    text = "A=1\nPROD_DAGSTER_DB_URL='old'\nZ=$weird\n"
    assert reader.replace_url_line(text, "new") == "A=1\nPROD_DAGSTER_DB_URL='new'\nZ=$weird\n"


def test_role_sql_holds_only_a_verifier_and_a_read_grant():
    sql = reader.render_role_sql("SCRAM-SHA-256$4096:c2FsdA==$a2V5:a2V5")
    assert "GRANT pg_read_all_data TO dagster_reader" in sql
    assert "NOSUPERUSER" in sql and "CONNECTION LIMIT 10" in sql
    assert sql.index("BEGIN;") < sql.index("ALTER ROLE") < sql.index("COMMIT;")


def test_safe_run_failures_keep_nothing_of_argv_or_output():
    with pytest.raises(reader.StepFailure) as caught:
        reader.safe_run(
            [sys.executable, "-c", f"import sys; print('{SUPER}', file=sys.stderr); sys.exit(4)"],
            step="bad",
        )
    assert caught.value.detail == "exit 4" and SUPER not in str(caught.value)
    with pytest.raises(reader.StepFailure) as caught:
        reader.safe_run(
            [sys.executable, "-c", f"import time; time.sleep(30) # {SUPER}"],
            step="slow",
            timeout=0.2,
        )
    assert caught.value.detail == "TimeoutExpired" and SUPER not in repr(caught.value)
    assert caught.value.__context__ is None


# Provision


def test_provision_moves_both_copies_to_the_reader(world, capsys):
    (world.checkout / ".env.bak-old").write_text(f"B=0\nPROD_DAGSTER_DB_URL={superuser_url()}\n")
    out = provisioned(world, capsys)
    assert out["problems"] == []
    url = world.parent_url()
    assert urlsplit(url).username == "dagster_reader"
    assert re.fullmatch(r"[A-Za-z0-9]{40}", urlsplit(url).password)
    assert (urlsplit(url).hostname, urlsplit(url).query) == ("db.example", "sslmode=require")
    assert world.note_url() == url
    assert world.parent.read_text() == f"B=1\nPROD_DAGSTER_DB_URL={url}\nC=2\n"
    assert world.notes[NOTE_1]["notes"] == f"B=1\nPROD_DAGSTER_DB_URL={url}\nC=2\n"
    assert world.notes[NOTE_2]["notes"] == "D=3\n"
    assert stat.S_IMODE(world.parent.stat().st_mode) == 0o600
    # A backup keeps its other names but loses the superuser value.
    backup = (world.checkout / ".env.bak-old").read_text()
    assert SUPER not in backup and quote(SUPER, safe="") not in backup and "B=0" in backup
    # No backup of the old line was left anywhere in the checkout.
    assert not any(quote(SUPER, safe="") in p.read_text() for p in world.checkout.glob(".*"))
    # Only a verifier reached the database, and it matches the password saved.
    password = urlsplit(url).password
    assert password not in world.sql[0] and "SCRAM-SHA-256$" in world.sql[0]
    assert world.login_urls == [url]


def test_provision_writes_nothing_if_the_new_url_cannot_log_in(world, capsys):
    before = world.parent.read_text()
    world.logged_in_as = "someone-else"
    assert world.main("provision") == 1
    assert "login_test" in capsys.readouterr().err
    assert world.parent.read_text() == before
    assert "vault_edit" not in world.events
    assert not list(world.checkout.glob(".env.*"))


def test_provision_refuses_before_any_change_when_the_note_cannot_be_edited(world, capsys):
    world.notes[NOTE_1]["fields"] = [{"name": "1password-item-id", "value": "x"}]
    before = world.parent.read_text()
    assert world.main("provision") == 1
    assert "mirrored" in capsys.readouterr().err
    assert world.events == [] and world.parent.read_text() == before


def test_provision_refuses_when_no_single_note_holds_the_url(world, capsys):
    world.notes[NOTE_2]["notes"] = world.notes[NOTE_1]["notes"]
    assert world.main("provision") == 1
    assert "expected one note" in capsys.readouterr().err
    assert world.events == []


def test_provision_rerun_is_safe_and_gives_a_new_password(world, capsys):
    provisioned(world, capsys)
    first = world.parent_url()
    provisioned(world, capsys)
    assert world.parent_url() != first and world.note_url() == world.parent_url()
    assert len(world.sql) == 2 and "IF NOT EXISTS" in world.sql[1]
    assert world.parent.read_text().count("PROD_DAGSTER_DB_URL=") == 1


def test_provision_url_host_moves_both_copies(world, capsys):
    assert world.main("provision", "--url-host=10.1.1.1", "--url-port=7") == 0
    assert urlsplit(world.parent_url()).netloc.endswith("@10.1.1.1:7")
    assert world.note_url() == world.parent_url()


def test_provision_keeps_a_line_added_after_it_started(world, capsys, monkeypatch):
    """Another card adds an address between the script's start and its write."""
    real_login = world.deps().login

    def login_then_add(url):
        world.parent.write_text(world.parent.read_text() + "ADDED_BY_ANOTHER_CARD=1\n")
        world.notes[NOTE_1]["notes"] += "ADDED_BY_ANOTHER_CARD=1\n"
        return real_login(url)

    deps = world.deps()
    deps.login = staticmethod(login_then_add)
    assert reader.main(world.args("provision"), deps) == 0
    assert "ADDED_BY_ANOTHER_CARD=1" in world.parent.read_text()
    assert "ADDED_BY_ANOTHER_CARD=1" in world.notes[NOTE_1]["notes"]
    assert urlsplit(world.parent_url()).username == "dagster_reader"


def test_provision_reports_another_name_changing_in_the_parent_during_the_edit(world, capsys, monkeypatch):
    real_write = reader.atomic_write

    def write_then_change(path, text):
        real_write(path, text)
        path.write_text(path.read_text().replace("C=2", "C=changed"))

    monkeypatch.setattr(reader, "atomic_write", write_then_change)
    assert world.main("provision") == 1
    assert json.loads(capsys.readouterr().out)["problems"] == [
        "another name in the parent env file changed during the edit"
    ]


def test_provision_reports_another_name_lost_from_the_note_during_the_edit(world, capsys):
    def lose_a_name(note):
        note["notes"] = note["notes"].replace("C=2\n", "")

    world.after_note_edit = lose_a_name
    assert world.main("provision") == 1
    assert json.loads(capsys.readouterr().out)["problems"] == [
        "another name in the vault note changed during the edit"
    ]


def test_provision_reports_a_vault_edit_that_did_not_take(world, capsys):
    world.drop_note_edit = True
    assert world.main("provision") == 1
    assert "did not take the edit" in capsys.readouterr().err


def test_provision_reports_a_backup_holding_the_value_elsewhere(world, capsys):
    (world.checkout / ".env.bak-odd").write_text(f"OTHER={SUPER}\n")
    assert world.main("provision") == 1
    out = json.loads(capsys.readouterr().out)
    assert out["checkout_backups_still_holding_value"] == [".env.bak-odd"]


def test_provision_needs_the_host_settings(world, capsys):
    (world.app_dir / ".env").write_text("POSTGRES_DB=dagster\n")
    assert world.main("provision") == 1
    assert "password missing" in capsys.readouterr().err
    assert world.events == []


# Verify


def test_verify_passes_once_provisioned(world, capsys):
    provisioned(world, capsys)
    assert world.main("verify") == 0
    out = json.loads(capsys.readouterr().out)
    assert out["logins"]["parent_env"]["user"] == "dagster_reader"
    assert out["logins"]["vault_note"]["user"] == "dagster_reader"
    assert out["superuser_value_holders"] == {
        "parent_env": [],
        "checkout_env_files": [],
        "worktree_env_files": [],
        "vault_notes": [],
    }


def test_verify_before_provision_fails_and_runs_no_write_probe_on_a_superuser(world, capsys):
    world.logged_in_as = "postgres"
    assert world.main("verify") == 1
    out = json.loads(capsys.readouterr().out)
    assert "checks" not in world.events
    assert out["superuser_value_holders"]["parent_env"] == ["PROD_DAGSTER_DB_URL"]
    assert out["superuser_value_holders"]["vault_notes"] == ["part 1"]


@pytest.mark.parametrize(
    "change, problem",
    [
        (lambda w: w.parent.write_text(w.parent.read_text() + f"COPY={SUPER}\n"), "in parent_env"),
        (lambda w: (w.checkout / ".env.bak-x").write_text(f"X={SUPER}\n"), "in checkout_env_files"),
        (lambda w: (w.worktree / ".env").write_text(f"X={SUPER}\n"), "in worktree_env_files"),
        (lambda w: w.notes[NOTE_2].update(notes=f"X={SUPER}\n"), "in vault_notes"),
        (lambda w: w.notes[NOTE_1].update(notes="PROD_DAGSTER_DB_URL=other\n"), "different URLs"),
        (lambda w: w.checks.update(has_power_attribute=True), "power attribute"),
        (lambda w: w.checks.update(connection_limit=-1), "connection limit"),
        (lambda w: w.checks.update(settings={}), "role settings"),
        (lambda w: w.checks.update(memberships=["pg_write_all_data"]), "role memberships"),
        (lambda w: w.checks.update(writable_relations=["public.runs"]), "write privileges"),
        (lambda w: w.checks.update(can_create_in_public=True), "can create"),
        (lambda w: w.checks.update(read_only_by_default=False), "read-only by default"),
        (lambda w: w.checks.update(real_insert="allowed"), "real insert"),
        (lambda w: w.checks["runbook_reads"].update(tick_errors=False), "tick_errors"),
    ],
)
def test_verify_reports_each_problem(world, capsys, change, problem):
    provisioned(world, capsys)
    change(world)
    assert world.main("verify") == 1
    assert any(problem in p for p in json.loads(capsys.readouterr().out)["problems"])


def test_verify_lists_host_backups_with_the_value_without_failing(world, capsys):
    (world.app_dir / ".env.bak-holds").write_text(f"POSTGRES_PASSWORD={SUPER}\n")
    (world.app_dir / ".env.bak-encoded").write_text(f"U=x:{quote(SUPER, safe='')}@h\n")
    (world.app_dir / ".env.bak-stale").write_text("POSTGRES_PASSWORD=retired\n")
    provisioned(world, capsys)
    assert world.main("verify") == 0
    out = json.loads(capsys.readouterr().out)
    assert out["host_backups_holding_superuser_value"] == [".env.bak-encoded", ".env.bak-holds"]


def test_verify_reports_parent_and_vault_differences_by_name(world, capsys):
    provisioned(world, capsys)
    world.parent.write_text(world.parent.read_text().replace("C=2", "C=9") + "ONLY_HERE=1\n")
    assert world.main("verify") == 0
    out = json.loads(capsys.readouterr().out)["parent_and_vault_differ"]
    assert out == {"only_in_parent": ["ONLY_HERE"], "only_in_vault": ["D"], "different_values": ["C"]}


def assert_shows_no_secret(output, secret: str) -> None:
    for form in forms_of(secret):
        assert form not in output.out and form not in output.err


def test_verify_prints_no_value_from_a_commented_superuser_url(world, capsys):
    provisioned(world, capsys)
    world.parent.write_text(world.parent.read_text() + f"# old: {superuser_url()}\n")
    assert world.main("verify") == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["superuser_value_holders"]["parent_env"] == [reader.UNNAMED_LINE]
    assert_shows_no_secret(output, SUPER)


@pytest.mark.parametrize(
    "line",
    ["postgres://postgres:0therS3cret@h/db?sslmode=require", "0therS3cret=", "# 0therS3cret=x"],
)
def test_verify_prints_no_value_from_a_line_with_no_real_name(world, capsys, line):
    provisioned(world, capsys)
    world.parent.write_text(world.parent.read_text() + f"{line}\n")
    assert world.main("verify") == 0
    assert_shows_no_secret(capsys.readouterr(), "0therS3cret")


def test_verify_prints_no_value_in_any_difference_list(world, capsys):
    provisioned(world, capsys)
    only_parent = "postgres://u:parentS3cret@h/db?sslmode=require\n"
    only_vault = "postgres://u:vaultS3cret@h/db?sslmode=require\n"
    both = "postgres://u:bothS3cret@h/db?sslmode"
    world.parent.write_text(world.parent.read_text() + only_parent + f"{both}=1\n")
    world.notes[NOTE_2]["notes"] += only_vault + f"{both}=2\n"
    assert world.main("verify") == 0
    output = capsys.readouterr()
    for secret in ("parentS3cret", "vaultS3cret", "bothS3cret"):
        assert_shows_no_secret(output, secret)
    differ = json.loads(output.out)["parent_and_vault_differ"]
    assert differ["only_in_parent"] == [reader.UNNAMED_LINE]
    assert differ["only_in_vault"] == [reader.UNNAMED_LINE, "D"]
    assert differ["different_values"] == [reader.UNNAMED_LINE]


# Push gate


def gate(world, capsys, names: str, tmp_path: Path) -> tuple[int, dict]:
    path = tmp_path / "store-names.txt"
    path.write_text(names)
    code = world.main("push-gate", f"--store-names-file={path}")
    return code, json.loads(capsys.readouterr().out or "{}")


def test_push_gate_stops_on_host_only_names(world, capsys, tmp_path):
    code, out = gate(world, capsys, "POSTGRES_PASSWORD\nIN_STORE\n", tmp_path)
    assert code == 1 and out["host_only"] == ["HAND_SET"]


def test_push_gate_prints_no_value_from_an_env_entry_with_no_name(world, capsys, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_EXTRA_ENV", "postgres://u:s3cretValue@h/db")
    code, out = gate(world, capsys, "POSTGRES_PASSWORD\nIN_STORE\nHAND_SET\n", tmp_path)
    assert code == 1 and out["host_only"] == [reader.UNNAMED_LINE]


def test_push_gate_passes_when_the_store_has_everything(world, capsys, tmp_path):
    code, out = gate(world, capsys, "POSTGRES_PASSWORD\nIN_STORE\nHAND_SET\n", tmp_path)
    assert code == 0 and out["host_only"] == []


def test_push_gate_refuses_an_empty_list_and_a_deploy_in_progress(world, capsys, tmp_path, monkeypatch):
    assert gate(world, capsys, "\n", tmp_path)[0] == 1
    monkeypatch.setenv("FAKE_APP_COUNT", "2")
    code, _ = gate(world, capsys, "A\n", tmp_path)
    assert code == 1


# The value never leaks, whichever call fails


@pytest.mark.parametrize("command", ["provision", "verify"])
def test_no_form_of_any_password_leaks_on_any_failure(tmp_path, monkeypatch, capsys, command):
    failed = 0
    for fail_at in range(1, 30):
        directory = tmp_path / str(fail_at)
        directory.mkdir()
        world = World(directory, monkeypatch)
        if command == "verify":
            provisioned(world, capsys)
        calls = {"n": 0}
        real = world.run

        def flaky(argv, *, step, input_bytes=None, timeout=120):
            calls["n"] += 1
            if calls["n"] == fail_at:
                raise reader.StepFailure(step, "exit 1")
            return real(argv, step=step, input_bytes=input_bytes, timeout=timeout)

        world.argvs.clear()
        deps = world.deps()
        deps.run = staticmethod(flaky)
        code = reader.main(world.args(command), deps)
        output = capsys.readouterr()
        new_password = urlsplit(world.parent_url()).password
        flat = "\n".join(" ".join(argv) for argv in world.argvs)
        for form in forms_of(SUPER) + forms_of(new_password):
            assert form not in output.out and form not in output.err
            assert form not in flat
        if calls["n"] < fail_at:
            assert code == 0
            break
        failed += 1
        assert code == 1 and "FAILED" in output.err
    assert failed >= 3


# A real Postgres with no network: the role SQL, the denials and a real login


def _docker_usable() -> bool:
    real = shutil.which("docker")
    return bool(real) and subprocess.run([real, "info"], capture_output=True).returncode == 0


@pytest.fixture
def real_postgres(tmp_path, monkeypatch):
    if not _docker_usable():
        pytest.skip("docker is not available")
    socket_dir = tmp_path / "socket"
    socket_dir.mkdir()
    socket_dir.chmod(0o777)
    name = f"dagster-reader-test-{secrets.token_hex(6)}"
    monkeypatch.setenv("POSTGRES_PASSWORD", secrets.token_urlsafe(16))
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "--network", "none", "-e", "POSTGRES_PASSWORD"]
        # By default loopback logins need no password, which would prove nothing.
        + ["-e", "POSTGRES_DB=dagster", "-e", "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256"]
        + ["-v", f"{socket_dir}:/var/run/postgresql", "postgres:17-alpine"],
        check=True,
        capture_output=True,
    )

    def superuser_sql(sql: str, step: str = "sql") -> str:
        return reader.safe_run(
            ["docker", "exec", "-i", "-u", "postgres", name]
            + ["psql", "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "dagster"],
            step=step,
            input_bytes=sql.encode(),
        )

    try:
        for _ in range(60):
            try:
                superuser_sql("SELECT 1;")
                # The image restarts the server once during its first start.
                time.sleep(1)
                superuser_sql("SELECT 1;")
                break
            except reader.StepFailure:
                time.sleep(1)
        else:
            pytest.fail("test Postgres did not start")
        superuser_sql(
            "CREATE TABLE runs (run_id text NOT NULL DEFAULT 'x', pipeline_name text, status text,"
            " create_timestamp timestamptz DEFAULT now());"
            "CREATE TABLE event_logs (id bigserial, dagster_event_type text, event text, timestamp timestamptz);"
            "CREATE TABLE job_ticks (id bigserial, tick_body text);"
            "INSERT INTO runs (status) VALUES ('STARTED');"
        )
        yield name, superuser_sql, socket_dir
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def socket_url(socket_dir: Path, password: str = "x") -> str:
    return f"postgresql://dagster_reader:{password}@/dagster?host={socket_dir}"


def test_real_postgres_reader_passes_every_check_and_cannot_write(real_postgres):
    name, superuser_sql, socket_dir = real_postgres
    password = secrets.token_urlsafe(24)
    sql = reader.render_role_sql(reader._scram_sha256_verifier(password))
    superuser_sql(sql)
    superuser_sql(sql)  # Rerunning is safe.

    # The precomputed hash logs in over a password route (TCP inside the container).
    def tcp_login(attempt: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "exec", "-i", "-e", "PGPASSWORD", name, "psql", "-X", "-At", "-h", "127.0.0.1"]
            + ["-U", "dagster_reader", "-d", "dagster", "-c", "SELECT current_user"],
            capture_output=True,
            env={**os.environ, "PGPASSWORD": attempt},
        )

    assert tcp_login(password).stdout.decode().strip() == "dagster_reader"
    assert tcp_login("wrong").returncode != 0

    url = socket_url(socket_dir)
    assert reader.login_identity(url) == {"user": "dagster_reader", "database": "dagster"}
    checks = reader.reader_checks(url)
    assert reader.role_problems(checks) == []
    assert checks["real_insert"] == "refused"

    # The denials the runbook promises, with the read-only default turned off.
    conn = psycopg2.connect(url)
    conn.autocommit = True
    denied = [
        "UPDATE runs SET status = 'x'",
        "DELETE FROM runs",
        "TRUNCATE runs",
        "SELECT nextval('job_ticks_id_seq')",
        "CREATE TABLE public.new_table (a int)",
        "ALTER ROLE dagster_reader SUPERUSER",
        "GRANT pg_write_all_data TO dagster_reader",
    ]
    try:
        with conn.cursor() as cur:
            cur.execute("SET default_transaction_read_only = off")
            for statement in denied:
                with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute(statement)
            # The documented limit: temporary objects are still possible, and do not touch data.
            cur.execute("CREATE TEMP TABLE scratch (a int)")
            # The documented exposure: pg_read_all_data can read the password hashes.
            cur.execute("SELECT count(*) FROM pg_authid WHERE rolpassword IS NOT NULL")
            assert cur.fetchone()[0] >= 1
    finally:
        conn.close()
    assert superuser_sql("SELECT count(*) FROM runs;").strip() == "1"


def test_real_postgres_checks_flag_a_login_that_can_write(real_postgres):
    _, superuser_sql, socket_dir = real_postgres
    superuser_sql(reader.render_role_sql(reader._scram_sha256_verifier("not-used")))
    superuser_sql("GRANT INSERT ON runs TO dagster_reader;")
    checks = reader.reader_checks(socket_url(socket_dir))
    assert checks["writable_relations"] == ["public.runs"]
    assert checks["real_insert"] == "allowed"
    assert superuser_sql("SELECT count(*) FROM runs;").strip() == "1"
    assert reader.role_problems(checks)
