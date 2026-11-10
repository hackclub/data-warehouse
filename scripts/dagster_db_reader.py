#!/usr/bin/env python3
"""Keep agents on dagster_reader, a read-only login for the Dagster instance database.

The superuser password belongs only in Coolify's store, the host `.env` and the app
container. See "The agents' read-only Dagster login" in
runbooks/prod-health-monitoring.md.

Subcommands, run as `PYTHONPATH=. .venv/bin/python scripts/dagster_db_reader.py <command>`:

    provision   Create the login, or give it a new password, and save its URL.
    verify      Check the login and that no agent copy holds the superuser value.
    push-gate   Before a push to main: stop if the app has variables Coolify's store lacks.

Secrets travel only through stdin or in-process values, never through a command's
arguments. Output is names and booleans. A failure reports a step name and an error
type, because a subprocess traceback echoes its argv and output. Identifiers come from
arguments (or an @file of arguments), since this repo is public.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import shlex
import string
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit, urlunsplit

from scripts.provision_warehouse_reader import (
    _parse_env_text,
    _scram_sha256_verifier,
    _update_env_file_content,
)

READER_ROLE = "dagster_reader"
URL_KEY = "PROD_DAGSTER_DB_URL"
CONNECTION_LIMIT = 10  # The server allows 50 connections and the app needs most of them.
ROLE_MEMBERSHIPS = ["pg_read_all_data"]
ROLE_SETTINGS = {
    # A backstop only: the login can turn it off. The missing privileges are the block.
    "default_transaction_read_only": "on",
    # A forgotten agent session must not hold anything open.
    "statement_timeout": "5min",
    "idle_in_transaction_session_timeout": "60s",
}
# Coolify adds these to every container itself, so its store never lists them.
COOLIFY_METADATA = re.compile(r"^(COOLIFY_.*|SOURCE_COMMIT)$")
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
# What output may show of an env line. Uppercase only: a base64 line ending in `=`, or a
# URL with no name, also parses as "name=value", and its "name" is then a secret.
PRINTABLE_NAME = re.compile(r"(#\s*)?[A-Z_][A-Z0-9_]*")
UNNAMED_LINE = "<unnamed line>"
# The runbook's reads, bounded so that a check stays cheap on a large event_logs.
READ_CHECKS = {
    "queue_by_status": "SELECT status, count(*) FROM runs"
    " WHERE create_timestamp > now() - interval '4 hours' GROUP BY 1",
    "active_runs": "SELECT run_id, pipeline_name, status, create_timestamp FROM runs"
    " WHERE status IN ('QUEUED','STARTED','CANCELING') ORDER BY create_timestamp LIMIT 5",
    "step_failures": "SELECT dagster_event_type, event FROM event_logs"
    " WHERE dagster_event_type = 'STEP_FAILURE' AND timestamp > now() - interval '1 hour' LIMIT 1",
    "tick_errors": "SELECT tick_body FROM job_ticks ORDER BY id DESC LIMIT 1",
    "storage_sanity": "SELECT count(*), min(create_timestamp) FROM runs",
}

# Runs on the app host, sent over stdin. Standard library only. Values stay on the host
# except the `read_superuser` reply, which this script keeps in memory and never prints.
HOST_PROGRAM = r"""
import json, subprocess, sys
from pathlib import Path
from urllib.parse import quote


def env_value(text, key):
    for line in text.splitlines():
        if line.startswith(key + "="):
            value = line[len(key) + 1 :]
            if len(value) > 1 and value[0] in "\"'" and value.endswith(value[0]):
                return value[1:-1]
            return value
    return None


def read_superuser(request):
    app_dir = Path(request["app_dir"])
    text = (app_dir / ".env").read_text()
    password = env_value(text, "POSTGRES_PASSWORD")
    forms = (password, quote(password, safe="")) if password else ()
    backups = sorted(
        path.name
        for path in app_dir.glob(".env.bak*")
        if path.is_file() and any(form in path.read_text(errors="replace") for form in forms)
    )
    return {
        "password": password,
        "role": env_value(text, "POSTGRES_USERNAME"),
        "database": env_value(text, "POSTGRES_DB"),
        "backups_holding_value": backups,
    }


def docker(*argv):
    return subprocess.run(
        ["docker", *argv], stdin=subprocess.DEVNULL, capture_output=True, timeout=60, check=True
    ).stdout.decode()


def env_names(pairs):
    return sorted({pair.split("=", 1)[0] for pair in pairs or []})


def app_env_names(request):
    label = "label=com.docker.compose.project=" + request["app_id"]
    containers = docker("ps", "--filter", label, "--format", "{{.Names}}").split()
    if len(containers) != 1:
        return {"error": "expected one app container, found %d" % len(containers)}
    config = json.loads(docker("inspect", "--format", "{{json .Config}}", containers[0]))
    baked = json.loads(docker("image", "inspect", "--format", "{{json .Config.Env}}", config["Image"]))
    return {"names": sorted(set(env_names(config["Env"])) - set(env_names(baked)))}


def host_main(request):
    try:
        reply = {"read_superuser": read_superuser, "app_env_names": app_env_names}[request["action"]](request)
    except BaseException as exc:
        reply = {"error": type(exc).__name__}
    print(json.dumps(reply))
"""


class StepFailure(Exception):
    """A failed step. `detail` is an error type or fixed text, never a value."""

    def __init__(self, step: str, detail: str):
        super().__init__(f"step={step} error={detail}")
        self.step = step
        self.detail = detail


def safe_run(
    argv: list[str], *, step: str, input_bytes: bytes | None = None, timeout: float = 120
) -> str:
    """Run a command and return stdout; on failure keep only the error type.

    The failure is raised outside the except block so that no traceback chain can
    reach TimeoutExpired or CalledProcessError, which carry argv and output.
    """
    result = None
    error = None
    try:
        result = subprocess.run(
            argv,
            input=input_bytes,
            capture_output=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL if input_bytes is None else None,
        )
    except Exception as exc:
        error = type(exc).__name__
    if error is not None or result is None:
        raise StepFailure(step, error or "no result")
    if result.returncode != 0:
        raise StepFailure(step, f"exit {result.returncode}")
    return result.stdout.decode("utf-8", "replace")


def printable_name(key: str) -> str:
    """A key as output may show it: a real env name, or a placeholder for anything else."""
    return key if PRINTABLE_NAME.fullmatch(key) else UNNAMED_LINE


def env_names_holding(text: str, value: str) -> list[str]:
    """The printable name of every line containing `value`, including URLs and comments."""
    forms = {value, quote(value, safe="")}
    names = []
    for line in text.splitlines():
        if any(form in line for form in forms):
            name, equals, _ = line.partition("=")
            bare_value = not equals or any(form in name for form in forms)
            names.append(UNNAMED_LINE if bare_value else printable_name(name.strip()))
    return names


def printable_names(keys: set[str]) -> list[str]:
    return sorted({printable_name(key) for key in keys})


def replace_url_line(text: str, url: str) -> str:
    """Change only the URL's line. Exactly one must exist, and no other name may change."""
    count = sum(1 for line in text.splitlines() if line.startswith(URL_KEY + "="))
    if count != 1:
        raise StepFailure("env_line", f"{URL_KEY}: expected one line, found {count}")
    updated = _update_env_file_content(text, {URL_KEY: url})
    if not others_unchanged(text, updated):
        raise StepFailure("env_line", "the edit would change another name")
    return updated


def others_unchanged(before: str, after: str) -> bool:
    return {**_parse_env_text(before), URL_KEY: ""} == {**_parse_env_text(after), URL_KEY: ""}


def atomic_write(path: Path, text: str) -> None:
    """Replace a file atomically, keeping its mode; no window with partial content."""
    mode = path.stat().st_mode & 0o777
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        os.chmod(tmp, mode)
        with os.fdopen(fd, "w", newline="") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def reader_url(source_url: str, password: str, host: str | None, port: int | None) -> str:
    """The same database and options as source_url, as the reader, optionally at a new address."""
    parts = urlsplit(source_url)
    target_host = host or parts.hostname or ""
    if ":" in target_host:
        target_host = f"[{target_host}]"
    target_port = port or parts.port
    netloc = f"{READER_ROLE}:{quote(password, safe='')}@{target_host}"
    if target_port:
        netloc += f":{target_port}"
    return urlunsplit(parts._replace(netloc=netloc))


def render_role_sql(verifier: str) -> str:
    """One transaction that creates the role, or brings an existing one to this state."""
    settings = "".join(
        f"ALTER ROLE {READER_ROLE} SET {name} = '{value}';\n"
        for name, value in ROLE_SETTINGS.items()
    )
    return (
        "SET log_statement = 'none'; SET log_min_duration_statement = -1;\n"
        "BEGIN;\n"
        f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{READER_ROLE}')"
        f" THEN CREATE ROLE {READER_ROLE}; END IF; END $$;\n"
        f"ALTER ROLE {READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"
        f" NOBYPASSRLS INHERIT CONNECTION LIMIT {CONNECTION_LIMIT} PASSWORD '{verifier}';\n"
        f"GRANT {', '.join(ROLE_MEMBERSHIPS)} TO {READER_ROLE};\n"
        f"{settings}"
        "COMMIT;\n"
    )


def login_identity(url: str) -> dict[str, str]:
    """Log in over this URL: who it is and where, or the error type."""
    import psycopg2

    try:
        conn = psycopg2.connect(url, connect_timeout=10)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT current_user, current_database()")
                user, database = cur.fetchone()
        finally:
            conn.close()
        return {"user": user, "database": database}
    except Exception as exc:
        return {"error": type(exc).__name__}


def reader_checks(url: str) -> dict[str, Any]:
    """What the login at this URL can do, as names and booleans. Nothing can change."""
    import psycopg2
    from psycopg2 import errors

    conn = psycopg2.connect(url, connect_timeout=10, application_name="dagster-reader-verify")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls,"
                " rolconnlimit, rolconfig FROM pg_roles WHERE rolname = current_user"
            )
            powers, limit, config = cur.fetchone()
            cur.execute(
                "SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid"
                " WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user)"
                " ORDER BY 1"
            )
            memberships = [row[0] for row in cur.fetchall()]
            cur.execute(
                "SELECT format('%I.%I', n.nspname, c.relname) FROM pg_class c"
                " JOIN pg_namespace n ON n.oid = c.relnamespace"
                " WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f') AND n.nspname = 'public'"
                " AND (CASE WHEN c.relkind = 'S' THEN has_sequence_privilege(c.oid, 'USAGE, UPDATE')"
                " ELSE has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')"
                " END) ORDER BY 1"
            )
            writable = [row[0] for row in cur.fetchall()]
            cur.execute(
                "SELECT has_schema_privilege('public', 'CREATE'),"
                " has_database_privilege(current_database(), 'CREATE'),"
                " current_setting('transaction_read_only') = 'on'"
            )
            schema_create, database_create, read_only = cur.fetchone()

            reads = {}
            for name, sql in READ_CHECKS.items():
                try:
                    cur.execute(sql)
                    cur.fetchall()
                    reads[name] = True
                except psycopg2.Error:
                    reads[name] = False

            # A real write attempt with the read-only default off, then rolled back. Only
            # a privilege error shows that the grants, not the backstop, stopped it.
            insert = "allowed"
            try:
                cur.execute("BEGIN READ WRITE")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute("INSERT INTO runs DEFAULT VALUES")
            except errors.InsufficientPrivilege:
                insert = "refused"
            except psycopg2.Error as exc:
                insert = f"inconclusive ({type(exc).__name__})"
            finally:
                cur.execute("ROLLBACK")
    finally:
        conn.close()
    return {
        "has_power_attribute": powers,
        "connection_limit": limit,
        "settings": dict(item.split("=", 1) for item in config or []),
        "memberships": memberships,
        "writable_relations": writable,
        "can_create_in_public": schema_create,
        "can_create_in_database": database_create,
        "read_only_by_default": read_only,
        "runbook_reads": reads,
        "real_insert": insert,
    }


class Deps:
    """Seams for tests; the defaults talk to the real world."""

    run: Callable[..., str] = staticmethod(safe_run)
    login: Callable[[str], dict[str, str]] = staticmethod(login_identity)
    checks: Callable[[str], dict[str, Any]] = staticmethod(reader_checks)


def host_call(args: argparse.Namespace, deps: Any, action: str) -> dict[str, Any]:
    """Run one action on the host; the program and its request both travel over stdin."""
    request = {
        "action": action,
        "app_id": args.app_id,
        "app_dir": args.app_dir or f"/data/coolify/applications/{args.app_id}",
    }
    program = f"{HOST_PROGRAM}\nhost_main(json.loads({json.dumps(json.dumps(request))}))\n"
    out = deps.run(
        shlex.split(args.ssh) + ["python3", "-"], step=f"host_{action}", input_bytes=program.encode()
    )
    lines = out.strip().splitlines()
    reply = json.loads(lines[-1]) if lines else {"error": "no reply"}
    if "error" in reply:
        raise StepFailure(f"host_{action}", reply["error"])
    return reply


def read_superuser(args: argparse.Namespace, deps: Any) -> dict[str, Any]:
    """The superuser's current login from the host, the only source this script trusts."""
    host = host_call(args, deps, "read_superuser")
    for key in ("password", "role", "database"):
        if not host.get(key):
            raise StepFailure("read_superuser", f"{key} missing from the host env file")
    for key in ("role", "database"):
        if not SAFE_NAME.match(host[key]):
            raise StepFailure("read_superuser", f"unexpected {key} name")
    return host


def apply_role_sql(args: argparse.Namespace, deps: Any, host: dict[str, Any], sql: str) -> None:
    """Over the database's local socket, so no password is needed and none travels."""
    if not SAFE_NAME.match(args.db_container):
        raise StepFailure("apply_role", "unexpected container name")
    deps.run(
        shlex.split(args.ssh)
        + ["docker", "exec", "-i", "-u", "postgres", args.db_container]
        + ["psql", "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1"]
        + ["-U", host["role"], "-d", host["database"]],
        step="apply_role",
        input_bytes=sql.encode(),
    )


def vault(deps: Any, step: str, *argv: str, input_bytes: bytes | None = None) -> str:
    return deps.run(["vaultwarden-agent", *argv], step=step, input_bytes=input_bytes)


def read_notes(args: argparse.Namespace, deps: Any) -> list[dict[str, Any]]:
    return [json.loads(vault(deps, "vault_read", "get", "item", note)) for note in args.vault_note]


def writable_note_id(args: argparse.Namespace, deps: Any) -> str:
    """The one note holding the URL. Fails before anything has changed if it cannot be edited."""
    holders = [n for n in read_notes(args, deps) if URL_KEY in _parse_env_text(n.get("notes") or "")]
    if len(holders) != 1:
        raise StepFailure("vault_read", f"expected one note with {URL_KEY}, found {len(holders)}")
    if any(field.get("name") == "1password-item-id" for field in holders[0].get("fields") or []):
        raise StepFailure("vault_read", "the note is mirrored from 1Password; refusing to edit")
    return holders[0]["id"]


def write_note(deps: Any, note_id: str, url: str) -> bool:
    """Read, change one line, write, read back. Other agents edit these notes too."""
    item = json.loads(vault(deps, "vault_read", "get", "item", note_id))
    before = item.get("notes") or ""
    item["notes"] = replace_url_line(before, url)
    encoded = base64.b64encode(json.dumps(item).encode())
    vault(deps, "vault_write", "edit", "item", note_id, input_bytes=encoded)
    after = json.loads(vault(deps, "vault_read", "get", "item", note_id)).get("notes") or ""
    if _parse_env_text(after).get(URL_KEY) != url:
        raise StepFailure("vault_write", "the vault note did not take the edit")
    return others_unchanged(before, after)


def write_parent(path: Path, url: str) -> bool:
    """Reread right before writing, so another card's new line survives this edit."""
    before = path.read_text()
    atomic_write(path, replace_url_line(before, url))
    return others_unchanged(before, path.read_text())


def checkout_env_files(parent: Path) -> list[Path]:
    """The checkout's other env files: backups taken by hand, not the live one."""
    return sorted(p for p in parent.parent.glob(".env*") if p.is_file() and p != parent)


def scrub_backups(parent: Path, value: str, url: str) -> list[str]:
    """Point any backup in the checkout that holds the superuser value at the reader.

    Everything else in a backup stays, so it can still restore the other names.
    Returns the names of backups still holding the value in some other line.
    """
    left = []
    for path in checkout_env_files(parent):
        text = path.read_text(errors="replace")
        names = env_names_holding(text, value)
        if names == [URL_KEY]:
            atomic_write(path, replace_url_line(text, url))
        elif names:
            left.append(path.name)
    return left


def worktree_env_files(args: argparse.Namespace, deps: Any, parent: Path) -> list[Path]:
    out = deps.run(
        ["git", "-C", str(parent.parent), "worktree", "list", "--porcelain"], step="read_worktrees"
    )
    roots = [Path(line[len("worktree ") :]) for line in out.splitlines() if line.startswith("worktree ")]
    return sorted(
        path
        for root in roots
        if root != parent.parent and root.is_dir()
        for path in root.glob(".env*")
        if path.is_file()
    )


def report(payload: dict[str, Any], problems: list[str]) -> int:
    print(json.dumps({**payload, "problems": problems}, indent=2))
    return 1 if problems else 0


def cmd_provision(args: argparse.Namespace, deps: Any) -> int:
    parent = Path(args.parent_env).resolve()
    source_url = _parse_env_text(parent.read_text()).get(URL_KEY)
    if not source_url:
        raise StepFailure("read_url", f"{URL_KEY} missing from the parent env file")
    note_id = writable_note_id(args, deps)
    host = read_superuser(args, deps)

    alphabet = string.ascii_letters + string.digits
    password = "".join(secrets.choice(alphabet) for _ in range(40))
    url = reader_url(source_url, password, args.url_host, args.url_port)
    apply_role_sql(args, deps, host, render_role_sql(_scram_sha256_verifier(password)))
    # Nothing an agent reads changes unless the new URL works from here, at this address.
    identity = deps.login(url)
    if identity != {"user": READER_ROLE, "database": host["database"]}:
        raise StepFailure("login_test", "the new URL did not log in as the reader on this database")

    parent_others_kept = write_parent(parent, url)
    note_others_kept = write_note(deps, note_id, url)
    backups_left = scrub_backups(parent, host["password"], url)
    problems = []
    if not parent_others_kept:
        problems.append("another name in the parent env file changed during the edit")
    if not note_others_kept:
        problems.append("another name in the vault note changed during the edit")
    if backups_left:
        problems.append("a checkout backup holds the superuser value in an unexpected line")
    return report(
        {
            "role": READER_ROLE,
            "parent_env_updated": True,
            "vault_note_updated": True,
            "checkout_backups_still_holding_value": backups_left,
        },
        problems,
    )


def role_problems(checks: dict[str, Any]) -> list[str]:
    problems = []
    if checks["has_power_attribute"]:
        problems.append("the login has a power attribute such as SUPERUSER")
    if checks["connection_limit"] != CONNECTION_LIMIT:
        problems.append("the connection limit is wrong")
    if checks["settings"] != ROLE_SETTINGS:
        problems.append("the role settings are wrong")
    if checks["memberships"] != ROLE_MEMBERSHIPS:
        problems.append("the role memberships are wrong")
    if checks["writable_relations"]:
        problems.append("the login holds write privileges on a relation")
    if checks["can_create_in_public"] or checks["can_create_in_database"]:
        problems.append("the login can create objects")
    if not checks["read_only_by_default"]:
        problems.append("sessions are not read-only by default")
    if checks["real_insert"] != "refused":
        problems.append("a real insert was not refused by privileges")
    problems += [f"runbook read failed: {name}" for name, ok in checks["runbook_reads"].items() if not ok]
    return problems


def cmd_verify(args: argparse.Namespace, deps: Any) -> int:
    parent = Path(args.parent_env).resolve()
    parent_text = parent.read_text()
    parent_env = _parse_env_text(parent_text)
    notes = read_notes(args, deps)
    note_env: dict[str, str] = {}
    for note in notes:
        note_env.update(_parse_env_text(note.get("notes") or ""))
    host = read_superuser(args, deps)
    value = host["password"]

    problems: list[str] = []
    parent_url, note_url = parent_env.get(URL_KEY), note_env.get(URL_KEY)
    if not parent_url or parent_url != note_url:
        problems.append("the parent env file and the vault note hold different URLs")
    logins = {}
    for name, url in (("parent_env", parent_url), ("vault_note", note_url)):
        logins[name] = deps.login(url) if url else {"error": "missing"}
        if logins[name] != {"user": READER_ROLE, "database": host["database"]}:
            problems.append(f"the {name} URL does not log in as the reader")
    # The checks run only on a proven reader: the insert attempt must never meet a superuser.
    checks = {}
    if logins["parent_env"].get("user") == READER_ROLE:
        checks = deps.checks(parent_url)
        problems += role_problems(checks)

    worktrees = worktree_env_files(args, deps, parent)
    holders = {
        "parent_env": env_names_holding(parent_text, value),
        "checkout_env_files": [
            p.name for p in checkout_env_files(parent) if env_names_holding(p.read_text(errors="replace"), value)
        ],
        "worktree_env_files": [
            f"{p.parent.name}/{p.name}" for p in worktrees if env_names_holding(p.read_text(errors="replace"), value)
        ],
        "vault_notes": [n.get("name") for n in notes if env_names_holding(n.get("notes") or "", value)],
    }
    problems += [f"the superuser value is in {where}" for where, found in holders.items() if found]
    return report(
        {
            "logins": logins,
            "reader": checks,
            "superuser_value_holders": holders,
            # Reported, not problems: removing the host's backups is a separate decision.
            "host_backups_holding_superuser_value": host["backups_holding_value"],
            # Reported, not problems: these copies drift on their own, so this only shows
            # up a lost edit. A provision run checks its own edits against the file it read.
            "parent_and_vault_differ": {
                "only_in_parent": printable_names(set(parent_env) - set(note_env)),
                "only_in_vault": printable_names(set(note_env) - set(parent_env)),
                "different_values": printable_names(
                    {key for key in set(parent_env) & set(note_env) if parent_env[key] != note_env[key]}
                ),
            },
        },
        problems,
    )


def cmd_push_gate(args: argparse.Namespace, deps: Any) -> int:
    """A push regenerates the host `.env` from Coolify's store, removing anything only on the host."""
    store = {line.strip() for line in Path(args.store_names_file).read_text().splitlines() if line.strip()}
    if not store:
        raise StepFailure("push_gate", "the list of store names is empty")
    running = host_call(args, deps, "app_env_names")["names"]
    host_only = printable_names(
        {name for name in running if name not in store and not COOLIFY_METADATA.match(name)}
    )
    problems = ["the app has variables Coolify's store lacks; a push would remove them"] if host_only else []
    return report({"app_names": len(running), "store_names": len(store), "host_only": host_only}, problems)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, fromfile_prefix_chars="@")
    common = argparse.ArgumentParser(add_help=False, fromfile_prefix_chars="@")
    common.add_argument("--ssh", required=True, help="command prefix that reaches the app host")
    common.add_argument("--app-id", required=True, help="Coolify application id")
    common.add_argument("--app-dir", help="default: /data/coolify/applications/<app-id>")
    sub = parser.add_subparsers(dest="command", required=True)

    copies = argparse.ArgumentParser(add_help=False, parents=[common])
    copies.add_argument("--parent-env", required=True, help="the checkout's .env file")
    copies.add_argument(
        "--vault-note", required=True, action="append", help="uuid of a .env vault note; repeat for each"
    )

    provision = sub.add_parser("provision", parents=[copies], fromfile_prefix_chars="@")
    provision.add_argument("--db-container", required=True, help="Dagster database container")
    provision.add_argument("--url-host", help="point both copies of the URL at this host")
    provision.add_argument("--url-port", type=int)
    provision.set_defaults(func=cmd_provision)

    verify = sub.add_parser("verify", parents=[copies], fromfile_prefix_chars="@")
    verify.set_defaults(func=cmd_verify)

    gate = sub.add_parser("push-gate", parents=[common], fromfile_prefix_chars="@")
    gate.add_argument("--store-names-file", required=True, help="Coolify store's names, one per line")
    gate.set_defaults(func=cmd_push_gate)
    return parser


def main(argv: list[str] | None = None, deps: Any = None) -> int:
    args = build_parser().parse_args(argv)
    message = None
    code = 1
    try:
        code = args.func(args, deps or Deps())
    except StepFailure as exc:
        message = f"FAILED step={exc.step} error={exc.detail}"
    except Exception as exc:
        message = f"FAILED step={args.command} error={type(exc).__name__}"
    if message:
        print(message, file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
