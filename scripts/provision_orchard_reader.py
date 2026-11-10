"""Create an Orchard-native read-only login for a warehouse program mirror.

Since about 2026-11-09 Orchard refuses exec into database pods, and an app's
owner role has no CREATEROLE, so the hc_warehouse_reader setup in
runbooks/prod-health-monitoring.md §8 step 2 cannot run on a new database.
This script uses Orchard's own create_database_user (access "read") instead,
through the `pdw` CLI's hack_club_orchard_k8s connection.

The login is named hc_warehouse_orchard_reader. Orchard makes it a member of
pg_read_all_data, so Postgres cannot narrow it to columns: the replication
config's `-col` exclusions are the only thing keeping excluded columns out of
the warehouse. The different name makes the §9 column-grant check skip it.

For each VAR=<databaseId> it (re)creates the login, waits for Orchard to apply
it, checks it can read but not write, and writes VAR=<public URL> into the
staged env file (mode 600). It prints no secrets. Afterwards run
scripts/provision_warehouse_reader.py --check against the staged file.

    PYTHONPATH=. .venv/bin/python scripts/provision_orchard_reader.py \\
        --env-file <staged.env> MACONDO_K8S_URL=<databaseId> ...
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlsplit

import psycopg2

READER = "hc_warehouse_orchard_reader"
SYSTEM_CA = "/etc/ssl/certs/ca-certificates.crt"
LOGIN_ATTEMPTS = 12
LOGIN_RETRY_SECONDS = 10


def _orchard(tool: str, data: dict) -> object:
    out = subprocess.run(
        ["pdw", "call", f"hack_club_orchard_k8s__{tool}", "--output", "json", "--data", json.dumps(data)],
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(json.loads(out.stdout)["content"][0]["text"])
    except (ValueError, KeyError, IndexError):
        # Orchard error text never carries the password; still keep it short.
        raise RuntimeError(f"Orchard {tool} failed: {out.stdout[:200]}") from None


def reader_url(owner_public_url: str, password: str) -> str:
    """The reader's URL on the database's public endpoint, in the shape the
    other *_K8S_URL values use (_sling_connection_url strips sslrootcert)."""
    owner = urlsplit(owner_public_url)
    return (
        f"postgresql://{READER}:{quote(password, safe='')}@{owner.hostname}:{owner.port}{owner.path}"
        "?sslmode=verify-full&sslrootcert=system"
    )


def set_env_line(lines: list[str], name: str, value: str) -> list[str]:
    return [line for line in lines if not line.startswith(f"{name}=")] + [f"{name}={value}"]


def _connect_when_ready(url: str) -> "psycopg2.extensions.connection":
    ca = os.environ.get("SSL_CERT_FILE", SYSTEM_CA)
    for attempt in range(LOGIN_ATTEMPTS):
        try:
            return psycopg2.connect(url.replace("sslrootcert=system", f"sslrootcert={ca}"), connect_timeout=15)
        except psycopg2.OperationalError as exc:
            if "password authentication failed" not in str(exc) or attempt == LOGIN_ATTEMPTS - 1:
                raise RuntimeError(f"reader login failed ({type(exc).__name__})") from None
            time.sleep(LOGIN_RETRY_SECONDS)
    raise AssertionError("unreachable")


def _assert_read_only(url: str) -> None:
    conn = _connect_when_ready(url)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT current_user,"
                " pg_has_role(current_user, 'pg_write_all_data', 'member'),"
                " (SELECT count(*) FROM pg_class k JOIN pg_namespace n ON n.oid = k.relnamespace"
                "  WHERE k.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema')"
                "  AND has_table_privilege(k.oid, 'INSERT'))"
            )
            who, writer, writable = cur.fetchone()
    finally:
        conn.close()
    if who != READER or writer or writable:
        raise RuntimeError(f"{who} is not read-only (pg_write_all_data={writer}, writable tables={writable})")


def provision(var: str, database_id: str) -> str:
    owner = _orchard("get_database_connection", {"databaseId": database_id})
    users = _orchard("list_database_users", {"databaseId": database_id})
    items = users if isinstance(users, list) else users.get("items", [])
    if any(user.get("name") == READER for user in items):
        # Orchard shows a password only at creation, so a lost one means a new login.
        _orchard("delete_database_user", {"databaseId": database_id, "name": READER})
    created = _orchard("create_database_user", {"databaseId": database_id, "name": READER, "access": "read"})
    url = reader_url(owner["publicConnectionString"], created["password"])
    _assert_read_only(url)
    return url


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, required=True, help="staged env file to update (mode 600)")
    parser.add_argument("targets", nargs="+", metavar="VAR=DATABASE_ID")
    args = parser.parse_args(argv)

    os.umask(0o077)
    lines = args.env_file.read_text().splitlines() if args.env_file.exists() else []
    for target in args.targets:
        var, _, database_id = target.partition("=")
        if not var or not database_id:
            parser.error(f"expected VAR=DATABASE_ID, got {target!r}")
        url = provision(var, database_id)
        lines = set_env_line(lines, var, url)
        args.env_file.write_text("\n".join(lines) + "\n")  # after each, so no password is lost
        endpoint = urlsplit(url)
        print(f"{var}: {READER} reads, cannot write, on {endpoint.hostname.split('.')[0]}:{endpoint.port}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
