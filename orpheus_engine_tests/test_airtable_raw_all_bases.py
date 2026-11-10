from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from dagster import build_asset_context

from orpheus_engine.defs.airtable_raw_all_bases import definitions as sync


@pytest.mark.parametrize("schema_fails", [False, True])
def test_discovered_names_reach_base_rows(monkeypatch, schema_fails):
    # Synthetic metadata spans two pages; ID-only bases have no name.
    infos = [
        {"id": "app_synthetic_a", "name": "Synthetic Base Alpha"},
        {"id": "app_synthetic_b", "name": "Synthetic Base Beta"},
    ]
    api = Mock()
    responses = [
        Mock(json=Mock(return_value={"bases": infos[:1], "offset": "page_two"})),
        Mock(json=Mock(return_value={"bases": infos[1:]})),
    ]
    api.session.get.side_effect = responses
    bases = {
        info["id"]: SimpleNamespace(
            id=info["id"],
            name=None,
            schema=Mock(return_value=SimpleNamespace(tables=[])),
        )
        for info in infos
    }
    if schema_fails:
        bases[infos[1]["id"]].schema.side_effect = ValueError("synthetic failure")
    api.base.side_effect = bases.__getitem__
    queue = Mock(skipped_records=0)
    monkeypatch.setattr(sync, "get_airtable_api", lambda: api)
    monkeypatch.setattr(sync, "get_db_connection", Mock())
    monkeypatch.setattr(sync, "ensure_schema_and_tables", Mock())
    monkeypatch.setattr(sync, "delete_stale_rows", Mock(return_value=(0, 0)))
    monkeypatch.setattr(sync, "WriteQueue", Mock(return_value=queue))
    monkeypatch.setattr(sync.time, "sleep", Mock())
    # Schema failures should reach the executor's error log without retry delays.
    monkeypatch.setattr(sync, "fetch_with_retry", lambda fn, **kwargs: fn())
    log = Mock()

    with build_asset_context() as context:
        # Keep Dagster's valid execution context while capturing sync log messages.
        monkeypatch.setattr(type(context), "log", property(lambda self: log))
        output = sync.airtable_raw_all_bases_sync(context)

    api.session.get.assert_has_calls(
        [
            call("https://api.airtable.com/v0/meta/bases", params={}),
            call("https://api.airtable.com/v0/meta/bases", params={"offset": "page_two"}),
        ],
        any_order=True,
    )
    expected = infos[:1] if schema_fails else infos
    assert sorted(queue.push_base.call_args_list) == sorted(
        call((info["id"], info["name"], "[]")) for info in expected
    )
    queue.flush_all.assert_called_once_with()
    assert output.metadata["base_errors"].value == int(schema_fails)
    if schema_fails:
        log.warning.assert_any_call(
            "Base Synthetic Base Beta (app_synthetic_b) failed: synthetic failure"
        )


# --- Values Postgres jsonb rejects --------------------------------------------
# On 2026-11-10 a few records with such values made whole 10k-row flushes fail,
# and every other base's rows in those batches (all of "YSWS - Aria") were lost.

import glob
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone

import psycopg2

LONE_HIGH_SURROGATE = "\ud83d"


def test_jsonb_safe_drops_nul_and_lone_surrogates():
    cleaned = sync._clean_record_fields(
        {
            "Name": f"a\x00b{LONE_HIGH_SURROGATE}c",
            "Tags": [f"x{LONE_HIGH_SURROGATE}", {"k\x00": "v\x00"}],
            "Emoji": "ok \U0001F600",
            "Count": 3,
        }
    )
    assert cleaned["Name"] == "ab\ufffdc"
    assert cleaned["Tags"] == ["x\ufffd", {"k": "v"}]
    assert cleaned["Emoji"] == "ok \U0001F600"
    assert cleaned["Count"] == 3
    json.dumps(cleaned).encode("utf-8")


def test_literal_backslash_u0000_text_stays_valid_json():
    # The old json.dumps(...).replace("\\u0000", "") cut this text's escape in
    # half and produced invalid JSON.
    text = "path C:\\u0000\\file"
    dumped = json.dumps(sync._clean_record_fields({"f": text}))
    assert json.loads(dumped) == {"f": text}


def _pg_bin_dir():
    initdb = shutil.which("initdb")
    if initdb and shutil.which("pg_ctl"):
        return os.path.dirname(initdb)
    for bindir in sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True):
        if os.path.exists(os.path.join(bindir, "initdb")):
            return bindir
    return None


@pytest.fixture(scope="module")
def pg_socket_dir():
    bindir = _pg_bin_dir()
    if not bindir:
        pytest.skip("initdb/pg_ctl not installed")
    tmp = tempfile.mkdtemp(prefix="at_raw_pg_")
    datadir = os.path.join(tmp, "data")
    subprocess.run(
        [os.path.join(bindir, "initdb"), "-D", datadir, "-U", "postgres", "--auth=trust"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            os.path.join(bindir, "pg_ctl"), "-D", datadir, "-w",
            "-l", os.path.join(tmp, "log"),
            "-o", f"-c listen_addresses='' -k {tmp}",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    try:
        yield tmp
    finally:
        subprocess.run(
            [os.path.join(bindir, "pg_ctl"), "-D", datadir, "-m", "immediate", "stop"],
            capture_output=True,
        )
        shutil.rmtree(tmp, ignore_errors=True)


def test_bad_row_does_not_drop_other_bases_rows(monkeypatch, pg_socket_dir):
    connect = lambda: psycopg2.connect(host=pg_socket_dir, user="postgres", dbname="postgres")
    monkeypatch.setattr(sync, "get_db_connection", connect)
    conn = connect()
    try:
        sync.ensure_schema_and_tables(conn)
    finally:
        conn.close()

    log = Mock()
    queue = sync.WriteQueue(datetime.now(timezone.utc), log)
    good = [("app_other", "tbl_a", f"rec{i}", json.dumps({"n": i})) for i in range(3)]
    cleaned = json.dumps(sync._clean_record_fields({"t": LONE_HIGH_SURROGATE}))
    # A raw lone-surrogate escape, as the old code sent it; jsonb rejects it.
    bad = ("app_bad", "tbl_b", "rec_bad", '{"t": "\\ud83d"}')
    queue.push_records(good[:2] + [bad, ("app_bad", "tbl_b", "rec_ok", cleaned)] + good[2:])
    queue.flush_all()

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT base_id, record_id FROM {sync.SCHEMA_NAME}.records ORDER BY 1, 2")
            stored = cur.fetchall()
    finally:
        conn.close()
    assert stored == [
        ("app_bad", "rec_ok"),
        ("app_other", "rec0"),
        ("app_other", "rec1"),
        ("app_other", "rec2"),
    ]
    assert queue.skipped_records == 1
