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
    queue = Mock()
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
