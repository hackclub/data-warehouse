"""
Tests for loops_campaign_recipient_metrics writing each campaign as it goes.

The asset used to keep every recipient row in memory and leave all writes to
loops_campaign_metrics_to_warehouse, so a run killed at the job's 6 h limit
wrote nothing (2026-10). Now each campaign is upserted and stamped right after
its fetch. The Loops tRPC calls and psycopg2 are mocked; no network or DB.
"""

import inspect
from types import SimpleNamespace

import polars as pl
import pytest

import dagster as dg

import orpheus_engine.defs.loops_campaign_and_metrics_export.definitions as loops_defs


class FakeCursor:
    def __init__(self, events, fail_on=None):
        self.events = events
        self.fail_on = fail_on

    def execute(self, sql, params=None):
        sql_upper = " ".join(sql.split()).upper()
        if sql_upper.startswith("CREATE TABLE"):
            self.events.append(("create",))
        elif sql_upper.startswith("UPDATE LOOPS.CAMPAIGNS"):
            if self.fail_on == "update":
                raise RuntimeError("warehouse write failed")
            self.events.append(("stamp", params[0]))
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, events, fail_on=None):
        self.events = events
        self.fail_on = fail_on

    def cursor(self):
        return FakeCursor(self.events, self.fail_on)

    def commit(self):
        self.events.append(("commit",))

    def close(self):
        self.events.append(("close",))


def _page(emails):
    return {"result": {"data": {"json": {"emailMetrics": {"emails": emails}}}}}


@pytest.fixture
def harness(monkeypatch):
    """Patches Loops and psycopg2; returns the shared ordered event log."""
    events = []
    state = SimpleNamespace(events=events, fail_on=None, raise_on_fetch={})

    monkeypatch.setenv("WAREHOUSE_COOLIFY_URL", "postgresql://fake")
    monkeypatch.delenv("LOOPS_METRICS_TIME_BUDGET_SECONDS", raising=False)
    monkeypatch.setattr(loops_defs.time, "sleep", lambda _s: None)

    def fake_resolve(_token, campaign_id):
        events.append(("resolve", campaign_id))
        if campaign_id == "unresolvable":
            return None
        return {"email_message_id": f"msg-{campaign_id}"}

    def fake_fetch_page(_token, email_message_id, page=0, page_size=100):
        campaign_id = email_message_id.removeprefix("msg-")
        events.append(("fetch", campaign_id, page))
        if campaign_id in state.raise_on_fetch:
            raise state.raise_on_fetch[campaign_id]
        if campaign_id == "empty":
            return _page([])
        return _page([
            {
                "email": f"{n}@{campaign_id}.example",
                "vendorDeliveredAt": "2026-10-01T00:00:00Z",
                "vendorOpen": 1,
                "vendorClick": 0,
            }
            for n in range(2)
        ])

    def fake_execute_values(_cursor, _sql, rows, page_size=1000):
        events.append(("upsert", rows[0][0], len(rows)))

    monkeypatch.setattr(loops_defs, "_resolve_email_message_id_from_campaign", fake_resolve)
    monkeypatch.setattr(loops_defs, "_fetch_campaign_metrics_page", fake_fetch_page)
    monkeypatch.setattr("psycopg2.extras.execute_values", fake_execute_values)
    monkeypatch.setattr(
        "psycopg2.connect", lambda _dsn: FakeConnection(events, state.fail_on)
    )
    return state


def _due(*campaign_ids):
    return pl.DataFrame({
        "id": list(campaign_ids),
        "name": [f"Campaign {c}" for c in campaign_ids],
        "emoji": [None] * len(campaign_ids),
    })


def _run(due):
    context = dg.build_asset_context(
        resources={"loops_session_token": SimpleNamespace(get_value=lambda: "token")}
    )
    return loops_defs.loops_campaign_recipient_metrics(context, due)


def _without_setup(events):
    """Drops the one-time CREATE TABLE connection at the start."""
    first_resolve = next(i for i, e in enumerate(events) if e[0] == "resolve")
    return events[first_resolve:]


def test_each_campaign_is_upserted_and_stamped_before_next_fetch(harness):
    summary = _run(_due("a", "b", "c"))

    assert harness.events[:3] == [("create",), ("commit",), ("close",)]
    assert _without_setup(harness.events) == [
        event
        for c in ("a", "b", "c")
        for event in [
            ("resolve", c),
            ("fetch", c, 0),
            ("upsert", c, 2),
            ("stamp", c),
            ("commit",),
            ("close",),
        ]
    ]
    assert summary.to_dicts() == [
        {"campaign_id": c, "recipients": 2} for c in ("a", "b", "c")
    ]


def test_interrupt_mid_loop_keeps_earlier_campaigns_written(harness):
    harness.raise_on_fetch["c"] = dg.DagsterExecutionInterruptedError()

    with pytest.raises(dg.DagsterExecutionInterruptedError):
        _run(_due("a", "b", "c", "d"))

    events = harness.events
    for c in ("a", "b"):
        stamp = events.index(("stamp", c))
        assert ("upsert", c, 2) in events
        assert events[stamp + 1] == ("commit",)
    assert ("stamp", "c") not in events
    assert ("resolve", "d") not in events


def test_fetch_error_skips_campaign_without_stamping(harness):
    harness.raise_on_fetch["b"] = RuntimeError("Loops 500")

    summary = _run(_due("a", "b", "c"))

    assert ("stamp", "a") in harness.events
    assert ("stamp", "b") not in harness.events
    assert ("stamp", "c") in harness.events
    assert summary["campaign_id"].to_list() == ["a", "c"]


def test_unresolvable_campaign_is_not_stamped(harness):
    summary = _run(_due("unresolvable", "a"))

    assert ("stamp", "unresolvable") not in harness.events
    assert summary["campaign_id"].to_list() == ["a"]


def test_campaign_with_no_recipients_is_still_stamped(harness):
    summary = _run(_due("empty"))

    assert not any(e[0] == "upsert" for e in harness.events)
    assert ("stamp", "empty") in harness.events
    assert summary.to_dicts() == [{"campaign_id": "empty", "recipients": 0}]


def test_warehouse_write_error_fails_the_run(harness):
    harness.fail_on = "update"

    with pytest.raises(RuntimeError, match="warehouse write failed"):
        _run(_due("a", "b"))

    assert ("resolve", "b") not in harness.events


def test_spent_time_budget_stops_before_any_campaign(harness, monkeypatch):
    monkeypatch.setenv("LOOPS_METRICS_TIME_BUDGET_SECONDS", "0")

    summary = _run(_due("a", "b"))

    assert summary.height == 0
    assert not any(e[0] == "resolve" for e in harness.events)


def test_no_due_campaigns_touches_nothing(harness):
    summary = _run(_due())

    assert summary.height == 0
    assert harness.events == []


def test_metrics_to_warehouse_is_summary_only(harness):
    summary = pl.DataFrame(
        {"campaign_id": ["a", "b"], "recipients": [2, 3]},
        schema=loops_defs.CAMPAIGN_METRICS_SUMMARY_SCHEMA,
    )
    result = loops_defs.loops_campaign_metrics_to_warehouse(
        dg.build_asset_context(), summary
    )

    assert result.metadata["num_campaigns"].value == 2
    assert result.metadata["num_records"].value == 5
    assert harness.events == []


def test_weekly_branch_covers_campaigns_sent_one_to_two_weeks_ago():
    source = inspect.getsource(
        loops_defs.loops_campaigns_needing_metrics_fetch.op.compute_fn.decorated_fn
    )

    assert "(sent_at < NOW() - INTERVAL '1 week'" in source
    assert "INTERVAL '2 weeks'" not in source


def test_expired_cookie_fails_the_run_and_keeps_earlier_campaigns(harness):
    # 2026-10-02: the cookie expired mid-run; each later campaign was logged
    # as a per-campaign failure and the run still succeeded.
    harness.raise_on_fetch["b"] = loops_defs.LoopsAuthError("HTTP 401")

    with pytest.raises(loops_defs.LoopsAuthError):
        _run(_due("a", "b", "c"))

    assert ("stamp", "a") in harness.events
    assert ("stamp", "b") not in harness.events
    assert ("resolve", "c") not in harness.events


@pytest.mark.parametrize("status", [401, 403])
def test_metrics_page_raises_auth_error_on_rejected_cookie(monkeypatch, status):
    response = SimpleNamespace(status_code=status, text='{"error":"UNAUTHORIZED"}')
    monkeypatch.setattr(loops_defs.requests, "get", lambda *a, **k: response)

    with pytest.raises(loops_defs.LoopsAuthError, match="LOOPS_SESSION_TOKEN"):
        loops_defs._fetch_campaign_metrics_page("token", "msg-a")


def test_incomplete_campaigns_are_due_daily_and_first():
    # 2026-10-02: a 401 at page 1,199 left "Dev @ Haven Orgwide Announcement"
    # with 119,900 of 243,779 recipients, stamped as fetched for 30 days.
    source = inspect.getsource(
        loops_defs.loops_campaigns_needing_metrics_fetch.op.compute_fn.decorated_fn
    )

    assert "COALESCE(rc.recipients, 0) < sent_count * 0.98" in source
    order_by = source.split("ORDER BY", 1)[1]
    assert order_by.index("IS NULL DESC") < order_by.index("sent_count * 0.98") < order_by.index(
        "metrics_last_fetched_at\n"
    )
