from datetime import datetime, timezone

import dagster as dg
from dagster._core.errors import DagsterExecutionInterruptedError

from orpheus_engine.defs.sling.programs import stardance as sd


def test_stardance_full_refresh_config_preserves_disabled_and_select_exclusions():
    cfg = sd._stardance_full_refresh_config()
    assert cfg["defaults"]["mode"] == "full-refresh"
    assert cfg["streams"]["public.active_insights_requests"] == {"disabled": True}
    assert cfg["streams"]["public.active_insights_jobs"] == {"disabled": True}
    assert cfg["streams"]["public.privacy_token_uses"] == {"disabled": True}
    assert "select" in cfg["streams"]["public.users"]
    assert "session_token" not in cfg["streams"]["public.users"]["select"]
    assert cfg["streams"]["public.versions"] is None


def test_stardance_full_refresh_defaults_do_not_drop_incremental_options():
    """F4: fail if incremental defaults ever gain source/target options that full refresh omits."""
    inc_defaults = sd.stardance_replication_config["defaults"]
    full_defaults = sd._stardance_full_refresh_config()["defaults"]
    incremental_only_keys = {"mode", "primary_key", "update_key"}
    for key, val in inc_defaults.items():
        if key not in incremental_only_keys:
            assert full_defaults.get(key) == val, f"full refresh dropped default option {key!r}"


def test_should_run_daily_full_refresh_window_and_dedup():
    before_window = datetime(2026, 11, 25, 2, 45, tzinfo=timezone.utc)
    in_window = datetime(2026, 11, 25, 3, 0, tzinfo=timezone.utc)
    assert not sd._should_run_daily_full_refresh(before_window, None)
    assert sd._should_run_daily_full_refresh(in_window, None)
    assert sd._should_run_daily_full_refresh(in_window, "2026-11-24")
    assert not sd._should_run_daily_full_refresh(in_window, "2026-11-25")


def test_real_instance_circuit_breaker_success_fail_killed_and_carry_forward(monkeypatch):
    """End-to-end test on a real Dagster test instance with frozen UTC clock:
    - before window -> incremental
    - Day 1 03:00 fails -> falls back to incremental at 03:15 and 04:00 (circuit breaker)
    - Day 2 02:00 -> incremental; Day 2 03:00 succeeds -> full-refresh; 03:15 & 04:00 -> incremental (carry-forward)
    - Day 3 03:00 killed (DagsterExecutionInterruptedError) -> Day 3 04:00 falls back to incremental
    - Day 4 03:00 -> tries full-refresh again and succeeds
    """
    modes: list[str] = []

    class FakeSling(dg.ConfigurableResource):
        outcome: str = "ok"

        def replicate(self, context, replication_config):
            modes.append(replication_config["defaults"]["mode"])
            if self.outcome == "fail":
                raise RuntimeError("transient failure")
            if self.outcome == "kill":
                raise DagsterExecutionInterruptedError("killed by max_runtime")
            return iter([])

    def _run(inst, now, outcome="ok"):
        class FakeDT(datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        monkeypatch.setattr(sd, "datetime", FakeDT)
        monkeypatch.setattr(sd, "_drop_invalid_indexes", lambda *a, **k: None)
        monkeypatch.setattr(sd, "_ensure_incremental_target_indexes", lambda *a, **k: None)
        return dg.materialize(
            [sd.stardance_warehouse_mirror],
            instance=inst,
            resources={"sling": FakeSling(outcome=outcome)},
            raise_on_error=False,
        )

    with dg.instance_for_test() as inst:
        d = lambda day, h, m=0: datetime(2026, 11, day, h, m, tzinfo=timezone.utc)

        # Day 1: before window -> incremental
        assert _run(inst, d(25, 2, 45)).success
        # Day 1: 03:00 full-refresh fails -> circuit breaker trips for rest of Day 1
        assert not _run(inst, d(25, 3, 0), outcome="fail").success
        assert _run(inst, d(25, 3, 15)).success
        assert _run(inst, d(25, 4, 0)).success

        # Day 2: before window -> incremental; 03:00 full-refresh succeeds; later ticks carry forward
        assert _run(inst, d(26, 2, 0)).success
        assert _run(inst, d(26, 3, 0)).success
        assert _run(inst, d(26, 3, 15)).success
        assert _run(inst, d(26, 4, 0)).success

        # Day 3: 03:00 killed by run monitoring -> Observation persists -> 04:00 falls back to incremental
        assert not _run(inst, d(27, 3, 0), outcome="kill").success
        assert _run(inst, d(27, 4, 0)).success

        # Day 4: 03:00 retries full-refresh and succeeds
        assert _run(inst, d(28, 3, 0)).success

    assert modes == [
        "incremental",   # Day 1 02:45
        "full-refresh",  # Day 1 03:00 (fail)
        "incremental",   # Day 1 03:15 (breaker fallback)
        "incremental",   # Day 1 04:00 (breaker fallback)
        "incremental",   # Day 2 02:00
        "full-refresh",  # Day 2 03:00 (ok)
        "incremental",   # Day 2 03:15 (carry-forward)
        "incremental",   # Day 2 04:00 (carry-forward)
        "full-refresh",  # Day 3 03:00 (killed)
        "incremental",   # Day 3 04:00 (breaker fallback after kill)
        "full-refresh",  # Day 4 03:00 (ok)
    ], modes


def test_breaker_not_masked_by_newer_unrelated_observation(monkeypatch):
    """F1: a newer AssetObservation without full_refresh_attempt_date must not mask the breaker."""
    modes: list[str] = []
    drop_calls: list[str] = []
    lock_timeouts: list[int | None] = []

    class FakeSling(dg.ConfigurableResource):
        outcome: str = "ok"

        def replicate(self, context, replication_config):
            modes.append(replication_config["defaults"]["mode"])
            if self.outcome == "fail":
                raise RuntimeError("boom")
            return iter([])

    def _run(inst, now, outcome="ok"):
        class FakeDT(datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        monkeypatch.setattr(sd, "datetime", FakeDT)
        monkeypatch.setattr(
            sd, "_drop_invalid_indexes", lambda ctx, schema: drop_calls.append(schema)
        )
        monkeypatch.setattr(
            sd,
            "_ensure_incremental_target_indexes",
            lambda ctx, cfg, lock_timeout_ms=None: lock_timeouts.append(lock_timeout_ms),
        )
        return dg.materialize(
            [sd.stardance_warehouse_mirror],
            instance=inst,
            resources={"sling": FakeSling(outcome=outcome)},
            raise_on_error=False,
        )

    with dg.instance_for_test() as inst:
        t1 = datetime(2026, 11, 25, 3, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 25, 3, 15, tzinfo=timezone.utc)

        # 03:00 full refresh fails (logs full_refresh_attempt_date observation)
        assert not _run(inst, t1, outcome="fail").success
        # Log a newer unrelated observation on the same asset key
        inst.report_runless_asset_event(
            dg.AssetObservation(
                asset_key=dg.AssetKey("stardance_warehouse_mirror"),
                metadata={"unrelated_probe": True},
            )
        )
        # 03:15 tick must STILL see the attempt observation and fall back to incremental
        assert _run(inst, t2).success

    assert modes == ["full-refresh", "incremental"]
    # F2 & F4: incremental path drops invalid indexes and sets lock_timeout_ms
    assert drop_calls == ["stardance"]
    assert lock_timeouts == [sd.STARDANCE_INDEX_LOCK_TIMEOUT_MS]


def test_ensure_target_indexes_sets_lock_timeout(monkeypatch):
    """F4: _ensure_target_indexes applies SET lock_timeout when lock_timeout_ms is provided."""
    from orpheus_engine.defs.sling import _helpers

    executed: list[tuple] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, query, params=None):
            executed.append((str(query), params))

        def fetchone(self):
            return (None,)

    class FakeConn:
        autocommit = False

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    class FakeSQL:
        @staticmethod
        def SQL(tmpl):
            class _Q:
                def format(self, *a, **k):
                    return tmpl
            return _Q()

        @staticmethod
        def Identifier(*parts):
            class _Id:
                def as_string(self, conn):
                    return ".".join(f'"{p}"' for p in parts)
            return _Id()

    monkeypatch.setenv("WAREHOUSE_COOLIFY_URL", "postgresql://u:p@localhost/db")
    monkeypatch.setattr(_helpers.psycopg2, "connect", lambda url: FakeConn())
    monkeypatch.setattr(_helpers, "sql", FakeSQL)
    ctx = dg.build_asset_context()
    _helpers._ensure_target_indexes(
        ctx,
        [("stardance", "versions", ("id",))],
        lock_timeout_ms=30_000,
    )
    assert any(q == "SET lock_timeout = %s" and p == ("30000ms",) for q, p in executed)
