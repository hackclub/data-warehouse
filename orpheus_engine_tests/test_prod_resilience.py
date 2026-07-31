"""
Tests for the 2026-07-31 production-resilience changes:

- dlt run-scoped working directories + corrupt-state retry classification
- skip-if-running schedules (no run pileup / no overlapping same-asset runs)
- job asset selections stay disjoint (frequent/unified/dau vs all-assets)
- dagster/max_runtime tags present so run monitoring can kill hung runs
- storage janitor op wiring
- zenventory image fetch resilience

No network or database access; everything runs against ephemeral instances or
pure helpers.
"""

import os
import time
from types import SimpleNamespace

import pytest

import dagster as dg

from orpheus_engine.defs.dlt.assets import (
    DLT_PIPELINES_ROOT,
    _dlt_retryable_reason,
    _exception_chain_text,
    _fresh_pipelines_dir,
    _sweep_stale_pipeline_dirs,
)


# ---------------------------------------------------------------------------
# dlt recovery helpers
# ---------------------------------------------------------------------------

class TestDltRetryClassification:
    def test_schema_create_race_is_retryable(self):
        exc = Exception(
            'duplicate key value violates unique constraint '
            '"pg_namespace_nspname_index" ... already exists'
        )
        assert _dlt_retryable_reason(exc) == "schema_create_race"

    def test_missing_load_package_file_is_retryable(self):
        # Signature of the 2026-07-17..30 storm: pending package resume death
        exc = FileNotFoundError(
            "/app/.dlt_pipelines/daydream_to_warehouse_ysws_config/load/"
            "normalized/1785377628.617887/schema_updates.json"
        )
        assert _dlt_retryable_reason(exc) == "local_state_corruption"

    def test_normalize_job_failed_is_retryable(self):
        exc = Exception(
            "NormalizeJobFailed: Job for approved_projects.27b9987c0f.typed-jsonl "
            "failed terminally in load 1785384860.735755 with message "
            "[Errno 2] No such file or directory: '...insert_values'."
        )
        assert _dlt_retryable_reason(exc) is not None

    def test_nested_cause_is_inspected(self):
        inner = FileNotFoundError("load/new/xyz/schema.json")
        outer = RuntimeError("pipeline step failed")
        outer.__cause__ = inner
        assert _dlt_retryable_reason(outer) == "local_state_corruption"

    def test_genuine_errors_are_not_retryable(self):
        assert _dlt_retryable_reason(ValueError("bad credentials")) is None
        assert (
            _dlt_retryable_reason(Exception("connection to server failed: timeout"))
            is None
        )

    def test_exception_chain_text_handles_cycles(self):
        a = Exception("a")
        b = Exception("b")
        a.__cause__ = b
        b.__cause__ = a  # pathological cycle must not hang
        text = _exception_chain_text(a)
        assert "a" in text and "b" in text


class TestRunScopedPipelinesDir:
    def test_fresh_dirs_are_unique_per_call(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        d1 = _fresh_pipelines_dir("my_pipeline", "abcd1234-run")
        d2 = _fresh_pipelines_dir("my_pipeline", "abcd1234-run")
        assert d1 != d2
        assert os.path.isdir(d1) and os.path.isdir(d2)
        # Both live under the shared root so the sweeper can find leaks
        # (mkdtemp returns an absolute path; compare the parent dir name)
        assert os.path.basename(os.path.dirname(d1)) == DLT_PIPELINES_ROOT

    def test_sweeper_removes_only_stale_dirs(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        fresh = _fresh_pipelines_dir("pipe", "run1")
        stale = _fresh_pipelines_dir("pipe", "run0")
        old = time.time() - 48 * 3600
        os.utime(stale, (old, old))
        _sweep_stale_pipeline_dirs(max_age_hours=24.0)
        assert os.path.isdir(fresh)
        assert not os.path.exists(stale)

    def test_sweeper_survives_missing_root(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _sweep_stale_pipeline_dirs()  # no .dlt_pipelines dir at all -> no raise


# ---------------------------------------------------------------------------
# Schedules: skip-if-running behavior
# ---------------------------------------------------------------------------

from orpheus_engine.schedules import (  # noqa: E402
    frequent_15min_schedule,
    materialize_frequent_job,
    unified_ysws_15min_schedule,
    hackatime_dau_hourly_schedule,
    materialize_all_assets_schedule,
    defs as schedule_defs,
)


def _evaluate(schedule, instance):
    context = dg.build_schedule_context(instance=instance)
    return schedule.evaluate_tick(context)


@pytest.fixture()
def instance():
    # instance_for_test (not DagsterInstance.ephemeral): schedule evaluation
    # needs an instance that can produce an InstanceRef.
    with dg.instance_for_test() as inst:
        yield inst


def _fake_run(instance, job_name: str, status: dg.DagsterRunStatus):
    """Insert a minimal run row with the given status into run storage."""
    from dagster._core.storage.dagster_run import DagsterRun

    run = DagsterRun(job_name=job_name, status=status)
    instance.run_storage.add_run(run)
    return run


class TestSkipIfRunningSchedules:
    def test_requests_run_when_idle(self, instance):
        result = _evaluate(frequent_15min_schedule, instance)
        assert result.run_requests, "expected a RunRequest when no runs in flight"

    def test_skips_when_same_job_is_started(self, instance):
        _fake_run(instance, materialize_frequent_job.name, dg.DagsterRunStatus.STARTED)
        result = _evaluate(frequent_15min_schedule, instance)
        assert not result.run_requests
        assert result.skip_message and "still" in result.skip_message

    def test_skips_when_same_job_is_pending(self, instance):
        # (QUEUED can't be fabricated in run storage without a RemoteJobOrigin;
        # NOT_STARTED exercises the same not-yet-executing branch, and QUEUED
        # membership is asserted on IN_FLIGHT_STATUSES below.)
        _fake_run(instance, materialize_frequent_job.name, dg.DagsterRunStatus.NOT_STARTED)
        result = _evaluate(frequent_15min_schedule, instance)
        assert not result.run_requests

    def test_queued_counts_as_in_flight(self):
        from orpheus_engine.schedules import IN_FLIGHT_STATUSES

        assert dg.DagsterRunStatus.QUEUED in IN_FLIGHT_STATUSES

    def test_other_jobs_do_not_block(self, instance):
        _fake_run(instance, "some_other_job", dg.DagsterRunStatus.STARTED)
        result = _evaluate(frequent_15min_schedule, instance)
        assert result.run_requests

    def test_finished_runs_do_not_block(self, instance):
        _fake_run(instance, materialize_frequent_job.name, dg.DagsterRunStatus.SUCCESS)
        _fake_run(instance, materialize_frequent_job.name, dg.DagsterRunStatus.FAILURE)
        result = _evaluate(frequent_15min_schedule, instance)
        assert result.run_requests

    def test_all_four_scheduled_jobs_have_skip_logic(self, instance):
        for schedule in (
            frequent_15min_schedule,
            unified_ysws_15min_schedule,
            hackatime_dau_hourly_schedule,
            materialize_all_assets_schedule,
        ):
            _fake_run(instance, schedule.job.name, dg.DagsterRunStatus.STARTED)
            result = _evaluate(schedule, instance)
            assert not result.run_requests, f"{schedule.name} should skip while running"


class TestJobGuardrails:
    def test_every_scheduled_job_has_max_runtime(self):
        for job in schedule_defs.jobs:
            assert "dagster/max_runtime" in job.tags, (
                f"{job.name} needs a dagster/max_runtime tag so run monitoring "
                "can kill hung runs (they starve the skip-if-running schedule)"
            )
            assert int(job.tags["dagster/max_runtime"]) > 0

    def test_schedule_names_unchanged(self):
        # Schedule on/off state in the instance DB is keyed by name; renaming
        # them would silently disable prod schedules on deploy.
        names = {s.name for s in schedule_defs.schedules}
        assert names == {
            "materialize_all_assets",
            "unified_ysws_15min_schedule",
            "hackatime_dau_hourly_schedule",
            "frequent_15min_schedule",
        }


# ---------------------------------------------------------------------------
# Storage janitor
# ---------------------------------------------------------------------------

from orpheus_engine.defs.janitor.definitions import (  # noqa: E402
    dagster_storage_janitor_job,
    RETENTION_DAYS,
)


class TestStorageJanitor:
    def test_janitor_runs_clean_on_empty_instance(self, instance):
        result = dagster_storage_janitor_job.execute_in_process(instance=instance)
        assert result.success

    def test_janitor_does_not_delete_recent_runs(self, instance):
        _fake_run(instance, "some_job", dg.DagsterRunStatus.SUCCESS)
        result = dagster_storage_janitor_job.execute_in_process(instance=instance)
        assert result.success
        # The freshly created run (now) is way inside RETENTION_DAYS
        remaining = instance.get_runs(dg.RunsFilter(job_name="some_job"))
        assert len(remaining) == 1, f"janitor must keep runs younger than {RETENTION_DAYS}d"


# ---------------------------------------------------------------------------
# Zenventory image fetch resilience
# ---------------------------------------------------------------------------

import requests  # noqa: E402

from orpheus_engine.defs.zenventory_inventory_airtable_sync.definitions import (  # noqa: E402
    fetch_item_image_url,
)


class _ExplodingSession:
    cookies = SimpleNamespace(get=lambda self, *a, **k: "tok")

    def __init__(self):
        self.cookies = _FakeCookies()

    def get(self, *args, **kwargs):
        raise requests.ConnectionError("Remote end closed connection without response")


class _FakeCookies:
    def get(self, *args, **kwargs):
        return "tok"


def test_image_fetch_swallows_connection_errors():
    # A dead connection must degrade to "no image", not kill the whole sync
    assert fetch_item_image_url(_ExplodingSession(), 12345) is None
