"""
Dagster storage janitor.

The Dagster instance database retains every run and its event logs forever by
default. By 2026-07-31 that had grown to 163 GB (160 GB of it event_logs, 48.5k
runs back to 2025-04-28), which bloats backups, slows the UI, and makes the
run-status queries the daemon issues every few seconds more expensive.

This job deletes terminal runs (and, via the storage layer, their event logs)
older than RETENTION_DAYS. Deletion is batched and capped per execution so a
single janitor run stays cheap; the initial ~1 year backlog drains over a
couple of weeks of daily runs.

Note: deleting runs does not shrink the Postgres files on disk -- it makes the
space reusable so the DB stops growing. A one-off VACUUM FULL / pg_repack of
event_logs can reclaim the disk space afterwards if wanted.
"""

from datetime import datetime, timedelta, timezone

import dagster as dg

# How much run history to keep. Runs older than this have their event logs
# deleted with them, so keep enough to debug seasonal patterns.
RETENTION_DAYS = 120

# Safety cap per janitor execution so a huge backlog can't produce a
# multi-hour delete storm. 2,000/day comfortably outpaces the ~130 runs/day
# the schedules create.
MAX_DELETES_PER_EXECUTION = 2_000

# Page size for fetching candidate runs.
FETCH_BATCH_SIZE = 200

TERMINAL_STATUSES = [
    dg.DagsterRunStatus.SUCCESS,
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
]


@dg.op
def purge_old_run_history(context: dg.OpExecutionContext) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    instance = context.instance

    deleted = 0
    while deleted < MAX_DELETES_PER_EXECUTION:
        records = instance.get_run_records(
            filters=dg.RunsFilter(
                statuses=TERMINAL_STATUSES,
                created_before=cutoff,
            ),
            limit=min(FETCH_BATCH_SIZE, MAX_DELETES_PER_EXECUTION - deleted),
            ascending=True,  # oldest first
        )
        if not records:
            break
        for record in records:
            instance.delete_run(record.dagster_run.run_id)
            deleted += 1
        context.log.info(f"Deleted {deleted} runs so far (oldest batch first)...")

    remaining = instance.get_runs_count(
        filters=dg.RunsFilter(statuses=TERMINAL_STATUSES, created_before=cutoff)
    )
    context.log.info(
        f"Janitor done: deleted {deleted} runs older than {RETENTION_DAYS} days "
        f"(cutoff {cutoff.isoformat()}); {remaining} still pending deletion in "
        "future executions."
    )


@dg.job(
    name="dagster_storage_janitor_job",
    tags={"dagster/max_runtime": "3600"},  # 60 min; deletes are batched anyway
)
def dagster_storage_janitor_job():
    purge_old_run_history()


# Runs daily at 05:45 ET -- after the nightly pgBackRest backup window
# (01:00-02:00 UTC) and offset from the :30 all-assets and :10/:15 sync ticks.
dagster_storage_janitor_schedule = dg.ScheduleDefinition(
    name="dagster_storage_janitor_schedule",
    job=dagster_storage_janitor_job,
    cron_schedule="45 5 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    jobs=[dagster_storage_janitor_job],
    schedules=[dagster_storage_janitor_schedule],
)
