# Prod health monitoring runbook

How to assess the health of the production Dagster deployment and the
warehouse Postgres, and what the known failure modes look like. Written after
the 2026-07-31 incident review (see git history of this file's branch for the
fixes that came out of it).

Connection strings referenced here live in `.env` (parent repo if you are on a
worktree): `PROD_DAGSTER_DB_URL` (Dagster instance DB) and
`WAREHOUSE_COOLIFY_URL` (warehouse Postgres).

## 1. Is the run queue healthy?

```sql
-- against PROD_DAGSTER_DB_URL
SELECT status, count(*) FROM runs
WHERE create_timestamp > now() - interval '4 hours' GROUP BY 1;

SELECT run_id, pipeline_name, status, create_timestamp
FROM runs WHERE status IN ('QUEUED','STARTED','CANCELING')
ORDER BY create_timestamp;
```

Healthy: a handful of STARTED, nothing QUEUED for more than a few minutes.
Sick: STARTED runs hours old + a growing QUEUED tail. That means something is
wedged (see §3) — the scheduled jobs fire every 15 minutes and each waiting
run holds warehouse connections.

Note: the Dagster DB container has Docker's default 64MB /dev/shm, so parallel
query can die with "could not resize shared memory segment". Prefix ad-hoc
analytics with `SET max_parallel_workers_per_gather=0;`.

## 2. What is failing, and why?

Failure counts by asset:

```sql
SELECT step_key, count(*) AS failures, max(timestamp) AS last
FROM event_logs
WHERE timestamp > now() - interval '3 days'
  AND dagster_event_type='STEP_FAILURE'
GROUP BY 1 ORDER BY 2 DESC LIMIT 30;
```

The real error is buried in the cause chain (the top-level message is always a
useless `DagsterExecutionStepExecutionError`). For Sling assets take the TAIL
of the message — the failing stream prints last:

```sql
SELECT right(coalesce(
    event::json->'dagster_event'->'event_specific_data'->'error'->'cause'->'cause'->>'message',
    event::json->'dagster_event'->'event_specific_data'->'error'->'cause'->>'message',
    event::json->'dagster_event'->'event_specific_data'->'error'->>'message'), 1200)
FROM event_logs
WHERE timestamp > now() - interval '2 days'
  AND dagster_event_type='STEP_FAILURE'
  AND step_key='<asset name>'
ORDER BY timestamp DESC LIMIT 3;
```

Last-success vs last-failure tells you when it broke (and therefore what
changed):

```sql
SELECT
  max(timestamp) FILTER (WHERE dagster_event_type='ASSET_MATERIALIZATION') AS last_success,
  max(timestamp) FILTER (WHERE dagster_event_type='STEP_FAILURE')          AS last_failure
FROM event_logs
WHERE step_key='<asset name>' AND timestamp > now() - interval '30 days';
```

## 3. Is the warehouse Postgres wedged?

```sql
-- against WAREHOUSE_COOLIFY_URL
SELECT count(*) AS total,
       count(*) FILTER (WHERE state='active') AS active,
       count(*) FILTER (WHERE wait_event_type='Lock') AS waiting_on_lock
FROM pg_stat_activity WHERE backend_type='client backend';
```

If `waiting_on_lock` is large, find the root blocker:

```sql
WITH blocked AS (
  SELECT pid, unnest(pg_blocking_pids(pid)) AS blocker
  FROM pg_stat_activity WHERE wait_event_type='Lock'
)
SELECT b.blocker, count(DISTINCT b.pid) AS blocks_n, a.state,
       now()-a.query_start AS age, a.application_name, left(a.query, 150) AS q
FROM blocked b JOIN pg_stat_activity a ON a.pid = b.blocker
GROUP BY 1,3,4,5,6 ORDER BY blocks_n DESC LIMIT 10;
```

Interpretation guide:

- The sync jobs TRUNCATE their target tables every run. TRUNCATE needs an
  ACCESS EXCLUSIVE lock, so **any long-lived reader of a synced table blocks
  the TRUNCATE, and the queued TRUNCATE then blocks every later reader**. One
  long transaction can freeze the entire warehouse this way.
- **`application_name = pg_dump` holding locks is a red alert.** pg_dump takes
  ACCESS SHARE on every table in the database for its entire duration. A
  full-database pg_dump of this warehouse (~2 TB) runs for many hours and
  freezes all sync jobs for the duration. This happened 2026-07-31 when a
  Coolify scheduled backup was enabled on the warehouse DB: 149 of 160
  backends stuck for 3+ hours, "sorry, too many clients already" errors, run
  queue 18 deep. Real backups are pgBackRest (physical, lock-free) on the
  host's systemd timers — a logical pg_dump of the whole DB should never run
  against prod. If one is mid-flight and wedging everything:
  `SELECT pg_terminate_backend(<pid>);` after confirming pgBackRest is green
  (`pgbackrest --stanza=warehouse info` inside the postgres container).
- "sorry, too many clients already" on its own usually means orphaned/blocked
  connections accumulating; see the reaper settings notes in the team memory
  (idle_in_transaction_session_timeout + tcp keepalives are configured).

## 4. Known failure signatures (fixed, but watch for regressions)

| Signature | Cause | Fix (2026-07-31) |
| --- | --- | --- |
| `FileNotFoundError: .../.dlt_pipelines/.../schema_updates.json` repeating for days | Killed run left a pending dlt load package; every next run tried to resume it and died | dlt loads now use run-scoped throwaway dirs + fresh-state retry (`defs/dlt/assets.py`) |
| `FileNotFoundError` on `.dlt_pipelines/.../load/new/...` during busy periods | Overlapping runs of the same asset raced on the shared pipeline dir | Same as above, plus skip-if-running schedules + all-assets job no longer re-syncs fast-cadence assets (`schedules.py`) |
| Sling: `duplicate key ... pg_type_typname_nsp_index` or `_tmp` table count exactly 2x stream count | Two overlapping runs of the same mirror wrote the same `_tmp` table | Skip-if-running schedules; overlap removed from all-assets job |
| Sling: `Text file busy` on the sling binary | Concurrent first-runs raced to extract the binary after a redeploy | Rare (redeploy-window only); retried by run_retries |
| `review_warehouse_mirror` connection refused | Source Postgres is gone (app moved to third-party MySQL, May 2026) | Asset deregistered; see comment in `defs/sling/assets.py` |
| Zoom `Exhausted 15 retries` | Zoom report endpoints' daily quota | Benign; next scheduled run picks up |
| Zenventory `Remote end closed connection without response` | Flaky Zenventory web API | Session retries + image fetch degrades to no-image |

## 5. Storage growth

The Dagster instance DB reached 163 GB (160 GB event_logs) on 2026-07-31
because nothing was ever purged. `dagster_storage_janitor_job` now deletes
terminal runs (and their event logs) older than 120 days, capped at 2,000
runs/day; tick retention is configured in `docker_deploy/dagster.yaml`.
Deleting reclaims reusable pages but does not shrink files — if disk pressure
ever matters, run a one-off `VACUUM FULL` / pg_repack on event_logs during a
quiet window.

Sanity check the janitor:

```sql
SELECT count(*), min(create_timestamp) FROM runs;  -- oldest should approach the retention floor
```

## 6. After changing schedules/jobs

- Schedule enabled/disabled state is keyed by schedule NAME in the instance
  DB. Renaming a schedule silently disables its replacement on deploy.
- The skip-if-running schedules only skip while a run of the SAME job is
  in flight. If a job wedges permanently, its `dagster/max_runtime` tag lets
  run monitoring kill it (both configured in `orpheus_engine/schedules.py`).
- Coolify rebuilds ALL repo services on any push to main (no path filtering),
  and a redeploy during a backup window kills in-flight `docker exec` backups.
