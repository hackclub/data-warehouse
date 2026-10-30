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

## 7. Cut over Half Life, Macondo, Auth and Theseus

Code changes alone do not restore these mirrors. Perform this procedure only
in the authorized deployment step, after code review. Never record URLs,
passwords, key material, or row data in this public repository.

### Prerequisites and credential repair

1. Inspect the queue (§1), active mirror writers, and backup activity (§3).
   Save the current deployment configuration securely outside Git for rollback.
   Read the latest `STEP_FAILURE` cause chain (§2) before acting.
2. Confirm the app's current database identity and the endpoint reachable from
   the warehouse for each source. Macondo must use Orchard `macondo-prod-pg18`.
   Establish Theseus's active database from its app configuration, then compare
   distinctive tables and source timestamps; a name match is insufficient.
3. Determine all consumers of Half Life's failing login before changing it.
   If dedicated to warehouse reads, rotate its password and preserve read-only
   grants. If shared with the app, leave its password unchanged: create a
   dedicated read-only warehouse login, or use the verified current password
   in the warehouse configuration. If ownership is unresolved, stop that change.
   Verify CONNECT, schema USAGE and SELECT for every configured stream, with
   no write grants or privileged role memberships.
4. Restore Auth's dedicated `warehouse_sync` credential and verify its read-only
   grants on the four configured tables. Preserve Auth's SSH tunnel and key.
5. Compare all configured streams and selected columns with the live source
   schemas, including exclusions and disabled sensitive tables. Do not widen
   a sensitive-column allow-list to make a query pass.

### Configuration and deployment

- Save `HALF_LIFE_DATABASE_URL`, `AUTH_DB_PASSWORD` (and any changed `AUTH_DB_*`
  fields), `MACONDO_K8S_URL` and `THESEUS_K8S_URL` in **Coolify's UI env store**,
  then reconcile the host application `.env`. Host-only edits are overwritten
  by Coolify. Reconcile the previously repaired Attend/Fallen values into the
  UI store too, so deployment cannot undo those repairs.
- Use the public Orchard endpoints with verified TLS. The shared
  `_sling_connection_url` helper removes `sslrootcert=system` for Sling while
  retaining `sslmode=verify-full`; store the original URL in Coolify.
- Deploy the reviewed revision using the existing `docker_deploy/` setup in a
  quiet window. A restart can cancel runs and backups. Verify the deployed
  revision and privately compare fingerprints of stored and running values.
  Check for orphaned containers; do not leave two schedulers running.
- Materialize only the four repaired mirrors, with no concurrent writers.
  Half Life, Macondo and Theseus already use full refresh. Auth is incremental:
  perform a one-off full refresh of its four configured streams with the same
  selections, including `oauth_access_tokens` whose cursor is `created_at`.
  This recovers outage gaps and edits/deletes that an incremental retry misses.
  Keep this override out of the normal scheduled configuration.
- After successful cutover, delete `MACONDO_COOLIFY_URL`,
  `THESEUS_COOLIFY_URL`, `THESEUS_SSH_TUNNEL` and
  `THESEUS_SSH_PRIVATE_KEY_B64` from **both** Coolify's env store and the host
  `.env`. Reconcile containers through the existing deployment process. Verify
  these variables are absent from running containers and that
  `AUTH_SSH_PRIVATE_KEY_B64` remains available to Auth alone.

### Acceptance and rollback

For each mirror, require a new `ASSET_MATERIALIZATION` event (§2) after deployment,
then compare source and warehouse counts for every configured stream and maximum
creation/update timestamps (notably Auth activities and Theseus letters). Check
source identity and timestamps even for green runs: a frozen pre-migration copy
can sync successfully. Require a subsequent scheduled success, and repeat all
checks after any cleanup deployment. Record aggregate results privately; a build
test is not production verification.

If a deployment fails, restore the prior reviewed image and required connection
settings from the secure configuration record. Deleted old settings must be
explicitly restored when needed by old code. Never restore a revoked password;
use a working read credential. Recheck run events and freshness after rollback.

Build investigation on 2026-10-30 UTC confirmed production failures for all four
mirrors. Last materializations in the preceding 60 days were Oct 15 for Half
Life and Theseus, Oct 2 for Macondo, and none for Auth. Orchard lists
`macondo-prod-pg18`.

Follow-up on 2026-10-30 UTC established that Half Life's Orchard app uses the
same database login as the failing warehouse connection. It is also the
Orchard database owner. **Do not rotate this shared app password.** Provision
a dedicated warehouse reader with CONNECT, schema USAGE and SELECT only,
verify its grants and stream access, then save its URL in Coolify before
cutover. The Half Life database reports healthy and its app pod reports ready.

### Theseus missing-container recovery gate

The earlier Orchard search did not establish the live database identity. Do not
interpret a missing project search result as proof that access is the only
problem. The handoff reports a missing database container while a worker still
references it. This requires source recovery investigation before a mirror
cutover, rather than substituting a frozen snapshot.

Read-only inspection on 2026-10-30 UTC also found a Theseus-related database
Compose configuration on the legacy source host, a retained data volume with
`PG_VERSION` 17, no `postmaster.pid`, and no container (running or stopped)
attached to that volume. These facts establish a recovery candidate, **not**
that this volume contains the live or newest Theseus data. Resource identifiers,
endpoints, and credentials must stay in the private deployment record.

Prepare and review the following recovery procedure before executing it:

1. Resolve the current web app and worker through their deployment records.
   Privately compare their database host, database name, and deployment history
   with the missing database resource. Record the intended database version,
   image digest, extensions, mounts, network aliases, and last successful app
   connection. Check Orchard as well as legacy deployments. If these disagree,
   stop the cutover until the authoritative source is established.
2. Inspect all containers and mounts, including stopped containers, to prove no
   server is using the candidate data directory. Check available disk space and
   backup coverage. A missing PID file alone is insufficient. Do not start
   Postgres on the original volume or use Compose's create-volume behavior to
   silently initialize an empty database.
3. With recovery explicitly authorized, preserve a consistent physical backup
   or storage snapshot of the unmounted volume and its configuration. Verify
   the backup and retain it outside Git. Restore a separate clone. Use the
   matching PostgreSQL major version and required extensions, with no app
   network attachment, public listener, workers, migrations, or outbound jobs.
   Crash recovery writes to the clone; never test recovery on the sole copy.
4. Start only the isolated database clone. Inspect recovery logs, database
   identities, table inventories, migration versions, and aggregate counts and
   maximum creation/update timestamps for letters and fulfillment tables.
   Compare against the warehouse, verified app history, and any newer backups
   or Orchard source. Do not print addresses, message contents, or row data.
   If the candidate stops at the known September snapshot, it does not satisfy
   recovery; locate the later source or backup before proceeding.
5. Review the resulting identity and freshness evidence and an exact cutover
   and rollback procedure before touching the live app. Recreating an app
   database service is a separate production action from repointing a warehouse
   reader. Keep application workers disconnected during validation so recovery
   cannot resend mail or repeat fulfillment jobs. Never upgrade Postgres or
   run application migrations as an incidental repair.
6. Only after the authoritative source is healthy, provision a dedicated
   read-only warehouse credential and validate every configured stream and
   allow-list. Store its verified URL in Coolify, then follow the deployment,
   backfill, acceptance, and rollback steps above. Preserve the old warehouse
   tables until this identity gate passes; do not full-refresh from an empty or
   stale recovery candidate.

No database recovery, credential mutation, Coolify edit, deployment, or backfill
was performed during this follow-up investigation. The missing-container
recovery procedure requires review before live execution. The four mirrors
still require production acceptance; passing connection tests is insufficient.
