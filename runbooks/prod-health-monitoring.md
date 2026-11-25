# Prod health monitoring runbook

How to assess the health of the production Dagster deployment and the
warehouse Postgres, and what the known failure modes look like. Written after
the 2026-07-31 incident review (see git history of this file's branch for the
fixes that came out of it).

Connection strings referenced here live in `.env` (parent repo if you are on a
worktree): `PROD_DAGSTER_DB_URL` (Dagster instance DB) and
`WAREHOUSE_COOLIFY_URL` (warehouse Postgres). In that `.env`,
`WAREHOUSE_COOLIFY_URL` is the agents' login `hc_warehouse_agent`, which cannot
change existing data; §10 lists what it can do and the elevated route for the
rest. `PROD_DAGSTER_DB_URL` is the read-only login `dagster_reader`; §11 lists
what it can do and the route for the rest.

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

A `check_warehouse_reader_grants` failure means a program's reader login has
drifted from its config: see §9.

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
  against prod. If one is mid-flight and wedging everything: confirm pgBackRest
  is green (`pgbackrest --stanza=warehouse info` inside the postgres container),
  then
  `bin/warehouse-admin-psql --reason "stuck pg_dump" -c "SELECT pg_terminate_backend(<pid>);"`.
  The agents' login is refused here because the session is a superuser's (§10).
- "sorry, too many clients already" on its own usually means orphaned/blocked
  connections accumulating; see the reaper settings notes in the team memory
  (idle_in_transaction_session_timeout + tcp keepalives are configured). In this
  state the agents' login cannot connect, so run the queries above with
  `bin/warehouse-admin-psql --reason "too many clients triage" -c "<query>"`,
  which uses a slot reserved for superusers.

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
| Sling: `pq: unrecognized configuration parameter "channel_binding"` on every mirror | The warehouse URL carries `channel_binding=require`; libpq clients (psycopg2, dbt, dlt) support it, but Sling's Go driver sends it to the server as a setting | `_sling_connection_url` strips `channel_binding` for Sling only; Python clients keep it (2026-11-22) |
| Zoom `TypeError: not all arguments converted during string formatting` in `zoom_meeting_participant_details` | The final partial-batch flush used a shorter placeholder template than the row width | Both flushes share `DETAIL_VALUE_TEMPLATE` (2026-11-22) |

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

### Theseus Orchard source access gate

A prior investigation on 2026-10-14 confirmed Orchard deployments for the live
Theseus app but found that the connected account could not access their project.
Cross-project Orchard searches on 2026-10-30 UTC for `theseus` and
`mail.hackclub.com` returned no results. That does not overturn the earlier
Orchard finding or establish the live database identity.

Read-only inspection on 2026-10-30 UTC found a Theseus-related database Compose
configuration on the legacy source host, a retained PostgreSQL 17 volume, and no
container attached to that volume. Treat this as the likely old copy, not the
authoritative source for the warehouse. Its freshness remains unverified.

Before repointing the warehouse:

1. Obtain access to the live Theseus Orchard project, or obtain a dedicated
   read-only database URL through a secure channel. Verify the live app's
   database configuration to establish the authoritative Orchard database.
2. Validate the reader's grants, configured streams and sensitive-column
   selections. Compare source counts and maximum creation/update timestamps,
   especially for letters, against the warehouse's September snapshot. Confirm
   the source is current before allowing a full refresh; a successful connection
   alone does not establish freshness.
3. Save the verified URL as `THESEUS_K8S_URL` in Coolify's UI env store, then
   follow the configuration, deployment and acceptance steps above. Keep
   endpoints, credentials and aggregate verification results in the private
   deployment record.

If evidence indicates the old volume is needed, stop and raise a separate
question for approval of application database recovery. Recovery of that volume,
recreation of the app database service and changes to application workers are
outside this warehouse connection repair.

No database recovery, credential mutation, Coolify edit, deployment or backfill
was performed during this follow-up investigation. Until source access and
freshness are verified, the Theseus cutover remains blocked. All four mirrors,
the Auth backfill and production acceptance checks remain incomplete.

## 8. Adding an Orchard program mirror

A program mirror is one module, `orpheus_engine/defs/sling/programs/<name>.py`,
exporting `<name>_db_connection`, `<name>_replication_config` and
`<name>_warehouse_mirror`; `assets.py` discovers it. Copy `fallen.py`: full
refresh, schema `<name>`, connection `<NAME>_DB`, env var `<NAME>_K8S_URL`.

1. **Public endpoint, starting URL, and streams.** The database's public
   endpoint may be off: check `get_database` in Orchard, turn it on with
   `enable_database_public` if needed, and verify the listed
   `<publicHost>:<publicPort>` answers (`nc -z`). A listed port that does not
   answer comes back after disabling and re-enabling public access (Haven
   booking's did). Copy `.env` to a scratch file `<staged.env>` (under
   `umask 077`). For a new copy that has no saved URL in `.env` yet, seed
   `<NAME>_K8S_URL` in `<staged.env>` with the database's public connection URL
   from `get_database` (`postgresql://<user>:<password>@<publicHost>:<publicPort>/<dbName>?sslmode=verify-full&sslrootcert=system`)
   so Step 2 has the endpoint and a login that can read the catalog before
   `hc_warehouse_reader` exists; for an existing mirror, `<staged.env>` already
   has `<NAME>_K8S_URL` (or `ATTEND_COOLIFY_URL` for Attend). List every table
   explicitly and exclude columns with `-col`. Leave out session, token,
   OTP/verification, rate-limit, cache, migration and backup tables. Drop
   tokens, passwords, encrypted values, join/share codes, IP addresses, street
   address lines, postcodes, phone numbers and tracking numbers; keep names,
   email, birthday, city, state and country. Check every stream and `-col`
   against the live schema (`--check` in Step 2), then add the module to
   `test_sling_orchard_connections.py` and `test_sling_program_exclusions.py`.
   Column names do not show secrets stored as row values: for every mirrored
   key/value settings table, list its key names (never the values) and leave
   the table out if any is a credential, adding it to `UNMIRRORED_TABLES`
   (Coral's `app_settings` holds an Anthropic auth token).
2. **Reader login.** Never use the app's own login as the warehouse mirror
   credential. Run
   `PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_reader.py --program <name> --env-file <staged.env> --check --render-sql-file <scratch.sql> --write-env-file <staged.env>`
   which connects with the starting login in `<staged.env>` (`get_database`'s
   login on initial setup, or the existing login / `hc_warehouse_reader` on a
   later switch or after losing the password; this always creates a new
   password, so use the grants-only refresh in step 3 for column changes) to
   inspect the live `pg_catalog` schema
   (automatically resolving system CA certificates for `psycopg2-binary` and
   seeing all tables and columns regardless of caller table/column privileges),
   derives the exact table and column grants from `<name>_replication_config`,
   renders the single-transaction SCRAM-SHA-256 setup SQL to `<scratch.sql>`,
   and overwrites `<NAME>_K8S_URL` in `<staged.env>` with the new
   `hc_warehouse_reader` URL in Orchard's public-endpoint shape with
   `sslmode=verify-full&sslrootcert=system` (`--write-env-file` is required with
   `--render-sql-file` so a generated password is never discarded). In the
   database's primary pod, named by `get_database`
   `clusterStatus.currentPrimary` (not always `pg-<cluster>-1`: contd's is
   `pg-production-database-2`), through `exec_in_pod` with container `postgres`,
   execute `<scratch.sql>` (`psql -U postgres -d <dbName> -v ON_ERROR_STOP=1`)
   in one transaction: `CREATE ROLE hc_warehouse_reader LOGIN CONNECTION LIMIT 4
   PASSWORD '<SCRAM-SHA-256 verifier>'`, `ALTER ROLE … SET
   default_transaction_read_only = on`, `GRANT CONNECT` on the database, `USAGE`
   on `public`, and `SELECT` on exactly the mirrored tables (`attend`, `fallen`,
   `horizons` and `midnight` use this same `hc_warehouse_reader` setup).
   Generate the password locally and send only its SCRAM verifier, so the
   plaintext never reaches the pod or Orchard's audit log. For tables with
   `-col` exclusions or positive `select` allow-lists, grant
   `SELECT (<kept columns>)` instead of the whole table, then confirm a Sling
   run still reads them; if Sling selects `*` and fails, fall back to a table
   grant and record that the excluded columns stay readable by the login (for
   Horizons `public.user_sessions`, `hc_warehouse_reader` retains `SELECT` on
   `id` because Postgres computes the SHA-256 digest on the source server).
   Verify over the public endpoint as `hc_warehouse_reader` with
   `PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_reader.py --program <name> --env-file <staged.env> --verify-reader`:
   `has_column_privilege` is true on kept columns and false on excluded columns
   and on every column of unmirrored tables, no write privileges or `pg_auth_members` role
   memberships exist, and a real `INSERT` over the public endpoint fails (both
   in the default read-only session and after `SET TRANSACTION READ WRITE`).
   If you cannot run SQL in the primary pod, use
   `scripts/provision_orchard_reader.py` and the private ops notes.
3. **The login or its column grants can go stale.** The role is created outside
   Orchard: restoring or recreating the database cluster drops it, an app
   migration that drops and recreates a table drops that table's grant, and an
   app migration that adds a new column to a table with `-col` exclusions leaves
   `hc_warehouse_reader` without `SELECT` on the new column. The mirror stays
   green and the warehouse lacks the column; §9's daily check reports it.
   - **Lost login** (a login error after the role was dropped): rerun step 2,
     apply `<scratch.sql>`, verify, then save the new URL as in step 4.
   - **Stale grants** (`permission denied`, or a §9 finding; an app that
     recreates a table drops its grants): first decide
     whether a new column is sensitive, using the rules in step 1. If it is, add
     a `-col` for it to the config and land that; the column stays ungranted, so
     no grant change is needed. If it is not, or the reader can read a column it
     should not, run
     `PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_reader.py --program <name> --env-file <staged.env> --refresh-grants-sql-file <scratch.sql>`
     (the existing `hc_warehouse_reader` URL can read the full `pg_catalog`
     schema). It renders one transaction of `REVOKE` and `GRANT` on the mirrored
     tables and leaves the role, password and URL alone, so there is no URL to
     save. Apply `<scratch.sql>` on the primary pod as in step 2 (or as the
     table owner; see the private ops notes), verify with
     `--verify-reader --env-file <staged.env>`, then materialize the mirror and
     confirm the warehouse has the column, comparing names case-insensitively
     because the warehouse column's case can differ from the source's.
     Incremental streams only fill the new column on rows updated after the
     grant; older rows need a full refresh of that stream.
4. **Save the URL, deploy, verify row-count parity, and roll back on failure.**
   Save the verified `<NAME>_K8S_URL` (or `ATTEND_COOLIFY_URL` for Attend)
   written by `--write-env-file` in `<staged.env>` in Coolify's UI env store
   (host-only `.env` edits are reverted: a subsequent Coolify deploy overwrites
   `/data/coolify/applications/<app-uuid>/.env` and can silently restore the
   previous write-capable login unless Coolify's UI env store or a persistent
   host env file outside Coolify's rewrite path is updated) and the parent
   repo's `.env`. Then deploy the reviewed revision and verify as in §7
   (build/deploy the reviewed image first, then write the updated URL to the
   host `.env` before starting the container, verify the running image revision
   and env fingerprints, and remove any orphaned container), including a new
   materialization and source/warehouse row-count parity. When switching an
   existing mirror to
   `hc_warehouse_reader`, back up the host `.env` and `docker-compose.yaml`
   first, record baseline source and warehouse row counts before the switch with
   `PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_reader.py --program <name> --env-file <pre.env> --record-counts-json <pre-counts.json>`,
   materialize the asset after deploying the reviewed revision and restarting,
   and verify post-switch row counts with
   `PYTHONPATH=. .venv/bin/python scripts/provision_warehouse_reader.py --program <name> --env-file <post.env> --compare-counts-json <pre-counts.json> --record-counts-json <post-counts.json>`
   (full refresh matches within live-write tolerance; incremental streams do
   not regress below baseline warehouse counts or drift in their
   source-to-warehouse gap). After verification passes, update the Vaultwarden
   `.env` notes. If a cutover fails verification, roll back by restoring the
   backed-up host `.env` and `docker-compose.yaml` (and parent `.env`),
   restarting the container, and confirming no orphaned container remains.

## 9. Reader grant drift

`warehouse_reader_grant_drift_job` runs daily at 06:20 ET
(`warehouse_reader_grant_drift_schedule`, on by default). It finds enabled
program modules whose URL logs in as `hc_warehouse_reader` and compares each
`-col` stream's grants, derived from the config, with `has_column_privilege`. It
reads catalog metadata only, in a read-only session with timeouts, using one of
the login's 4 connections, and it never grants anything. Allow-list, `sql:` and
whole-table streams are not checked: a new column cannot change what an
allow-list copies. Programs on any other login are skipped and logged.

A failure appears in the §2 queries as step `check_warehouse_reader_grants`,
naming programs, tables and columns (no URLs or row values). `run_retries` runs a
failed job once more, so each failing day counts two `STEP_FAILURE` events.

| Message | Meaning | Fix |
| --- | --- | --- |
| `kept column not granted` | A column the mirror should copy is missing from the warehouse, usually after an app migration | §8 step 3, stale grants |
| `excluded column is readable` | The reader can read a column the config excludes | §8 step 3 refresh, then find out who granted it |
| `config no longer matches the source` | The table is gone, or a `-col` names a column that no longer exists | Update the program config, and refresh grants if the kept columns changed |
| `could not check` | Connection failed or the catalog query timed out; only the error type is shown | Check the endpoint and the connection limit. A one-off collision with a running mirror clears on the next run |
| `No program was checked` | No URL logs in as `hc_warehouse_reader`, so the program URLs probably did not load | Check the container's env and the URLs' fingerprints; a Coolify deploy can revert them (§8 step 4) |

## 10. The agents' login and the elevated routes

`WAREHOUSE_COOLIFY_URL` holds **three different values on purpose**:

- **Agents' login, `hc_warehouse_agent`:** the parent repo's `.env` and the
  Vaultwarden `.env` note.
- **Query tool's login, `hc_warehouse_mcp`:** the `warehouse-mcp` Coolify app only,
  and its own Vaultwarden item. It has the same grants, settings and limits on what
  it can do as the agents' login, and its own password and connection limit.
- **Superuser login, unchanged:** the Dagster app's Coolify settings, the host `.env`
  and that app's containers. The syncs and dbt's scheduled jobs use it.

Never copy the variable from one place to another: every sync would lose write
access, or the query tool would lose its limits.

### What each action uses

| Action | How |
| --- | --- |
| Ad-hoc SELECTs, `bin/readonly-psql`, `bin/ysws_true_expense.py`, spend preview without `--dbt`, reader row counts | Agents' login |
| Lock and wedge queries (§3): `pg_stat_activity`, `pg_blocking_pids`, full query text | Agents' login, through `pg_read_all_stats` |
| Cancel or end your own stuck session | Agents' login |
| Cancel a stuck query-tool session | Agents' login: `pg_cancel_backend`, see below |
| End or cancel any superuser session: `pg_dump`, a sync | `bin/warehouse-admin-psql` |
| Any triage during "too many clients" | `bin/warehouse-admin-psql`, because reserved slots are for superusers |
| Hand fixes: `metrics_last_fetched_at`, `ALTER COLUMN` for a type overflow, a backfill `UPDATE` | `bin/warehouse-admin-psql` |
| Sling test into a scratch schema | Prefer a `target: LOCAL` dry run. Otherwise run Sling in the app container and drop the schema with `bin/warehouse-admin-psql` |
| `dbt compile`, `parse`, `ls` | `bin/warehouse-dbt <command> ...` |
| `dbt run`, `dbt test`, spend preview `--dbt` | `bin/warehouse-dbt --elevated --reason "<why>" ...` |
| Dagster materialize from a laptop against prod | Refused, on purpose. In-process runs in the app container still work |

To cancel a query-tool session, find it with `usename = 'hc_warehouse_mcp'` and use
`pg_cancel_backend`. Do not use `pg_terminate_backend`: the tool retries a lost
connection, so it runs the same query again without saying so. A cancelled query
fails and is not retried.

### Rules for the elevated routes

Use one only with a written `--reason`, and only for a runbook step that names it
or a request that asks for the write. The reason shows as `application_name`
(`elevated:<reason>` or `elevated-dbt:<reason>`) in `pg_stat_activity`.

Both routes need SSH access to the host, so a leaked `.env` cannot reach them, and
no superuser password reaches the machine you run them on. They read three settings
from the environment or the parent `.env`, kept out of this public repo:
`WAREHOUSE_SSH_RELAY` (the ssh command that reaches the host, relay included),
`WAREHOUSE_POSTGRES_CONTAINER` and `WAREHOUSE_APP_CONTAINER` (each a name, or the
part before the `-<timestamp>` a deploy adds). Write the relay with SSH host aliases,
not `user@host`: `bin/readonly-psql` loads `.env` by splitting on spaces, and an `@`
word makes it abort.

Each tool refuses unless exactly one running container is named that, with or without
a timestamp; a sibling such as `<name>-proxy` does not count. Finding several means a
deploy is running, so wait it out. Finding none means the container is down or the
setting is wrong.

`bin/warehouse-dbt --elevated` uploads this checkout's `orpheus_engine_dbt/` tree
to the app container and runs it there, so a branch is built and tested before any
merge. It prints the commit, and whether the tree has uncommitted changes. It runs
as the syncs' login and uses the container's `HACK_CLUBBERS_LOCATION_SALT`. A model
that needs a new variable fails until the container has it.

If the relay drops during a long run, dbt can keep running and leave a
`/tmp/warehouse-dbt-*` folder in the app container. The folder holds no password,
and the next deploy removes it. Check `pg_stat_activity` for the `elevated-dbt:` name
before starting another run.

### What the agents' and query tool's logins can still do

- **Read everything,** including tokens in mirrored tables and other sessions'
  query text. Treat the URL as sensitive.
- **Read `pg_authid`,** so it can see every login's SCRAM verifier, including the
  superuser's. `pg_read_all_data` covers system catalogs, and Postgres cannot deny this
  to one login. A verifier cannot log in, but it allows offline password guessing, so
  every warehouse login needs a long random password.
- **Store data on the server,** after turning off its read-only default: temporary
  tables and large objects. A query through the tool can make temporary tables too,
  because `SELECT ... INTO TEMP` passes its SQL check. Neither login can change
  existing tables, but an exposed URL could use these to fill the disk. `temp_file_limit` caps sort and hash spill files
  only; Postgres does not count temporary tables against it. Closing this needs
  `REVOKE TEMP ON DATABASE` and `REVOKE EXECUTE` on the large-object functions
  `FROM PUBLIC`, which affect every login, so it is not done today.
- **Hold locks,** up to its timeouts: `lock_timeout` 30s, `statement_timeout` 30min,
  `idle_in_transaction_session_timeout` 5min. To adjust one, change `ROLE_SETTINGS` in
  the script below and run it again.

`pg_signal_backend` is left out on purpose. Postgres refuses to let a non-superuser
signal a superuser session, so it cannot end the stuck `pg_dump`. It would only let
an exposed URL kill every other non-superuser login. Agents need to cancel only
query-tool sessions, so `hc_warehouse_agent` is a member of `hc_warehouse_mcp` with
`INHERIT TRUE, SET FALSE, ADMIN FALSE`. Postgres lets a role signal the sessions of a
role whose privileges it inherits. The query tool's role owns nothing and has no
grant the agents' login lacks, and the agents' login cannot become it.

The query tool's connection limit is 12: 5 for its pool (`max_size` in
`warehouse-mcp/db.py`), 5 for the old container during a deploy overlap, and 2 for
sockets that were cut but still look open on the server. If you change the pool,
change `LOGINS` in the script below and run it again. A query the login's limits stop
(`lock_timeout`, `temp_file_limit`) fails at once, because the tool retries only a
lost connection.

### Create, check, roll back

`scripts/provision_warehouse_agent_login.py --login agent|mcp` renders the role SQL
with only a SCRAM verifier and writes the new URL to a 0600 file; running it again
gives the role a new password. Create the agents' login first, because the query
tool's SQL grants its role to it. `--verify` checks the login's attributes and exact
role memberships, that it can see other logins' query text, and that changing existing
tables, `COPY ... PROGRAM`, server file reads, ending a superuser session and
cancelling an idle session of another non-superuser login are all refused. If it finds
no such idle session it fails and says so; run it again when one is connected. With
`--login mcp` it needs `--agent-env-file`, checks both logins, and cancels a throwaway
query-tool session from the agents' login.

To roll back the agents' login, restore `WAREHOUSE_COOLIFY_URL` in the parent `.env`
from the host `.env` through the relay, update the Vaultwarden note, then
`bin/warehouse-admin-psql --reason "remove agent login" -c "DROP ROLE hc_warehouse_agent;"`.
The query tool's SQL needs that role, so roll back the query tool first.

To roll back the query tool's login, set `WAREHOUSE_COOLIFY_URL` in the `warehouse-mcp`
Coolify app back to the value in the host `.env` and redeploy that app. Its host is
the internal one, so check from the `warehouse-mcp` container that it resolves. Then
`bin/warehouse-admin-psql --reason "remove query tool login" -c "DROP ROLE hc_warehouse_mcp;"`.
After that, `--verify --login agent` reports the missing membership until the role is
created again.

The syncs are not affected either way.

## 11. The agents' read-only Dagster login

`PROD_DAGSTER_DB_URL` in the parent `.env` and the Vaultwarden `.env` note logs in as
`dagster_reader`, which has `pg_read_all_data` and nothing else. The Dagster app's
write-capable database credentials (`POSTGRES_USERNAME` / `POSTGRES_PASSWORD`) belong
only in Coolify's store, the host `.env` and the app container (not in `PROD_DAGSTER_DB_URL`).
The app does not use `PROD_DAGSTER_DB_URL`, so nothing here touches it.

| Action | How |
| --- | --- |
| §1, §2, §5 queries, freshness checks, `scripts/ysws_true_spend_preview.py` | `PROD_DAGSTER_DB_URL` |
| Launch, retry or cancel a run, turn a schedule on or off | Dagster GraphQL (`scripts/cancel_orphaned_runs.py`), never SQL |
| Superuser work on this database | The database's local socket on the host: `docker exec -i -u postgres <container> psql -U postgres`. Only for a request that asks for it |

Do not copy the Dagster app's write-capable credentials back into the parent `.env` or
the vault note, even to restore a backup: `provision` also changes the line in the
checkout's `.env*` backups.

### What the login can still do

- **Read everything,** including tokens in event logs. Treat the URL as sensitive.
- **Read `pg_authid`,** so it can see the superuser's SCRAM hash. `pg_read_all_data`
  covers system catalogs. A hash cannot log in, but it allows offline password
  guessing, so the superuser password must stay long and random.
- **Store data on the server,** after turning off its read-only default: temporary
  tables and large objects. It cannot change Dagster's tables.
- **Hold connections,** up to 10 of the server's 50, with `statement_timeout` 5min and
  `idle_in_transaction_session_timeout` 60s. To adjust these, change `ROLE_SETTINGS` or
  `CONNECTION_LIMIT` in the script below and run `provision` again.

This limits what a leaked connection string can do. Anyone with root on the host can
still read the host `.env` or use the socket.

### Create, check, roll back

Use `scripts/dagster_db_reader.py`. It sends secrets only through stdin and prints
names and booleans. A line whose name is not an uppercase identifier, such as a
commented-out URL, prints as `<unnamed line>`. Identifiers are arguments; keep them
in a private `@args` file, not in this repo. `--ssh` is the command that reaches the host (see the skill
`warehouse-dagster-prod-health`), and `--vault-note` is repeated for each `.env` note.

- **`provision`** generates a password and sends only its SCRAM hash to Postgres over
  the local socket. It logs in with the new URL at its public address, and changes
  nothing an agent reads unless that works. It then rereads and changes the one URL line
  in the parent `.env` and the vault note, and the same line in the checkout's `.env*`
  backups. Every other name stays as it was. Running it again is safe and gives the
  login a new password. `--url-host` and `--url-port` move both copies to a new address;
  never change only one, or a restore brings back the old address.
- **`verify`** checks the login's attributes, memberships and settings. It confirms no
  write privilege on any relation in `public`, that a real insert is refused with the
  read-only default off, and that the §1, §2 and §5 reads work. It confirms both copies
  log in as the reader, and that the parent `.env`, the checkout's backups, worktree
  env files and the vault notes do not hold the superuser value. It also lists, without
  failing, host `.env.bak*` files holding the current value and names that differ
  between the parent `.env` and the vault notes.
- **`push-gate --store-names-file <file>`** runs before a push to main, because a push
  regenerates the host `.env` from Coolify's store (§8 step 4). It compares the running
  app's variable names with the store's, listed one per line, and stops if the app has
  any the store lacks. Do not push until those are resolved.

**Rotating the Dagster app DB password:** read the current app credentials from the
host, not the parent `.env`. The parent URL holds the reader's password and only says
where the database is. `scripts/dagster_db_reader.py` always connects over the local
socket as `-U postgres` regardless of `POSTGRES_USERNAME`.

**Roll back:** restore the write-capable URL from the host `.env` into `PROD_DAGSTER_DB_URL`
in the parent `.env` and the vault note, in-process, then drop the login over the
socket (`-U postgres`) with `DROP ROLE dagster_reader;`. Nothing in the app depends on
the login.

## 12. Direct database access logging

MCP queries are logged with verified Hack Club Auth identity in `mcp_auth.audit`
(see `runbooks/warehouse-inventory.md`). Direct Postgres access to all three
databases — the warehouse Postgres (including restored clusters), the Dagster
instance Postgres, and the MCP auth Postgres — must also record every
connection, disconnection, and SQL statement (reads and changes) for every login
role, including `postgres` over the local Unix socket, with role, timestamp,
client address, and `application_name`.

### Required PostgreSQL settings

**PostgreSQL 17 (warehouse Postgres and Dagster instance Postgres):** Both
databases should run with the settings in
`warehouse-postgres-docker/logging.conf`, copied here for pasting into a custom
config. `orpheus_engine_tests/test_postgres_access_logging.py` fails if this copy
and the file differ.

```ini
log_destination = 'stderr'
logging_collector = off
log_connections = on
log_disconnections = on
log_statement = 'all'
log_parameter_max_length = 0
log_parameter_max_length_on_error = 0
log_line_prefix = '%m [%p] %quser=%u db=%d app=%a client=%r '
```

**PostgreSQL 18 (MCP auth Postgres):** PostgreSQL 18 replaced the boolean
`log_connections` with a comma-separated stage list; use `'all'` so every
connection stage is logged with the same prefix and statement-level auditing:

```ini
log_destination = 'stderr'
logging_collector = off
log_connections = 'all'
log_disconnections = on
log_statement = 'all'
log_parameter_max_length = 0
log_parameter_max_length_on_error = 0
log_line_prefix = '%m [%p] %quser=%u db=%d app=%a client=%r '
```

Why these settings:

- `log_connections = on` (PG 17) / `log_connections = 'all'` (PG 18) records
  every connection attempt (`connection received`), authentication
  (`connection authenticated`), authorization (`connection authorized` with role,
  database, and TLS cipher), and failed authentication (`FATAL:`).
- `log_disconnections = on` records session end and total duration.
- `log_statement = 'all'` at the cluster level records every read and write
  statement for **every existing and future login role**, including `postgres`
  over the local Unix socket. Simple-protocol queries log as `statement: ...`;
  extended-protocol parameterized queries (`psycopg` v3, JDBC, etc.) log as
  `execute <name>: ...` (for example `execute <unnamed>: ...`). Because
  `log_statement` has PostgreSQL context `superuser`, non-superuser logins
  cannot disable or bypass it with `SET log_statement = 'none'` (PostgreSQL
  refuses with `permission denied`). It takes effect on
  `SELECT pg_reload_conf();` with **no restart** (unlike `pgaudit`, which
  requires installing an extension, setting `shared_preload_libraries`, and
  restarting every database).
- **Proposed exception (pending approval) — omitting bind parameter values:**
  `log_parameter_max_length = 0` and `log_parameter_max_length_on_error = 0`
  suppress `DETAIL: parameters: ...` on extended-protocol parameterized queries
  (such as `psycopg` v3 in `warehouse-mcp` and JDBC/Metabase). This keeps bind
  values (user emails, token hashes, JSON payloads) out of the central log store
  while recording the SQL template (`SELECT ... WHERE ... = $1`), meaning the
  exact bound values read or written by a parameterized query are not logged.
  Inline literals in simple-protocol queries (including SQLAlchemy + `psycopg2`)
  are still logged verbatim; password-provisioning scripts
  (`scripts/dagster_db_reader.py`, `scripts/provision_warehouse_agent_login.py`,
  and `scripts/provision_warehouse_reader.py`) send only SCRAM-SHA-256 verifiers
  and suppress statement and error-statement logging
  (`SET [LOCAL] log_statement = 'none'`, `SET [LOCAL] log_min_error_statement = 'panic'`)
  for the role DDL transaction.
- `log_line_prefix` tags every session log line with timestamp (`%m`), backend PID
  (`%p`), Postgres role (`%u`), database (`%d`), `application_name` (`%a`), and
  peer address/port (`%r`). `%q` suppresses the session fields on non-session
  background processes (such as checkpointer).
- Keep `log_destination = 'stderr'` and `logging_collector = off`. Turning
  `logging_collector` on redirects stderr into local `$PGDATA/log` files and stops
  container stderr output, which breaks shipping to the central log store.

### Proposed per-role exclusions (pending approval)

Cluster-level `log_statement = 'all'` applies to every role unless an explicit
exception is approved and documented here. Note that with PostgreSQL's default
`log_min_error_statement = error`, **statements that fail** still log their full
SQL text regardless of `log_statement`; setting `log_min_error_statement = 'panic'`
on the excluded bulk-write roles is therefore also proposed below so failed bulk
inserts do not ship raw payloads to the central log store. The following
exclusions are **proposed, pending approval**:

| Database | Role | Proposed Setting | Scope & Rationale |
| --- | --- | --- | --- |
| Warehouse Postgres | `hc_warehouse_dagster` | `ALTER ROLE hc_warehouse_dagster SET log_statement = 'ddl';` and `SET log_min_error_statement = 'panic';` | Applies to **all** sessions of `hc_warehouse_dagster`: scheduled syncs (Sling, dlt, dbt — both writes and reads) and human/agent `bin/warehouse-dbt --elevated` runs (§10). Prevents batched inline `INSERT` literals from shipping row-level PII to the central log store (including on statement failure) and avoids sync-window log spikes, while still logging all DDL (`CREATE`, `ALTER`, `DROP`). Note: `fivetran` is **not** excluded and remains at `log_statement = 'all'`. |
| Dagster instance Postgres | `dagster_app` (dedicated non-superuser app role) | `ALTER ROLE dagster_app SET log_statement = 'ddl';` and `SET log_min_error_statement = 'panic';` | Dagster writes through SQLAlchemy + `psycopg2`, which interpolates values client-side into SQL literals (`log_parameter_max_length = 0` does not strip them). `event_logs` and `asset_keys.last_materialization` include multi-megabyte JSON payloads (containing emails and tokens) that exceed central log-store per-line and per-stream limits. Moving the Dagster app onto a dedicated non-superuser `dagster_app` role excluded at `'ddl'` (with `log_min_error_statement = 'panic'` so failed inserts do not log payloads either) keeps `postgres` (socket-only after cutover) and `dagster_reader` at `log_statement = 'all'`. |

No role exclusion is proposed on the MCP auth DB (low query volume; all queries
from `warehouse_mcp_auth` use extended-protocol bind parameters).

### Configuration durability, rollout, and rollback

The custom warehouse image (`ghcr.io/hackclub/warehouse-postgres:17`) bakes the
PG 17 settings into `/etc/postgresql/logging.conf` and includes them automatically
on first init via `warehouse-postgres-docker/conf.sh` (with **no per-role
exclusion** baked in; fresh clusters log all roles until an approved override is
applied). Clusters started with a custom Postgres configuration (including
restored warehouse clusters and the Dagster instance database, which uses stock
`postgres:17-alpine`) must keep these settings in their configuration (or
`include '/etc/postgresql/logging.conf'` on the custom image) so redeploys do
not clear them. The MCP auth database runs stock `postgres:18-alpine` with a
persistent data volume, so `ALTER SYSTEM` writes to `postgresql.auto.conf` on
that volume and survives container restarts and redeploys.

**Logical restore durability limit:** Roles (`dagster_app`), per-role
`ALTER ROLE ... SET` overrides, and table ownership live in the cluster catalog,
and `ALTER SYSTEM` lives in `postgresql.auto.conf`. Restoring a single database
from a logical `pg_dump` does **not** restore cluster roles, role settings, or
`postgresql.auto.conf`; paste the logging settings into Coolify's Custom Postgres
Configuration and re-apply roles, table ownership, and approved overrides after
any logical restore.

All logging parameters above (`log_connections`, `log_disconnections`,
`log_statement`, `log_parameter_max_length`, `log_parameter_max_length_on_error`,
`log_line_prefix`) activate immediately via `SELECT pg_reload_conf();` with **no
database restart**.

**Mandatory ordering for role exclusions (apply and rollback):**
`ALTER ROLE ... SET log_statement` applies only to **new** sessions, whereas
cluster-wide `log_statement = 'all'` applies immediately on reload to
**existing** sessions.

- **Apply order:**
  1. Apply the approved `ALTER ROLE ... SET log_statement = 'ddl'` (and
     `SET log_min_error_statement = 'panic'`) first; for the Dagster DB, transfer
     ownership of the `public` tables **only** (owned sequences follow
     automatically) one table at a time with `SET lock_timeout = '5s'` in a quiet
     window after confirming no `pg_dump` or long transaction is running, then
     cut the Dagster app over to `dagster_app`.
  2. Take the cutoff timestamp from the database **after** the `ALTER ROLE`
     commits (`SELECT clock_timestamp()`), and wait until **zero** pre-existing
     sessions of that role (with `backend_start <= cutoff`, or old Dagster app
     TCP sessions as `postgres`) remain in `pg_stat_activity`.
  3. Update the config file idempotently, confirm
     `SELECT sourcefile, sourceline, name, setting, error FROM pg_file_settings WHERE error IS NOT NULL;`
     returns zero rows, and **only then** run `SELECT pg_reload_conf();`.
- **Rollback order (cluster-first):**
  1. Reset cluster-level `log_statement` (via `ALTER SYSTEM RESET` or restoring
     the backed-up config file) and run `SELECT pg_reload_conf();` **first**.
  2. Confirm `SHOW log_statement;` returns `none`.
  3. **Only then** reset per-role overrides (`ALTER ROLE ... RESET log_statement`
     / `RESET log_min_error_statement`), so no new session of an excluded role
     can ever inherit `log_statement = 'all'` during the rollback window.

Use `scripts/postgres_audit_logging.py` to render idempotent apply/rollback SQL
or run a read-only settings check:

```bash
# Render reload-only apply SQL (pg17 for warehouse/dagster, pg18 for mcp-auth)
.venv/bin/python scripts/postgres_audit_logging.py render --pg-major 17 --exclude-role hc_warehouse_dagster=ddl --suppress-error-role hc_warehouse_dagster
.venv/bin/python scripts/postgres_audit_logging.py render --pg-major 17 --exclude-role dagster_app=ddl --suppress-error-role dagster_app
.venv/bin/python scripts/postgres_audit_logging.py render --pg-major 18

# Render safe cluster-first rollback SQL
.venv/bin/python scripts/postgres_audit_logging.py render --pg-major 17 --exclude-role hc_warehouse_dagster=ddl --suppress-error-role hc_warehouse_dagster --rollback
.venv/bin/python scripts/postgres_audit_logging.py render --pg-major 18 --rollback

# Read-only live settings and role-override drift check
.venv/bin/python scripts/postgres_audit_logging.py check --dsn-env WAREHOUSE_COOLIFY_URL --pg-major 17 --exclude-role hc_warehouse_dagster=ddl --suppress-error-role hc_warehouse_dagster
```

### Verify live settings (read-only)

Run `scripts/postgres_audit_logging.py check` or execute directly against all
three databases (warehouse Postgres, Dagster instance Postgres, and MCP auth
Postgres):

```sql
SELECT name, setting, source
FROM pg_settings
WHERE name IN (
  'log_destination',
  'logging_collector',
  'log_connections',
  'log_disconnections',
  'log_statement',
  'log_parameter_max_length',
  'log_parameter_max_length_on_error',
  'log_line_prefix'
)
ORDER BY name;

SELECT r.rolname, d.datname, s.setconfig
FROM pg_db_role_setting s
LEFT JOIN pg_roles r ON r.oid = s.setrole
LEFT JOIN pg_database d ON d.oid = s.setdatabase
WHERE s.setconfig::text LIKE '%log_%'
ORDER BY 1, 2;
```

Confirm:

- `log_destination = stderr`, `logging_collector = off`,
  `log_disconnections = on`, `log_statement = all`,
  `log_parameter_max_length = 0`, `log_parameter_max_length_on_error = 0`.
- `log_connections = on` on PG 17 (warehouse and Dagster DB) and
  `log_connections = all` on PG 18 (MCP auth DB).
- `log_line_prefix` includes `%m`, `%p`, `%u`, `%d`, `%a`, and `%r`.
- Only documented approved role overrides appear in `pg_db_role_setting`.

To verify end-to-end log shipping, run a marked read-only query such as
`SELECT /* audit_check_<id> */ 1;` on each database through each testable access
path (MCP, direct/tailnet login, SSH forward, and local socket; for external
source-restricted relays such as Metabase and Fivetran, inspect live traffic in
the central log store) and confirm the `statement:` or `execute ` line appears
in the central log store with `user=`, `db=`, `app=`, and `client=`.

### Where logs are retained and how to check who logged in

- **Durable off-host retention:** Container stderr is shipped to the warehouse's
  central log store. See the private ops notes for query endpoints, queries,
  retention, and recurring ingestion checks. Filter for `connection authorized`,
  `connection authenticated`, `disconnection`, `statement:`, `execute `, or
  `FATAL` (e.g. `|~ "statement:|execute "`) to inspect logins, statements, and
  failed auth attempts.
- **Recent local logs on the host (ephemeral):** Recent logs can also be checked
  directly on the host for quick triage, though local `docker logs` vanish when a
  container is recreated:

```bash
docker logs --since 24h <postgres-container> 2>&1 | grep -E 'connection (received|authenticated|authorized)|disconnection|statement:|execute |authentication failed'
```

Look up the warehouse, Dagster, and MCP auth Postgres container names from the
private ops notes or `docker ps` on the host.

### Attribution limits, bypass limits, and tracing the real source

Postgres logs record the **database role (`user=`)**, the **TCP peer address
Postgres sees (`client=`)**, and the **client-supplied `application_name`
(`app=`)**:

- **Superuser bypass limit (cannot be prevented inside PostgreSQL):** No
  PostgreSQL setting can stop a cluster superuser (`postgres`) from disabling
  statement logging in its own session — via `SET log_statement = 'none'` (which
  logs only the `SET` command) or at connect time via
  `PGOPTIONS='-c log_statement=none'` (which logs connection/disconnection lines
  but **zero** statement lines). `postgres` is restricted to the local Unix
  socket on the warehouse DB and MCP auth DB (and becomes socket-only on the
  Dagster DB once `dagster_app` is deployed). **Compensating control:** host
  Linux `auditd` (`EXECVE`) and SSH auth logs shipped to the central log store
  record SSH logins and `docker exec` command arguments (including `-e PGOPTIONS`
  or `-c` SQL flags). **Limit:** `auditd` captures command-line arguments, not
  SQL piped over stdin to `psql`.
- **Multi-line SQL statements (known limit):** PostgreSQL writes continuation
  lines of multi-line SQL with a leading tab (`\t`) and no `log_line_prefix`,
  and the container log driver ships each line as a separate record. Single-line
  queries (including `warehouse-mcp`, which uses a single-line
  `/* warehouse-mcp <caller> */` comment prefix) keep `user=`, `db=`, `app=`,
  `client=`, and the full statement in one record; multi-line SQL records carry
  `user=`/`client=` on the first line only. In particular, Metabase prepends a
  `-- Metabase::...\n` comment line, so Metabase SQL appears on unprefixed `\t`
  continuation lines and must be paired by timestamp with the preceding
  `-- Metabase` line that carries `user=metabase` and `client=`. Stitching
  continuation lines requires multiline filtering in the Coolify-managed log
  drain (future follow-up).
- **Shared roles and untrusted `app=`:** Direct database roles (`postgres`,
  `dagster_reader`, `dagster_app`, `hc_warehouse_agent`, `hc_warehouse_mcp`,
  `warehouse_mcp_auth`, and service roles) may be shared across operators or
  services, so a Postgres login identifies a role rather than an individual
  person. `application_name` (`app=`, such as `elevated:<reason>`) is set by the
  client rather than authenticated by the server. If direct database access must
  be attributed to an individual person without correlating host-side logs,
  per-person database logins (or an authenticating proxy) remain future work.
- **Proxied or relayed connections:** Connections arriving through a host relay
  (such as the Metabase, Fivetran, or tailnet relays for the warehouse DB) show
  the relay's host address in `client=` rather than the caller's real IP.
  Correlate the timestamp with the host-side relay logs shipped to the central
  log store (documented in the private ops notes) to identify the real remote
  source.
- **Dagster instance DB and MCP auth DB remote access:** Public TCP proxy ports
  for the Dagster instance DB and MCP auth DB are disabled, blocked at the host
  firewall, and rejected in `pg_hba.conf`. Non-container access arrives only over
  the local Unix socket (`docker exec`) or an SSH port forward from the host
  bridge gateway. Host SSH auth logs (`sshd` at `LogLevel INFO`) record the jump
  host's egress IP on `Accepted publickey` and correlate to database logins by
  timestamp (exact 1:1 port-forward mapping is not logged at `LogLevel INFO`).
  If a TCP proxy were ever re-enabled for either database, it must log the real
  client address and ship those logs to the central log store before accepting
  traffic.
- **Local socket logins (`client=[local]`):** `docker exec -u postgres ... psql`
  and `bin/warehouse-admin-psql` connect over the Unix socket. Trace them to the
  operator's SSH session via the host-side SSH and audit logs described in the
  private ops notes.
