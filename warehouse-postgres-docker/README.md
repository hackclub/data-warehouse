Custom Postgres image for Postgres warehouse.

Image name: `ghcr.io/hackclub/warehouse-postgres:17`

## pgBackRest S3 Backups

The image includes pgBackRest and auto-configures WAL archiving on first init. WAL archiving runs asynchronously with 8 parallel uploaders (`pgbackrest.conf`, baked into the image at `/etc/pgbackrest/pgbackrest.conf`) — synchronous per-segment pushes cannot keep up with ETL-level WAL volume and will silently build an unbounded `pg_wal` backlog that also blocks backups. `PGBACKREST_STANZA` must be set or the container will refuse to start. Set the following environment variables on your container to enable backups to S3-compatible storage:

| Variable | Required | Description |
|---|---|---|
| `PGBACKREST_REPO1_TYPE` | yes | Set to `s3` |
| `PGBACKREST_REPO1_S3_BUCKET` | yes | Bucket name |
| `PGBACKREST_REPO1_S3_REGION` | yes | Bucket region (e.g. `fsn1`) |
| `PGBACKREST_REPO1_S3_ENDPOINT` | yes | S3 endpoint (e.g. `fsn1.your-objectstorage.com` for Hetzner) |
| `PGBACKREST_REPO1_S3_KEY` | yes | S3 access key |
| `PGBACKREST_REPO1_S3_KEY_SECRET` | yes | S3 secret key |
| `PGBACKREST_REPO1_S3_URI_STYLE` | yes* | Set to `path` for non-AWS providers (Hetzner, R2, MinIO, etc.) |
| `PGBACKREST_REPO1_PATH` | yes | Path prefix in the bucket (e.g. `/backups/warehouse`) |
| `PGBACKREST_REPO1_RETENTION_FULL` | no | Retention value — number of full backups (default) or days if `RETENTION_FULL_TYPE=time` |
| `PGBACKREST_REPO1_RETENTION_FULL_TYPE` | no | `count` (default) or `time` (retain fulls for N days) |
| `PGBACKREST_REPO1_CIPHER_TYPE` | no | Set to `aes-256-cbc` to encrypt backups |
| `PGBACKREST_REPO1_CIPHER_PASS` | no | Encryption passphrase (required if `CIPHER_TYPE` is set). **Must be set before first backup.** |
| `PGBACKREST_STANZA` | yes | Stanza name (use `warehouse`) |
| `PGBACKREST_PG1_PATH` | yes | PostgreSQL data directory (default: `/var/lib/postgresql/data`) |
| `PGBACKREST_LOG_LEVEL_CONSOLE` | no | Log level: `info`, `detail`, `debug` |
| `PGBACKREST_START_FAST` | no | Set to `y` for fast backups |
| `PGBACKREST_PROCESS_MAX` | no | Parallel compression threads (default: `2`) |

### First-time setup

After deploying with the env vars set, exec into the container and run:

```bash
pgbackrest --stanza=warehouse stanza-create
pgbackrest --stanza=warehouse check
```

### Taking backups

```bash
# Full backup
pgbackrest --stanza=warehouse --type=full backup

# Differential backup (requires an existing full)
pgbackrest --stanza=warehouse --type=diff backup
```

## Scheduled Backups

Backups are run by systemd timers on the host. The schedule is:

- **Full backup**: 1st and 15th of each month at 3am
- **Diff backup**: every day at 3am, except the 1st and 15th (when the full runs)

### Host setup

The following files are deployed to the host (see `backup/` directory):

| File | Destination |
|---|---|
| `pgbackrest-backup.sh` | `/opt/scripts/pgbackrest-backup.sh` |
| `pgbackrest-full.service` | `/etc/systemd/system/pgbackrest-full.service` |
| `pgbackrest-full.timer` | `/etc/systemd/system/pgbackrest-full.timer` |
| `pgbackrest-diff.service` | `/etc/systemd/system/pgbackrest-diff.service` |
| `pgbackrest-diff.timer` | `/etc/systemd/system/pgbackrest-diff.timer` |

Create `/etc/default/pgbackrest` on the host with:

```
PGBACKREST_CONTAINER=<coolify-container-name>
PGBACKREST_UPTIME_KUMA_URL=<uptime-kuma-push-url>
```

Then enable the timers:

```bash
systemctl daemon-reload
systemctl enable --now pgbackrest-full.timer pgbackrest-diff.timer
```

### Monitoring

On success, the backup script pings an Uptime Kuma push monitor. If no ping arrives within the expected window, Uptime Kuma sends an alert. Failed backups are also visible in `journalctl -u pgbackrest-full.service` or `journalctl -u pgbackrest-diff.service`.

## Direct Database Access Logging

The image bakes in `/etc/postgresql/logging.conf` (from `logging.conf` here, the source of truth for these settings), which turns on connection, disconnection, and statement-level logging (`log_statement = 'all'`, with `log_parameter_max_length = 0` and `log_parameter_max_length_on_error = 0` so extended-protocol bind parameters are omitted) with a `log_line_prefix` that records timestamp, PID, role, database, `application_name`, and client host/port.

- **Fresh clusters:** `conf.sh` appends `include = '/etc/postgresql/logging.conf'` to `$PGDATA/postgresql.conf` on first init. Note that `logging.conf` baked into the image sets cluster-wide `log_statement = 'all'` with **no per-role exclusion**, so fresh clusters log every role until an approved per-role override is applied.
- **Restored or custom-config clusters:** If the container starts with `-c config_file=/etc/postgresql/postgresql.conf`, add `include '/etc/postgresql/logging.conf'` to the custom configuration file (or copy the settings from `logging.conf` directly). Pushing changes under `warehouse-postgres-docker/` rebuilds and publishes the image via CI; clusters pinned by image digest are not redeployed automatically.
- **Log shipping:** `log_destination = 'stderr'` and `logging_collector = off` are kept on purpose so PostgreSQL writes to container stderr, where the container runtime forwards logs to the central log store while keeping recent logs readable via `docker logs`. See `runbooks/prod-health-monitoring.md` §12 for inspection, proposed per-role exclusions, PG 18 (`log_connections = 'all'`), and verification steps.
