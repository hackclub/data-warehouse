#!/bin/bash
set -euo pipefail

# Set these for your environment
CONTAINER="${PGBACKREST_CONTAINER:?Set PGBACKREST_CONTAINER to the Coolify container name}"
UPTIME_KUMA_URL="${PGBACKREST_UPTIME_KUMA_URL:?Set PGBACKREST_UPTIME_KUMA_URL to the push monitor URL}"

TYPE="${1:?Usage: pgbackrest-backup.sh <full|diff>}"

# Drain the WAL archive backlog before starting. pgBackRest forces a WAL switch at
# backup start and aborts (error 82) if that segment is not archived within
# --archive-timeout; a transient backlog at 03:00 broke the 2026-06-01 full.
# Wait up to 60 min for the backlog to clear, then proceed regardless so a truly
# stuck archiver still surfaces as a failed (un-pinged) backup rather than hanging.
echo "Checking WAL archive backlog before ${TYPE} backup..."
for _ in $(seq 1 60); do
    READY=$(docker exec "$CONTAINER" sh -c "ls /var/lib/postgresql/data/pg_wal/archive_status/ 2>/dev/null | grep -c .ready || true")
    echo "  ${READY} WAL segment(s) waiting to archive"
    [ "${READY:-0}" -lt 100 ] && break
    sleep 60
done

echo "Starting pgbackrest ${TYPE} backup..."
docker exec "$CONTAINER" pgbackrest --stanza=warehouse --type="$TYPE" --archive-timeout=600 backup
echo "Backup completed successfully."

# Ping Uptime Kuma on success
curl -fsS -m 10 --retry 5 "${UPTIME_KUMA_URL}?status=up&msg=${TYPE}%20backup%20OK&ping="
