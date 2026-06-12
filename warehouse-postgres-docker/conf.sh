#!/bin/bash
set -e

if [ -z "$PGBACKREST_STANZA" ]; then
  echo "FATAL: PGBACKREST_STANZA is not set. Cannot configure WAL archiving." >&2
  exit 1
fi

cat >> "$PGDATA/postgresql.conf" <<EOF

# WAL archiving for pgBackRest
wal_level = replica
archive_mode = on
# --process-max on the command line: env PGBACKREST_PROCESS_MAX (meant for
# backup parallelism) would otherwise override the archive-push setting in
# /etc/pgbackrest/pgbackrest.conf, since pgBackRest precedence is CLI > env > config.
archive_command = 'pgbackrest --stanza=$PGBACKREST_STANZA --process-max=8 archive-push %p'
archive_timeout = 60
EOF
