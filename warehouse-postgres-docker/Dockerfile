ARG BASE_IMAGE=postgres:17-alpine

## Custom Alpine Postgres docker file with pg_rowhash extension
## pg_rowhash: Fast row hashing using xxHash - hashes binary tuples directly
FROM ${BASE_IMAGE} AS builder

# Install required dependencies for building the extension. The clang/llvm
# major must match the one the base image's PostgreSQL was built with (PGXS
# CLANG = clang-N); it moves with the Alpine release (19 on 3.23, 21 on 3.24),
# so read it instead of pinning it.
RUN LLVM_MAJOR="$(sed -n 's/^CLANG = clang-\([0-9]*\)$/\1/p' /usr/local/lib/postgresql/pgxs/src/Makefile.global)" && \
    test -n "$LLVM_MAJOR" && \
    apk --no-cache add \
    make \
    gcc \
    g++ \
    "clang${LLVM_MAJOR}" \
    "llvm${LLVM_MAJOR}" \
    postgresql-dev

# Copy pg_rowhash source
COPY pg_rowhash /rowhash

WORKDIR /rowhash

# Build and install pg_rowhash
RUN make && make install

## Final image - start fresh from base
FROM ${BASE_IMAGE}

## Copy all extensions from the builder stage
COPY --from=builder /usr/local/lib/postgresql/* /usr/local/lib/postgresql/
COPY --from=builder /usr/local/share/postgresql/extension/* /usr/local/share/postgresql/extension/

# Add pgbackrest client for WAL archiving
RUN apk add --no-cache pgbackrest && \
    mkdir -p /tmp/pgbackrest /var/spool/pgbackrest && \
    chown postgres:postgres /tmp/pgbackrest /var/spool/pgbackrest

# Async parallel WAL archiving (overwrites the package's stock config)
COPY pgbackrest.conf /etc/pgbackrest/pgbackrest.conf

# Configure PostgreSQL for WAL archiving on first init
COPY conf.sh /docker-entrypoint-initdb.d/conf.sh
