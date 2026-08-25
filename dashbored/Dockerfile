# Build context: repo root (docker build -f dashbored/Dockerfile .)
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.4@sha256:c4089b0085cf4d38e38d5cdaa5e57752c1878a6f41f2e3a3a234dc5f23942cb4 /uv /usr/local/bin/uv

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
      curl libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g yarn@1.22.22

ARG AGE_VERSION=v1.2.1
RUN ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
      amd64) AGE_SHA256=7df45a6cc87d4da11cc03a539a7470c15b1041ab2b396af088fe9990f7c79d50 ;; \
      arm64) AGE_SHA256=57fd79a7ece5fe501f351b9dd51a82fbee1ea8db65a8839db17f5c080245e99f ;; \
      *) echo "no pinned age build for $ARCH" >&2; exit 1 ;; \
    esac && \
    curl -fsSL -o /tmp/age.tgz \
      "https://github.com/FiloSottile/age/releases/download/${AGE_VERSION}/age-${AGE_VERSION}-linux-${ARCH}.tar.gz" && \
    echo "${AGE_SHA256}  /tmp/age.tgz" | sha256sum -c - && \
    tar xzf /tmp/age.tgz --strip-components=1 -C /usr/local/bin age/age age/age-keygen && \
    rm /tmp/age.tgz

WORKDIR /app

COPY dashbored/pyproject.toml dashbored/uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt -o /tmp/requirements.txt && \
    uv pip install --system --require-hashes -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

COPY dashbored/package.json dashbored/yarn.lock ./
RUN yarn install --frozen-lockfile

COPY dashbored/ .

# validate_locally dry-runs the generated patches against the real upstream
# sources, so /api/validate_code can check generated code before a human
# reviews it. These are the same files, from the same commit, not a stale
# clone — if the model shape changes, the validator sees it immediately.
COPY orpheus_engine/defs/ /opt/orpheus-engine/orpheus_engine/defs/
COPY orpheus_engine_dbt/models/ /opt/orpheus-engine/orpheus_engine_dbt/models/
ENV REPO_ROOT=/opt/orpheus-engine

RUN yarn build

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", \
     "--worker-class", "gthread", "--workers", "2", "--threads", "8", \
     "--timeout", "180", "--graceful-timeout", "30", \
     "--access-logfile", "-", \
     "app:create_app()"]
