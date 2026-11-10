# AGENTS.md

Welcome to the Hack Club Data Warehouse. This is a Dagster project, which we develop and run using uv, and an mcp server for querying it. The data warehouse is used to generate dashboards and do reporting.

## Code Style
- **DataFrames**: Use Polars (`pl.DataFrame`) as primary data processing library (NOT pandas)
- **Environment**: Use `.env` file for local development; `DAGSTER_ENV=development` locally. If you are on a worktree and need access to `.env`, please check the upstream repo.

## Project Structure
- Main code: `orpheus_engine/` (assets, resources, definitions)
- Tests: `orpheus_engine_tests/`
- Config: `pyproject.toml` (not setup.py)
- **Deployment**: `docker_deploy/` directory contains Docker deployment configs
  - `docker_deploy/Dockerfile` - production Docker setup
  - `docker_deploy/dagster.yaml` - Dagster instance config for production (NOT project root)
  - Docker sets `DAGSTER_HOME=/opt/dagster/dagster_home` and copies dagster.yaml there

Note: If you are working in a git worktree and need credentials, check the parent repo's .env.

## Deployment Notes
- **Production**: Uses Docker with configs in `docker_deploy/` directory
- **Dagster Config**: Production dagster.yaml is in `docker_deploy/`, not project root
- **Postgres**: We build a custom Postgres image from this repo and use it
- **Always check existing deployment setup before adding configuration files**

## Servers / Etc
This is deployed on Coolify to the server `ssh root@warehouse.limited.selfhosted.hackclub.com`. You can SSH into it (read only) to inspect how healthy the server is. You can also check `.env` in the parent repo if you're on a worktree to to access the prod Dagster database to query it to get logs and understand health of the system.

**One exception to read only:** `bin/warehouse-admin-psql` and `bin/warehouse-dbt --elevated` write through the host. Use them only with a written `--reason`, and only for a runbook step that names them or a request that asks for the write. See `runbooks/prod-health-monitoring.md` §10.

**`WAREHOUSE_COOLIFY_URL` has three different values, on purpose.** In the parent `.env` and Vaultwarden's `.env` note it is the agents' login, `hc_warehouse_agent`, which cannot change existing data. In the `warehouse-mcp` Coolify app only, it is that tool's login, `hc_warehouse_mcp`, with the same grants. In the Dagster app's Coolify settings, the host `.env` and that app's containers it is the superuser login the syncs use. Never copy it from one place to another: that breaks every sync or the query tool's own limits.

**`PROD_DAGSTER_DB_URL` is read-only on purpose.** In the parent `.env` and Vaultwarden it is `dagster_reader`, which cannot change Dagster's tables. Change Dagster state through GraphQL, not SQL. Never copy the superuser value from Coolify or the host into the parent `.env` or Vaultwarden. See "The agents' read-only Dagster login" in `runbooks/prod-health-monitoring.md`.

## Public Repo
This repository is public. Always triple check your changes before commiting and pushing them. Never, ever publish secret data publicly - even stuff that may not feel private, like people's names in a test.

### Never commit agent scratch work
- `work/` is local scratch space (scripts, SQL, query results, notes, decks). It is in `.gitignore`. Never commit it, never `git add -f` it, and never remove it from `.gitignore`.
- On 2026-11-10 two audit and analysis folders under `work/` were pushed to `main` and the history had to be rewritten. Don't repeat it.
- Ship findings as a runbook in `runbooks/` or as code with tests, written for a public audience. Share raw evidence through private channels, not this repo.
- Before every commit, run `git diff --cached --stat`. If any path starts with `work/`, `.factory-scratch/` or `scratch/`, unstage it.
- `orpheus_engine_tests/test_no_scratch_tracked.py` fails if Git tracks any of those paths.
