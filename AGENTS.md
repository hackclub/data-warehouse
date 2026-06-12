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

## Deployment Notes
- **Production**: Uses Docker with configs in `docker_deploy/` directory
- **Dagster Config**: Production dagster.yaml is in `docker_deploy/`, not project root
- **Postgres**: We build a custom Postgres image from this repo and use it
- **Always check existing deployment setup before adding configuration files**

## Servers / Etc
This is deployed on Coolify to the server `ssh root@warehouse.limited.selfhosted.hackclub.com`. You can SSH into it (read only) to inspect how healthy the server is. You can also check `.env` in the parent repo if you're on a worktree to to access the prod Dagster database to query it to get logs and understand health of the system.

## Public Repo
This repository is public. Always triple check your changes before commiting and pushing them. Never, ever publish secret data publicly - even stuff that may not feel private, like people's names in a test.
