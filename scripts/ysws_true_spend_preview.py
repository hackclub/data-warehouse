#!/usr/bin/env python3
"""
Preview harness for the YSWS true-spend site (dev loop, not production).

Renders the site to a versioned build directory on this machine and flips a
`current` symlink at it, so a single stable URL always shows the newest build
and a browser refresh is the whole review loop:

    uv run python scripts/ysws_true_spend_preview.py

    -> https://porygon.ocelot-basilisk.ts.net:8899/   (tailnet only)

Warehouse rows are cached in the preview root, so iterating on layout costs a
render (~2 s) instead of a re-query (~10 s). The cache is refreshed
automatically once it is older than --max-data-age, or on demand with
--refresh-data.

Old builds stay reachable at /builds/<id>/ for before-and-after comparison.
"""

import argparse
import os
import pickle
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# The dbt models behind the site. Rebuilt into the warehouse with --dbt, which
# is needed after editing a model (and after the scheduled prod dbt run rebuilds
# them from main, dropping any column this branch added).
TRUE_SPEND_MODELS = [
    "ysws_spend_programs",
    "ysws_spend_org_tree",
    "marketing_videos_db_payments",
    "ysws_spend_ledger",
    "ysws_spend_by_program",
    "ysws_spend_monthly",
    "ysws_unmatched_orgs",
    "ysws_unlinked_programs",
]

DEFAULT_ROOT = Path.home() / "previews" / "ysws-true-spend-site"
DEFAULT_PORT = 8899
# Tailnet hostname of this machine; the preview is tailnet-only (no funnel).
PREVIEW_HOST = os.environ.get("YSWS_PREVIEW_HOST", "porygon.ocelot-basilisk.ts.net")
KEEP_BUILDS = 4

ROOT_INDEX = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="0; url=current/">
<title>YSWS true spend preview</title></head>
<body><p><a href="current/">Latest build</a> &middot;
<a href="builds/">All builds</a></p></body></html>
"""


def _load_env() -> None:
    if os.getenv("WAREHOUSE_COOLIFY_URL") and os.getenv("PROD_DAGSTER_DB_URL"):
        return
    wanted = ("WAREHOUSE_COOLIFY_URL", "PROD_DAGSTER_DB_URL")
    for candidate in (
        REPO_ROOT / ".env",
        Path.home() / "dev" / "hackclub" / "data-warehouse" / ".env",
    ):
        if not candidate.exists():
            continue
        for line in candidate.read_text().splitlines():
            line = line.strip()
            for key in wanted:
                if line.startswith(key + "="):
                    os.environ.setdefault(key, line.split("=", 1)[1].strip())
        if os.getenv("WAREHOUSE_COOLIFY_URL"):
            return


def run_dbt(models=TRUE_SPEND_MODELS) -> None:
    """Rebuild the true-spend models in the warehouse (target: prod)."""
    _load_env()
    for line in (Path.home() / "dev" / "hackclub" / "data-warehouse" / ".env").read_text().splitlines():
        if line.startswith("HACK_CLUBBERS_LOCATION_SALT="):
            os.environ.setdefault("HACK_CLUBBERS_LOCATION_SALT", line.split("=", 1)[1].strip())
    from orpheus_engine.defs.dbt.definitions import DBT_PROFILES_DIR_PATH

    dbt_bin = REPO_ROOT / ".venv" / "bin" / "dbt"
    started = time.time()
    result = subprocess.run(
        [
            str(dbt_bin) if dbt_bin.exists() else "dbt", "run",
            "--select", *models,
            "--project-dir", str(REPO_ROOT / "orpheus_engine_dbt"),
            "--profiles-dir", str(DBT_PROFILES_DIR_PATH),
            "--target", "prod",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        tail = "\n".join((result.stdout or result.stderr).splitlines()[-25:])
        sys.exit(f"dbt run failed:\n{tail}")
    built = [l for l in result.stdout.splitlines() if "OK created" in l]
    print(f"dbt: rebuilt {len(built)} models in {time.time() - started:.0f}s")


def _fetch_data():
    from orpheus_engine.defs.ysws_true_spend_site.data import fetch_site_data
    from orpheus_engine.defs.ysws_true_spend_site.freshness import dagster_times_from_db
    import psycopg2

    _load_env()
    url = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not url:
        sys.exit("WAREHOUSE_COOLIFY_URL not found (env or .env)")
    conn = psycopg2.connect(url)
    try:
        data = fetch_site_data(conn)
    finally:
        conn.close()

    # In production the asset reads these off its own Dagster instance; here the
    # prod Dagster database is the only place that knows when the HCB mirror and
    # the dbt models last succeeded.
    dagster_url = os.getenv("PROD_DAGSTER_DB_URL")
    if dagster_url and data.freshness is not None:
        pulled, recalculated = dagster_times_from_db(dagster_url)
        data.freshness.hcb_pulled_at = pulled
        data.freshness.recalculated_at = recalculated
    elif not dagster_url:
        print("note: PROD_DAGSTER_DB_URL not set - pull/recalc times will show as unknown")
    return data


def load_data(root: Path, refresh: bool, max_age: float):
    """Warehouse rows, from the local cache when it is fresh enough."""
    cache = root / "data.pkl"
    if not refresh and cache.exists():
        age = time.time() - cache.stat().st_mtime
        if age <= max_age:
            with cache.open("rb") as fh:
                data = pickle.load(fh)
            print(f"data: cache hit ({age / 60:.0f} min old)")
            return data, age
    started = time.time()
    data = _fetch_data()
    root.mkdir(parents=True, exist_ok=True)
    with cache.open("wb") as fh:
        pickle.dump(data, fh, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"data: queried warehouse in {time.time() - started:.1f}s")
    return data, 0.0


def write_build(root: Path, data, build_id: str) -> Path:
    from orpheus_engine.defs.ysws_true_spend_site.definitions import _write_files
    from orpheus_engine.defs.ysws_true_spend_site.site import render_site

    started = time.time()
    files = render_site(data, datetime.now(timezone.utc))
    build_dir = root / "builds" / build_id
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)
    _write_files(build_dir, files)
    size = sum(
        len(c) if isinstance(c, bytes) else len(c.encode("utf-8"))
        for c in files.values()
    )
    print(
        f"build {build_id}: {len(files)} files, {size / 1_000_000:.1f} MB "
        f"in {time.time() - started:.1f}s"
    )
    return build_dir


def swap_current(root: Path, build_dir: Path) -> None:
    """Point `current` at the new build. Atomic, so no half-built page is served."""
    link = root / "current"
    tmp = root / ".current.new"
    if tmp.exists() or tmp.is_symlink():
        tmp.unlink()
    tmp.symlink_to(build_dir.resolve())
    os.replace(tmp, link)
    (root / "index.html").write_text(ROOT_INDEX)


def prune_builds(root: Path, keep: int) -> None:
    builds = sorted((root / "builds").glob("*"), key=lambda p: p.name, reverse=True)
    current = (root / "current").resolve()
    for stale in builds[keep:]:
        if stale.resolve() != current:
            shutil.rmtree(stale, ignore_errors=True)


def _port_open(port: int) -> bool:
    with socket.socket() as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def serve_forever(root: Path, port: int) -> None:
    """Static server with caching disabled, so a browser refresh always shows
    the newest build instead of a cached copy of the previous one."""
    from functools import partial
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

    class NoCacheHandler(SimpleHTTPRequestHandler):
        def end_headers(self):
            self.send_header("Cache-Control", "no-store, must-revalidate")
            self.send_header("Expires", "0")
            SimpleHTTPRequestHandler.end_headers(self)

    handler = partial(NoCacheHandler, directory=str(root))
    ThreadingHTTPServer(("127.0.0.1", port), handler).serve_forever()


def ensure_server(root: Path, port: int) -> bool:
    """Start a detached static server on `port` if nothing is listening yet."""
    if _port_open(port):
        return False
    log = root / "server.log"
    # start_new_session detaches the server into its own process group, so
    # stopping the tool call that launched it (or this script exiting) does not
    # take the preview down with it.
    with log.open("a") as handle:
        subprocess.Popen(
            [
                sys.executable, str(Path(__file__).resolve()),
                "--serve-only", "--root", str(root), "--port", str(port),
            ],
            stdout=handle, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    for _ in range(40):
        if _port_open(port):
            return True
        time.sleep(0.1)
    sys.exit(f"server did not come up on port {port}; see {log}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--refresh-data", action="store_true",
                        help="re-query the warehouse even if the cache is fresh")
    parser.add_argument("--dbt", action="store_true",
                        help="rebuild the true-spend dbt models first (implies --refresh-data)")
    parser.add_argument("--max-data-age", type=float, default=1800,
                        help="seconds before the cached warehouse rows are refetched")
    parser.add_argument("--keep", type=int, default=KEEP_BUILDS)
    parser.add_argument("--no-serve", action="store_true")
    parser.add_argument("--serve-only", action="store_true",
                        help="internal: run the static server in the foreground")
    args = parser.parse_args()

    root = args.root.expanduser()
    root.mkdir(parents=True, exist_ok=True)

    if args.serve_only:
        serve_forever(root, args.port)
        return

    if args.dbt:
        run_dbt()
    data, age = load_data(root, args.refresh_data or args.dbt, args.max_data_age)
    build_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    build_dir = write_build(root, data, build_id)
    swap_current(root, build_dir)
    prune_builds(root, args.keep)

    if not args.no_serve:
        started = ensure_server(root, args.port)
        print("server:", "started" if started else "already running", f"on port {args.port}")

    print(
        f"programs {len(data.programs)}, orgs "
        f"{sum(len(v) for v in data.orgs_by_program.values())}, transactions "
        f"{data.transaction_count}, data {age / 60:.0f} min old"
    )
    print(f"\n  https://{PREVIEW_HOST}:{args.port}/")
    print(f"  https://{PREVIEW_HOST}:{args.port}/builds/{build_id}/   (this build, pinned)")


if __name__ == "__main__":
    main()
