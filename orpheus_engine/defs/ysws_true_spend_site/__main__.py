"""
Render the YSWS true-spend site to a local directory (no git, no push).

    uv run python -m orpheus_engine.defs.ysws_true_spend_site --out /tmp/ysws-site

Needs WAREHOUSE_COOLIFY_URL in the environment or in the repo's .env.
"""

import argparse
import os
from pathlib import Path

from .definitions import build_site_files, _write_files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="output directory (created/overwritten)")
    args = parser.parse_args()

    if not os.getenv("WAREHOUSE_COOLIFY_URL"):
        env_file = Path(__file__).resolve().parents[3] / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("WAREHOUSE_COOLIFY_URL="):
                    os.environ["WAREHOUSE_COOLIFY_URL"] = line.split("=", 1)[1].strip()
                    break

    built = build_site_files()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    _write_files(out, built["files"])
    print(
        f"{len(built['files'])} files, {built['bytes'] / 1_000_000:.1f} MB -> {out}\n"
        f"{built['program_count']} programs, {built['org_count']} orgs, "
        f"{built['spend_transaction_count']} spend / "
        f"{built['revenue_transaction_count']} revenue transactions\n"
        f"true spend ${built['total_true_spend_dollars']:,.2f}, "
        f"revenue ${built['total_external_revenue_dollars']:,.2f}"
    )


if __name__ == "__main__":
    main()
