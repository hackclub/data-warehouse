"""Check every program referenced by index.json before publication.

Also usable against a downloaded/generated site:
    uv run python -m orpheus_engine.defs.ysws_true_spend_site.reconciliation SITE_DIR

Private organizations remain summarized, never exposed to make a sum match.
"""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict


def reconcile_programs(documents: Dict[str, Any]) -> list:
    """Return exact-cent reconciliation rows, raising on any unexplained gap.

    With no withheld spend this enforces the literal listed-transactions sum.
    Otherwise only explicitly declared *true* spend may explain the difference;
    gross withheld outflows include non-spend and cannot stand in for it.
    """
    index = documents["index.json"]
    results = []
    for program in index["ysws_programs_with_linked_hcbs"] + index["ysws_marketing"]:
        document = documents[program["json"]]
        listed = sum((Decimal(str(t["amount_dollars"]))
                      for t in document["spend_transactions"] if t["counted_as_spend"]),
                     Decimal(0))
        withheld = sum((Decimal(str(w["true_spend_dollars"]))
                        for w in document["withheld_orgs"]), Decimal(0))
        total = Decimal(str(document["totals"]["true_spend_dollars"]))
        if listed + withheld != total or total != Decimal(str(program["true_spend_dollars"])):
            raise ValueError(
                f"True-spend reconciliation failed for {program['root_slug']}: "
                f"listed {listed:.2f} + withheld {withheld:.2f} != "
                f"detail/index totals {total:.2f}/{program['true_spend_dollars']}"
            )
        results.append({"root_slug": program["root_slug"], "listed": listed,
                        "withheld": withheld, "total": total})
    return results


def validate_site_files(files: Dict[str, Any]) -> list:
    """Validate the exact JSON bytes destined for publication."""
    index = json.loads(files["index.json"])
    documents = {"index.json": index}
    for program in index["ysws_programs_with_linked_hcbs"] + index["ysws_marketing"]:
        documents[program["json"]] = json.loads(files[program["json"]])
    return reconcile_programs(documents)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site_dir", type=Path)
    args = parser.parse_args()
    files = {"index.json": (args.site_dir / "index.json").read_text()}
    index = json.loads(files["index.json"])
    for program in index["ysws_programs_with_linked_hcbs"] + index["ysws_marketing"]:
        files[program["json"]] = (args.site_dir / program["json"]).read_text()
    results = validate_site_files(files)
    for result in results:
        if result["withheld"]:
            print(f"{result['root_slug']}: listed {result['listed']:.2f} + "
                  f"withheld {result['withheld']:.2f} = {result['total']:.2f}")
    print(f"Reconciled {len(results)} programs (including marketing).")


if __name__ == "__main__":
    main()
