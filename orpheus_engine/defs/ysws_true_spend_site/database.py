"""
The same documents, as a DuckDB file.

Third rendering of the JSON documents (after HTML and the JSON itself), so it
carries exactly what the pages show and nothing else. Built for people who would
rather write SQL than walk 266 JSON files:

    duckdb ysws-true-spend.duckdb
    SELECT name, true_spend_dollars FROM programs ORDER BY 2 DESC LIMIT 10;

Tables mirror the documents: programs, program_orgs, spend_transactions,
revenue_transactions, unmatched_orgs, unlinked_programs, budgets,
budget_transactions, people_without_budget, metadata.
"""

import tempfile
from pathlib import Path
from typing import Any, Dict, List

DUCKDB_FILENAME = "ysws-true-spend.duckdb"


def _flat_programs(program_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for document in program_documents:
        row = {
            "name": document["name"],
            "root_slug": document["root_slug"],
            "is_ysws_program": document["is_ysws_program"],
            "hcb_url": document["hcb_url"],
            "hcb_org_count": document["hcb_org_count"],
            "first_outflow_date": document["first_outflow_date"],
            "last_outflow_date": document["last_outflow_date"],
            "weighted_projects": document["weighted_projects"],
            "weighted_hours": document["weighted_hours"],
            "approved_project_count": document["approved_project_count"],
            "cost_per_weighted_hour": document["cost_per_weighted_hour"],
        }
        row.update(document["totals"])
        rows.append(row)
    return rows


def _flat_orgs(program_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The org trees, flattened with depth and parent so the shape survives."""
    rows = []

    def walk(document, nodes, parent_slug, depth):
        for node in nodes:
            rows.append({
                "program": document["name"],
                "root_slug": document["root_slug"],
                "org_slug": node["slug"],
                "org_name": node["name"],
                "parent_org_slug": parent_slug,
                "depth": depth,
                "hcb_url": node["hcb_url"],
                "external_revenue_dollars": node.get("external_revenue_dollars"),
                "true_spend_dollars": node["true_spend_dollars"],
                "balance_dollars": node["balance_dollars"],
                "transaction_count": node["transaction_count"],
            })
            walk(document, node["children"], node["slug"], depth + 1)

    for document in program_documents:
        walk(document, document["orgs"], None, 0)
    return rows


def _flat_transactions(
    program_documents: List[Dict[str, Any]], key: str, extra: Dict[str, Any]
) -> List[Dict[str, Any]]:
    rows = []
    for document in program_documents:
        for txn in document[key]:
            rows.append({
                "program": document["name"],
                "root_slug": document["root_slug"],
                **txn,
                **extra,
            })
    return rows


def _flat_budgets(budget_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for document in budget_documents:
        row = {
            "budget_name": document["budget_name"],
            "person_name": document["person_name"],
            "slug": document["slug"],
            "hcb_url": document["hcb_url"],
            "matched_by": document["matched_by"],
            "has_person": document["has_person"],
            "is_also_program_root": document["is_also_program_root"],
            "also_program_name": document["also_program_name"],
            "first_activity_date": document["first_activity_date"],
            "last_activity_date": document["last_activity_date"],
        }
        row.update(document["totals"])
        rows.append(row)
    return rows


def _flat_budget_transactions(budget_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for document in budget_documents:
        for key in ("spend_transactions", "funding_transactions"):
            for txn in document[key]:
                rows.append({
                    "budget_name": document["budget_name"],
                    "person_name": document["person_name"],
                    "slug": document["slug"],
                    **txn,
                })
    return rows


def build_duckdb(index_document: Dict[str, Any],
                 program_documents: List[Dict[str, Any]],
                 budget_documents: List[Dict[str, Any]] = ()) -> bytes:
    """Return a DuckDB database file holding every published table."""
    import duckdb
    import polars as pl

    tables: Dict[str, List[Dict[str, Any]]] = {
        "metadata": [index_document["metadata"]],
        "programs": _flat_programs(program_documents),
        "program_orgs": _flat_orgs(program_documents),
        "spend_transactions": _flat_transactions(
            program_documents, "spend_transactions", {}
        ),
        "revenue_transactions": (
            _flat_transactions(program_documents, "revenue_transactions",
                               {"is_intra_tree": False})
            + _flat_transactions(program_documents, "intra_tree_transactions",
                                 {"is_intra_tree": True})
        ),
        "withheld_orgs": [
            {"program": d["name"], "root_slug": d["root_slug"], **w}
            for d in program_documents for w in d["withheld_orgs"]
        ],
        "unmatched_orgs": [
            # related_programs is a list in JSON; SQL wants a scalar.
            {**o, "related_programs": ", ".join(o["related_programs"])}
            for o in index_document["hcb_orgs_no_program_claims"]
        ],
        "unlinked_programs": index_document["ysws_programs_with_no_linked_hcbs"],
        "budgets": _flat_budgets(list(budget_documents)),
        "budget_transactions": _flat_budget_transactions(list(budget_documents)),
        "people_without_budget": index_document["ysws_people_with_no_linked_individual_budget"],
    }

    with tempfile.TemporaryDirectory(prefix="ysws-duckdb-") as tmp:
        path = Path(tmp) / DUCKDB_FILENAME
        con = duckdb.connect(str(path))
        try:
            for name, rows in tables.items():
                if not rows:
                    continue
                frame = pl.DataFrame(rows, infer_schema_length=None)  # noqa: F841
                con.execute(f"CREATE TABLE {name} AS SELECT * FROM frame")
        finally:
            con.close()
        return path.read_bytes()
