"""
The site's JSON documents — the single source the HTML is rendered from.

Pipeline: dbt models -> data.py (SQL rows) -> documents.py (JSON) -> site.py
(HTML). The HTML renderer sees nothing but these documents, so a page cannot
show a number the JSON lacks: adding a field to a page means adding it here,
and it lands in both surfaces at once.

Two documents, mirroring the two kinds of page:

  index.json                 metadata + the four sections of the home page
  programs/<root_slug>.json  one program: totals, category breakdown, HCB org
                             tree, and every transaction listed on its page

Redaction happens here rather than in the renderer, so the JSON is publishable
under the same rules as the HTML: email addresses stripped, and transactions of
organizations outside HCB's transparency mode summarised instead of listed.
"""

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .data import SiteData
from .freshness import Freshness

HCB_ORG_URL = "https://hcb.hackclub.com/{slug}"

# See site.py's publication policy: HCB publishes names but never email
# addresses, and publishes nothing at all for organizations outside
# transparency mode.
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
EMAIL_PLACEHOLDER = "[email hidden]"
REDACT_EMAILS = True
HIDE_NON_TRANSPARENT_ORG_DETAIL = True

CATEGORY_ORDER = ["A", "C", "M", "B", "D", "X", "I"]
CATEGORY_LABELS = {
    "A": "A — spent on the event (grants to makers, external card/ACH/wire/check spend)",
    "C": "C — internal cost (postage, printing, fulfillment, hosting, fines)",
    "M": "M — offset: budget funded by marketing (negative, avoids double counting)",
    "B": "B — into personal author/reviewer funds (not this program's spend)",
    "D": "D — returned to HQ (overfunding)",
    "X": "X — other internal transfer (round-trip washes, other programs)",
    "I": "I — intra-tree transfer (netted; the sub-org's own spend is counted)",
}
TRUE_SPEND_CATEGORIES = {"A", "C", "M"}

UNMATCHED_REASONS = {
    "parent_of_mapped_root": "parent of a linked program",
    "funded_by_mapped_program": "took money from programs",
    "funds_mapped_program": "sent money into programs",
}

GAP_TYPES = {
    "no_hcb_link": "No HCB link on the Unified YSWS DB record",
    "unparseable_hcb": "The hcb field is not an hcb.hackclub.com org URL",
    "org_not_found": "The linked slug matches no HCB org",
    "org_deleted": "The linked HCB org is deleted",
}

_SAFE_SLUG = re.compile(r"^[A-Za-z0-9._-]+$")


# --- value helpers ----------------------------------------------------------

def _dec(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _money(value: Any) -> Optional[float]:
    """Dollars as a JSON number. None stays None: absent is not zero."""
    if value is None:
        return None
    return float(_dec(value).quantize(Decimal("0.01")))


def _text(value: Any) -> Optional[str]:
    """Free text as published: emails stripped, everything else verbatim."""
    if value is None:
        return None
    text = str(value)
    if REDACT_EMAILS:
        text = _EMAIL.sub(EMAIL_PLACEHOLDER, text)
    return text


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def page_slug(root_slug: str, root_event_id: Any) -> str:
    """File-safe page name. HCB slugs already are, but never trust them."""
    if root_slug and _SAFE_SLUG.match(root_slug):
        return root_slug
    return f"program-{root_event_id}"


# --- org tree ---------------------------------------------------------------

def _org_node(org: Dict[str, Any], include_revenue: bool) -> Dict[str, Any]:
    """
    One org, carrying the columns its page shows. The index tree shows spend,
    balance and transaction count; the program page's tree adds revenue.
    """
    node = {
        "name": org["org_name"] or org["org_slug"],
        "slug": org["org_slug"],
        "hcb_url": HCB_ORG_URL.format(slug=org["org_slug"]),
    }
    if include_revenue:
        node["external_revenue_dollars"] = _money(org["external_revenue_dollars"])
    node.update({
        "true_spend_dollars": _money(org["true_spend_dollars"]),
        "balance_dollars": _money(org["balance_dollars"]),
        "transaction_count": int(org["transaction_count"] or 0),
        "children": [],
    })
    return node


def _org_tree(orgs: List[Dict[str, Any]], include_revenue: bool = True) -> List[Dict[str, Any]]:
    """
    The program's orgs as HCB nests them. Any org whose parent is outside this
    program's tree (the root itself, or a manually attached org) sits at the top
    level. Children are ordered by spend so the biggest branch reads first.
    """
    ids = {o["event_id"] for o in orgs}
    nodes = {o["event_id"]: _org_node(o, include_revenue) for o in orgs}
    roots: List[Dict[str, Any]] = []
    for org in orgs:
        parent = org["parent_id"] if org["parent_id"] in ids else None
        if parent is None:
            roots.append(nodes[org["event_id"]])
        else:
            nodes[parent]["children"].append(nodes[org["event_id"]])

    def sort(level: List[Dict[str, Any]]) -> None:
        level.sort(key=lambda n: (-_dec(n["true_spend_dollars"]), n["name"].lower()))
        for node in level:
            sort(node["children"])

    sort(roots)
    return roots


# --- transactions -----------------------------------------------------------

def _spend_transaction(txn: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": _iso(txn["transaction_date"]),
        "org_slug": txn["org_slug"],
        "category": txn["spend_category"],
        "bucket": txn["spend_bucket"],
        "type": txn["transaction_type"],
        "description": _text(txn["description"]),
        "counterparty": _text(txn["counterparty"]),
        "initiated_by": _text(txn["initiated_by_name"]),
        "amount_dollars": _money(txn["outflow_dollars"]),
        "counted_as_spend": bool(txn["is_true_spend"]),
        "hcb_code": txn["hcb_code"],
        "hcb_url": txn["hcb_url"],
    }


def _revenue_transaction(txn: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": _iso(txn["transaction_date"]),
        "org_slug": txn["org_slug"],
        "type": txn["transaction_type"],
        "source": _text(txn["source"]),
        "description": _text(txn["description"]),
        "amount_dollars": _money(txn["amount_dollars"]),
        "hcb_code": txn["hcb_code"],
        "hcb_url": txn["hcb_url"],
    }


def _category_breakdown(spend_txns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    totals: Dict[str, Decimal] = {}
    counts: Dict[str, int] = {}
    for txn in spend_txns:
        category = txn["spend_category"] or "?"
        totals[category] = totals.get(category, Decimal(0)) + _dec(txn["outflow_dollars"])
        counts[category] = counts.get(category, 0) + 1
    order = [c for c in CATEGORY_ORDER if c in totals] + [
        c for c in sorted(totals) if c not in CATEGORY_ORDER
    ]
    return [
        {
            "category": category,
            "label": CATEGORY_LABELS.get(category, category),
            "transaction_count": counts[category],
            "dollars": _money(totals[category]),
            "counted_as_spend": category in TRUE_SPEND_CATEGORIES,
        }
        for category in order
    ]


def _withheld(
    orgs: List[Dict[str, Any]],
    spend_txns: List[Dict[str, Any]],
    revenue_txns: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Orgs HCB keeps private, with the totals that stand in for their rows.

    Their dollars remain in every total; only the line-level detail is withheld,
    exactly as HCB withholds it.
    """
    private = {
        o["org_slug"]: (o["org_name"] or o["org_slug"])
        for o in orgs
        if HIDE_NON_TRANSPARENT_ORG_DETAIL and not o.get("is_public", True)
    }
    if not private:
        return []
    out = []
    for slug, name in sorted(private.items()):
        spend = [t for t in spend_txns if t["org_slug"] == slug]
        revenue = [t for t in revenue_txns if t["org_slug"] == slug]
        if not spend and not revenue:
            continue
        out.append({
            "org_slug": slug,
            "org_name": name,
            "hcb_url": HCB_ORG_URL.format(slug=slug),
            "reason": "not in HCB transparency mode",
            "spend_transaction_count": len(spend),
            "spend_dollars": _money(sum((_dec(t["outflow_dollars"]) for t in spend), Decimal(0))),
            "revenue_transaction_count": len(revenue),
            "revenue_dollars": _money(sum((_dec(t["amount_dollars"]) for t in revenue), Decimal(0))),
        })
    return out


# --- documents --------------------------------------------------------------

def build_program_document(
    program: Dict[str, Any],
    orgs: List[Dict[str, Any]],
    spend_txns: List[Dict[str, Any]],
    revenue_txns: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """One program, with everything its page shows."""
    name = page_slug(program["root_slug"], program["root_event_id"])
    external = [t for t in revenue_txns if not t["is_intra_tree"]]
    intra = [t for t in revenue_txns if t["is_intra_tree"]]
    withheld = _withheld(orgs, spend_txns, external)
    private_slugs = {w["org_slug"] for w in withheld}

    return {
        "name": program["program_name"],
        "root_slug": program["root_slug"],
        "is_ysws_program": bool(program.get("is_ysws_program", True)),
        "hcb_url": HCB_ORG_URL.format(slug=program["root_slug"]),
        "page": f"programs/{name}.html",
        "json": f"programs/{name}.json",
        "hcb_org_count": int(program["org_count"] or 0),
        "first_outflow_date": _iso(program["first_outflow_date"]),
        "last_outflow_date": _iso(program["last_outflow_date"]),
        "weighted_projects": _money(program["weighted_projects"]),
        "weighted_hours": _money(program["weighted_hours"]),
        "approved_project_count": program["approved_project_count"],
        "cost_per_weighted_hour": _money(program["cost_per_weighted_hour"]),
        "totals": {
            "external_revenue_dollars": _money(program["external_revenue_dollars"]),
            "true_spend_dollars": _money(program["true_spend_dollars"]),
            "balance_dollars": _money(program["balance_dollars"]),
            "spent_on_event_dollars": _money(program["spent_on_event_dollars"]),
            "internal_cost_dollars": _money(program["internal_cost_dollars"]),
            "funded_by_marketing_dollars": _money(program["funded_by_marketing_dollars"]),
            "author_fund_dollars": _money(program["author_fund_dollars"]),
            "returned_to_hq_dollars": _money(program["returned_to_hq_dollars"]),
            "other_internal_dollars": _money(program["other_internal_dollars"]),
            "intra_tree_dollars": _money(program["intra_tree_dollars"]),
            "gross_outflow_dollars": _money(program["gross_outflow_dollars"]),
            "stated_outflow_dollars": _money(program["stated_outflow_dollars"]),
            "stated_overstatement_pct": _money(program["stated_overstatement_pct"]),
            "intra_tree_revenue_dollars": _money(program["intra_tree_revenue_dollars"]),
            "card_grants_funded_dollars": _money(program["card_grants_funded_dollars"]),
            "card_grants_remaining_dollars": _money(program["card_grants_remaining_dollars"]),
        },
        "category_breakdown": _category_breakdown(spend_txns),
        "orgs": _org_tree(orgs),
        "withheld_orgs": withheld,
        "spend_transactions": [
            _spend_transaction(t) for t in spend_txns if t["org_slug"] not in private_slugs
        ],
        "revenue_transactions": [
            _revenue_transaction(t) for t in external if t["org_slug"] not in private_slugs
        ],
        "intra_tree_transactions": [
            _revenue_transaction(t) for t in intra if t["org_slug"] not in private_slugs
        ],
    }


def index_summary(document: Dict[str, Any], orgs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    A program as the home page shows it: the row's columns, and the org tree the
    row expands to. Everything else — the category breakdown, the full totals,
    the transactions — is on the program's own page, and so in its own document.
    """
    return {
        "name": document["name"],
        "root_slug": document["root_slug"],
        "hcb_url": document["hcb_url"],
        "page": document["page"],
        "json": document["json"],
        "hcb_org_count": document["hcb_org_count"],
        "weighted_projects": document["weighted_projects"],
        "true_spend_dollars": document["totals"]["true_spend_dollars"],
        "cost_per_weighted_hour": document["cost_per_weighted_hour"],
        "balance_dollars": document["totals"]["balance_dollars"],
        "orgs": _org_tree(orgs, include_revenue=False),
    }


def build_index_document(
    data: SiteData,
    program_documents: List[Dict[str, Any]],
    generated_at: datetime,
) -> Dict[str, Any]:
    """The home page: metadata, then its four sections, in page order."""
    fresh = data.freshness or Freshness()
    summaries = [
        index_summary(d, data.orgs_by_program.get(d["root_slug"], []))
        for d in program_documents
    ]
    linked = [
        s for s, d in zip(summaries, program_documents) if d["is_ysws_program"]
    ]
    marketing = [
        s for s, d in zip(summaries, program_documents) if not d["is_ysws_program"]
    ]

    return {
        "metadata": {
            "title": "YSWS true spend",
            "hcb_data_pulled": _iso(fresh.hcb_pulled_at),
            "newest_hcb_record_held": _iso(fresh.hcb_data_through),
            "spend_recalculated": _iso(fresh.recalculated_at),
            "page_built": _iso(generated_at),
            "transaction_detail": (
                "Mirrors hcb.hackclub.com: names published, email addresses "
                "removed, organizations outside HCB transparency mode summarised "
                "rather than listed."
            ),
        },
        "ysws_programs_with_linked_hcbs": linked,
        "ysws_programs_with_no_linked_hcbs": [
            {
                "name": g["program_name"],
                "problem": GAP_TYPES.get(g["gap_type"], g["gap_type"]),
                "hcb_field": g["hcb_field"],
            }
            for g in data.unlinked_programs
        ],
        "ysws_marketing": marketing,
        "hcb_orgs_no_program_claims": [
            {
                "name": o["org_name"] or o["org_slug"],
                "slug": o["org_slug"],
                "hcb_url": o["hcb_url"],
                "why": UNMATCHED_REASONS.get(o["reason"], o["reason"]),
                "hcb_parent_slug": o["parent_slug"],
                "dollars_from_programs": _money(o["dollars_from_programs"]),
                "dollars_to_programs": _money(o["dollars_to_programs"]),
                "own_outflow_dollars": _money(o["gross_outflow_dollars"]),
                "balance_dollars": _money(o["balance_dollars"]),
                "related_programs": [
                    n.strip() for n in str(o["related_programs"] or "").split(",") if n.strip()
                ],
            }
            for o in data.unmatched_orgs
        ],
    }


def build_documents(data: SiteData, generated_at: datetime) -> Dict[str, Any]:
    """Every document this run publishes, keyed by its path."""
    program_documents = [
        build_program_document(
            program,
            data.orgs_by_program.get(program["root_slug"], []),
            data.spend_by_program.get(program["root_slug"], []),
            data.revenue_by_program.get(program["root_slug"], []),
        )
        for program in data.programs
    ]
    documents = {"index.json": build_index_document(data, program_documents, generated_at)}
    for document in program_documents:
        documents[document["json"]] = document
    return documents
