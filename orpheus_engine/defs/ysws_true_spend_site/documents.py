"""
The site's JSON documents — the single source the HTML is rendered from.

Pipeline: dbt models -> data.py (SQL rows) -> documents.py (JSON) -> site.py
(HTML). The HTML renderer sees nothing but these documents, so a page cannot
show a number the JSON lacks: adding a field to a page means adding it here,
and it lands in both surfaces at once.

Three documents, mirroring the three kinds of page:

  index.json                 metadata + the sections of the home page
  programs/<root_slug>.json  one program: totals, category breakdown, HCB org
                             tree, and every transaction listed on its page
  budgets/<slug>.json        one person's individual budget: totals, bucket
                             breakdown, and every transaction behind them

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

# A budget pot's outflows. Only the first two are the person's own spend: money
# sent back to an org is spent (or not) by that org, and its ledger counts it.
BUDGET_BUCKET_ORDER = [
    "external_spend", "card_grant_funding", "transfer_to_org", "internal_leg",
    "funding_received", "other_inflow",
]
BUDGET_BUCKET_LABELS = {
    "external_spend": "Spent on the outside world (grants, cards, transfers out)",
    "card_grant_funding": "Loaded onto this person's grant cards",
    "transfer_to_org": "Sent back to an HCB org (that org's ledger counts it)",
    "internal_leg": "Internal leg on the bank rails (not spend)",
    "funding_received": "Funding received from a program",
    "other_inflow": "Other money in (refunds, donations)",
}
PERSONAL_SPEND_BUCKETS = {"external_spend", "card_grant_funding"}

# Why a roster row has no budget attached. Mirrors GAP_TYPES for programs: the
# link is one hand-typed field, so it fails in the same handful of ways.
BUDGET_GAP_TYPES = {
    "no_budget_link": "No HCB Budget Fund on the YSWS Authors record",
    "unparseable_budget_link": "The HCB Budget Fund field is not an hcb.hackclub.com org URL",
    "org_not_found": "The linked slug matches no HCB org",
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


def private_org_slugs(orgs: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Slug -> name for every org HCB keeps out of transparency mode.

    This, not the withheld summary below, is what suppresses transaction rows:
    an org with nothing to summarise (no spend, no external revenue) is still
    private, and its intra-tree inflows must not leak through the gap.
    """
    return {
        o["org_slug"]: (o["org_name"] or o["org_slug"])
        for o in orgs
        if HIDE_NON_TRANSPARENT_ORG_DETAIL and not o.get("is_public", True)
    }


def _withheld(
    private: Dict[str, str],
    spend_txns: List[Dict[str, Any]],
    revenue_txns: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Orgs HCB keeps private, with the totals that stand in for their rows.

    Their dollars remain in every total; only the line-level detail is withheld,
    exactly as HCB withholds it. Orgs with no rows to stand in for are simply
    absent here; they are suppressed by private_org_slugs regardless.
    """
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

def _budget_transaction(txn: Dict[str, Any]) -> Dict[str, Any]:
    """
    One line of a pot's ledger. Outflows carry a positive amount and inflows a
    negative one on the ledger's own sign convention, so both are republished
    as the dollars that moved, with direction as its own field.
    """
    return {
        "date": _iso(txn["transaction_date"]),
        "direction": txn["flow_direction"],
        "bucket": txn["budget_bucket"],
        "bucket_label": BUDGET_BUCKET_LABELS.get(txn["budget_bucket"], txn["budget_bucket"]),
        "type": txn["transaction_type"],
        "description": _text(txn["description"]),
        "counterparty": _text(txn["counterparty"] if txn["flow_direction"] == "outflow"
                              else txn["source"]),
        "initiated_by": _text(txn["initiated_by_name"]),
        "merchant_category": txn["merchant_category"],
        "amount_dollars": _money(
            txn["outflow_dollars"] if txn["flow_direction"] == "outflow"
            else txn["amount_dollars"]
        ),
        "counted_as_personal_spend": bool(txn["is_personal_spend"]),
        "receipt_count": int(txn["receipt_count"] or 0),
        "hcb_code": txn["hcb_code"],
        "hcb_url": txn["hcb_url"],
    }


def _budget_bucket_breakdown(txns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    totals: Dict[str, Decimal] = {}
    counts: Dict[str, int] = {}
    for txn in txns:
        bucket = txn["budget_bucket"] or "?"
        amount = _dec(txn["outflow_dollars"] if txn["flow_direction"] == "outflow"
                      else txn["amount_dollars"])
        totals[bucket] = totals.get(bucket, Decimal(0)) + amount
        counts[bucket] = counts.get(bucket, 0) + 1
    order = [b for b in BUDGET_BUCKET_ORDER if b in totals] + [
        b for b in sorted(totals) if b not in BUDGET_BUCKET_ORDER
    ]
    return [
        {
            "bucket": bucket,
            "label": BUDGET_BUCKET_LABELS.get(bucket, bucket),
            "transaction_count": counts[bucket],
            "dollars": _money(totals[bucket]),
            "counted_as_personal_spend": bucket in PERSONAL_SPEND_BUCKETS,
        }
        for bucket in order
    ]


def build_budget_document(
    budget: Dict[str, Any], txns: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """One person's individual budget, with every transaction behind its total."""
    name = page_slug(budget["budget_slug"], budget["budget_event_id"])
    outflows = [t for t in txns if t["flow_direction"] == "outflow"]
    inflows = [t for t in txns if t["flow_direction"] != "outflow"]
    return {
        "budget_name": budget["budget_name"],
        "person_name": budget["person_name"],
        "slug": budget["budget_slug"],
        "hcb_url": budget["hcb_url"],
        "page": f"budgets/{name}.html",
        "json": f"budgets/{name}.json",
        "matched_by": budget["matched_by"],
        "has_person": bool(budget["has_person"]),
        "is_also_program_root": bool(budget["is_also_program_root"]),
        "also_program_name": budget["also_program_name"],
        "first_activity_date": _iso(budget["first_activity_date"]),
        "last_activity_date": _iso(budget["last_activity_date"]),
        "totals": {
            "personal_spend_dollars": _money(budget["personal_spend_dollars"]),
            "funding_received_dollars": _money(budget["funding_received_dollars"]),
            "transferred_to_orgs_dollars": _money(budget["transferred_to_orgs_dollars"]),
            "other_inflow_dollars": _money(budget["other_inflow_dollars"]),
            "balance_dollars": _money(budget["balance_dollars"]),
            "card_grants_funded_dollars": _money(budget["card_grants_funded_dollars"]),
            "card_grants_unspent_dollars": _money(budget["card_grants_unspent_dollars"]),
        },
        "bucket_breakdown": _budget_bucket_breakdown(txns),
        "spend_transactions": [_budget_transaction(t) for t in outflows],
        "funding_transactions": [_budget_transaction(t) for t in inflows],
    }


def budget_index_summary(document: Dict[str, Any]) -> Dict[str, Any]:
    """An individual budget as the home page lists it."""
    return {
        "budget_name": document["budget_name"],
        "person_name": document["person_name"],
        "slug": document["slug"],
        "hcb_url": document["hcb_url"],
        "page": document["page"],
        "json": document["json"],
        "personal_spend_dollars": document["totals"]["personal_spend_dollars"],
        "funding_received_dollars": document["totals"]["funding_received_dollars"],
        "transferred_to_orgs_dollars": document["totals"]["transferred_to_orgs_dollars"],
        "balance_dollars": document["totals"]["balance_dollars"],
        "transaction_count": (len(document["spend_transactions"])
                              + len(document["funding_transactions"])),
        "last_activity_date": document["last_activity_date"],
        "is_also_program_root": document["is_also_program_root"],
        "also_program_name": document["also_program_name"],
    }


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
    private_slugs = private_org_slugs(orgs)
    withheld = _withheld(private_slugs, spend_txns, external)

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
    budget_documents: List[Dict[str, Any]],
    generated_at: datetime,
) -> Dict[str, Any]:
    """The home page: metadata, then its sections, in page order."""
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
    budgets = [budget_index_summary(d) for d in budget_documents]
    # A pot with no roster link is a gap on both sides: nobody is named here,
    # and the person (if they have a roster row at all) shows up in the list
    # below with an empty field. Both lists are published so either end can be
    # fixed.
    budgets_without_person = [b for b, d in zip(budgets, budget_documents)
                              if not d["has_person"]]
    people_without_budget = [
        {
            "name": person["person_name"],
            "problem": BUDGET_GAP_TYPES.get(person["link_status"], person["link_status"]),
            "hcb_budget_field": _text(person["hcb_budget_field"]),
            "airtable_url": person["airtable_record_url"],
            "grants_attributed_dollars": _money(person["grants_attributed_dollars"]),
        }
        for person in data.budget_people
        if person["link_status"] != "linked"
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
        "ysws_individual_budgets": budgets,
        "ysws_individual_budgets_with_no_linked_person": budgets_without_person,
        "ysws_people_with_no_linked_individual_budget": people_without_budget,
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
    budget_documents = [
        build_budget_document(budget, data.budget_txns_by_slug.get(budget["budget_slug"], []))
        for budget in data.budgets
    ]
    documents = {
        "index.json": build_index_document(
            data, program_documents, budget_documents, generated_at
        )
    }
    for document in program_documents:
        documents[document["json"]] = document
    for document in budget_documents:
        documents[document["json"]] = document
    return documents
