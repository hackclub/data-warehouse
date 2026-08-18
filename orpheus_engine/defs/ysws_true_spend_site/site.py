"""
Static-site renderer for YSWS true spend.

Pure functions: `render_site(data, generated_at)` returns a
{relative path -> file contents} dict, which the Dagster asset commits to
https://github.com/hackclub/ysws-true-spend. No Dagster, no network, no disk.

Deliberately plain: hand-written HTML, one small <style> block, native
<details> elements for the collapsible org tree (no JavaScript, so it works
from a file:// path or any static host).
"""

import html
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from .data import SiteData

REPO_URL = "https://github.com/hackclub/ysws-true-spend"
HCB_ORG_URL = "https://hcb.hackclub.com/{slug}"

# Zach's call (2026-08-18): publish the transaction detail as-is, matching HCB's
# public transparency pages, rather than stripping recipient names. Flip this to
# False to drop every person-identifying column from the site and the JSON.
INCLUDE_PERSONAL_FIELDS = True

_SAFE_SLUG = re.compile(r"^[A-Za-z0-9._-]+$")

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

STYLE = """
body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
       font-size: 13px; line-height: 1.5; margin: 1.5rem; max-width: 78rem; }
h1, h2, h3 { font-size: 1.1em; margin: 1.4em 0 .4em; }
table { border-collapse: collapse; margin: .4em 0 1em; }
th, td { padding: .1em .6em .1em 0; text-align: left; vertical-align: top;
         white-space: nowrap; }
td.n, th.n { text-align: right; }
td.memo { white-space: normal; max-width: 34rem; }
tr.excluded td { color: #666; }
thead th { border-bottom: 1px solid #999; }
details { margin: .1em 0 .1em 1.1em; }
summary { cursor: pointer; }
ul { list-style: none; padding-left: 1.1em; margin: .1em 0; }
.tot { border-top: 1px solid #999; font-weight: bold; }
.note { color: #555; max-width: 46rem; }
"""


# --- formatting helpers ------------------------------------------------------

def _dec(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def money(value: Any) -> str:
    """$1,234.56 / -$1,234.56 / $0.00"""
    d = _dec(value).quantize(Decimal("0.01"))
    sign = "-" if d < 0 else ""
    return f"{sign}${abs(d):,.2f}"


def money0(value: Any) -> str:
    """Whole dollars, for tree lines where cents are noise."""
    d = _dec(value).quantize(Decimal("1"))
    sign = "-" if d < 0 else ""
    return f"{sign}${abs(d):,.0f}"


def esc(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def fmt_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def page_slug(root_slug: str, root_event_id: Any) -> str:
    """File-safe page name. HCB slugs already are, but never trust them."""
    if root_slug and _SAFE_SLUG.match(root_slug):
        return root_slug
    return f"program-{root_event_id}"


def _link(href: str, text: str) -> str:
    return f'<a href="{esc(href)}">{esc(text)}</a>'


def _hcb_org_link(slug: str) -> str:
    return _link(HCB_ORG_URL.format(slug=slug), "hcb")


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value)


def _page(title: str, body: str) -> str:
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{esc(title)}</title>\n<style>{STYLE}</style>\n</head>\n<body>\n"
        f"{body}\n</body>\n</html>\n"
    )


# --- org tree ---------------------------------------------------------------

def _tree_children(orgs: List[Dict[str, Any]]) -> Dict[Optional[int], List[Dict[str, Any]]]:
    """
    Group a program's orgs by parent, so the page can render HCB's real
    sub-organization nesting. Any org whose parent is outside this program's
    tree (the root itself, or a manually attached fulfillment org) is hung off
    the top level.
    """
    ids = {o["event_id"] for o in orgs}
    children: Dict[Optional[int], List[Dict[str, Any]]] = {}
    for org in orgs:
        parent = org["parent_id"] if org["parent_id"] in ids else None
        children.setdefault(parent, []).append(org)
    for bucket in children.values():
        bucket.sort(key=lambda o: (-_dec(o["true_spend_dollars"]), (o["org_name"] or "").lower()))
    return children


def _org_line(org: Dict[str, Any], page: Optional[str]) -> str:
    bits = [
        f"<strong>{esc(org['org_name'] or org['org_slug'])}</strong>",
        f"({esc(org['org_slug'])})",
        f"spend {money0(org['true_spend_dollars'])}",
        f"· revenue {money0(org['external_revenue_dollars'])}",
        f"· balance {money0(org['balance_dollars'])}",
        f"· [{_hcb_org_link(org['org_slug'])}]",
    ]
    if page:
        anchor = "{}#org-{}".format(page, org["org_slug"])
        bits.append("· [" + _link(anchor, "transactions") + "]")
    return " ".join(bits)


def _render_org_nodes(
    children: Dict[Optional[int], List[Dict[str, Any]]],
    parent: Optional[int],
    page: Optional[str],
) -> str:
    nodes = children.get(parent, [])
    if not nodes:
        return ""
    out = ["<ul>"]
    for org in nodes:
        kids = children.get(org["event_id"], [])
        line = _org_line(org, page)
        if kids:
            out.append(
                f"<li><details><summary>{line} · {len(kids)} sub-org"
                f"{'s' if len(kids) != 1 else ''}</summary>"
                f"{_render_org_nodes(children, org['event_id'], page)}</details></li>"
            )
        else:
            out.append(f"<li>{line}</li>")
    out.append("</ul>")
    return "".join(out)


def render_org_tree(orgs: List[Dict[str, Any]], page: Optional[str] = None) -> str:
    children = _tree_children(orgs)
    return _render_org_nodes(children, None, page)


# --- index ------------------------------------------------------------------

def _program_summary_line(program: Dict[str, Any], page: str) -> str:
    return " ".join(
        [
            f"<strong>{esc(program['program_name'])}</strong>",
            f"— spend {money0(program['true_spend_dollars'])}",
            f"· revenue {money0(program['external_revenue_dollars'])}",
            f"· balance {money0(program['balance_dollars'])}",
            f"· {int(program['org_count'] or 0)} org{'s' if int(program['org_count'] or 0) != 1 else ''}",
        ]
    )


def render_index(data: SiteData, generated_at: datetime) -> str:
    programs = data.programs
    total_spend = sum((_dec(p["true_spend_dollars"]) for p in programs), Decimal(0))
    total_revenue = sum((_dec(p["external_revenue_dollars"]) for p in programs), Decimal(0))
    total_balance = sum((_dec(p["balance_dollars"]) for p in programs), Decimal(0))
    total_stated = sum((_dec(p["stated_outflow_dollars"]) for p in programs), Decimal(0))

    out = [
        "<h1>YSWS true spend</h1>",
        '<p class="note">What each YSWS program actually spent. HCB reports a program\'s '
        "spend as money that left its account, which counts transfers to reviewer "
        "budget orgs, author funds and the fiscal host as spend. This site walks each "
        "program's real HCB sub-organization tree, classifies every outflow, and counts "
        "only the dollars that left for the outside world. "
        f'See {_link("methodology.html", "methodology")} for the category rules.</p>',
        "<table>",
        f'<tr><td>Programs</td><td class="n">{len(programs)}</td></tr>',
        f'<tr><td>Total revenue (in from outside each tree)</td><td class="n">{money(total_revenue)}</td></tr>',
        f'<tr><td>Total true spend</td><td class="n">{money(total_spend)}</td></tr>',
        f'<tr><td>Total balance still held</td><td class="n">{money(total_balance)}</td></tr>',
        f'<tr><td>Spend as HCB states it (gross outflow)</td><td class="n">{money(total_stated)}</td></tr>',
        "</table>",
        f'<p class="note">Generated {esc(generated_at.strftime("%Y-%m-%d %H:%M UTC"))} '
        f'from the Hack Club data warehouse. Source: {_link(REPO_URL, "this repo")}.</p>',
        "<h2>Programs</h2>",
        '<p class="note">Click a program to expand its HCB org tree; click its name '
        "for every transaction counted.</p>",
    ]

    for program in programs:
        page = f"programs/{page_slug(program['root_slug'], program['root_event_id'])}.html"
        tree = data.orgs_by_program.get(program["root_slug"], [])
        out.append(
            "<details><summary>"
            + _program_summary_line(program, page)
            + "</summary>"
            + "<p>"
            + f"[{_link(page, 'all transactions')}] "
            + f"[{_hcb_org_link(program['root_slug'])}]"
            + (" · marketing" if program["bucket"] == "marketing" else "")
            + "</p>"
            + render_org_tree(tree, page)
            + "</details>"
        )

    out.append(f'<p class="note">{_link("data/programs.json", "programs.json")} '
               "has the same numbers as JSON.</p>")
    return _page("YSWS true spend", "\n".join(out))


# --- program page -----------------------------------------------------------

def _summary_table(program: Dict[str, Any]) -> str:
    rows = [
        ("Revenue (in from outside the tree)", money(program["external_revenue_dollars"])),
        ("True spend (A + C + offsets)", money(program["true_spend_dollars"])),
        ("Balance still held", money(program["balance_dollars"])),
        ("", ""),
        ("A — spent on the event", money(program["spent_on_event_dollars"])),
        ("C — internal cost", money(program["internal_cost_dollars"])),
        ("M — funded by marketing (offset)", money(-_dec(program["funded_by_marketing_dollars"]))),
        ("B — into author/reviewer funds (not spend)", money(program["author_fund_dollars"])),
        ("D — returned to HQ (not spend)", money(program["returned_to_hq_dollars"])),
        ("X — other internal transfer (not spend)", money(program["other_internal_dollars"])),
        ("I — intra-tree transfer (netted)", money(program["intra_tree_dollars"])),
        ("Gross outflow (all of the above)", money(program["gross_outflow_dollars"])),
        ("", ""),
        ("Spend as HCB states it", money(program["stated_outflow_dollars"])),
        (
            "HCB overstates by",
            f"{program['stated_overstatement_pct']}%"
            if program["stated_overstatement_pct"] is not None
            else "n/a",
        ),
        ("Transfers inside the tree, incl. grant funding (not revenue)", money(program["intra_tree_revenue_dollars"])),
        ("Card grants funded", money(program["card_grants_funded_dollars"])),
        ("Card grants unspent", money(program["card_grants_unspent_dollars"])),
    ]
    if program.get("weighted_hours"):
        rows += [
            ("", ""),
            ("Weighted hours shipped", f"{_dec(program['weighted_hours']):,.0f}"),
            ("Approved projects", f"{int(program['approved_project_count'] or 0):,}"),
            ("True spend per weighted hour", money(program["cost_per_weighted_hour"])),
        ]
    body = "".join(
        '<tr><td colspan="2"></td></tr>'
        if not label
        else f'<tr><td>{esc(label)}</td><td class="n">{value}</td></tr>'
        for label, value in rows
    )
    return f"<table>{body}</table>"


def _category_table(txns: List[Dict[str, Any]]) -> str:
    totals: Dict[str, Decimal] = {}
    counts: Dict[str, int] = {}
    for txn in txns:
        cat = txn["spend_category"] or "?"
        totals[cat] = totals.get(cat, Decimal(0)) + _dec(txn["outflow_dollars"])
        counts[cat] = counts.get(cat, 0) + 1
    order = [c for c in CATEGORY_ORDER if c in totals] + [
        c for c in sorted(totals) if c not in CATEGORY_ORDER
    ]
    rows = "".join(
        f"<tr><td>{esc(CATEGORY_LABELS.get(cat, cat))}</td>"
        f'<td class="n">{counts[cat]:,}</td>'
        f'<td class="n">{money(totals[cat])}</td>'
        f'<td>{"counted" if cat in TRUE_SPEND_CATEGORIES else "excluded"}</td></tr>'
        for cat in order
    )
    true_total = sum(
        (totals[c] for c in totals if c in TRUE_SPEND_CATEGORIES), Decimal(0)
    )
    return (
        '<table><thead><tr><th>Category</th><th class="n">Txns</th>'
        f'<th class="n">Amount</th><th>Counted?</th></tr></thead><tbody>{rows}'
        f'<tr class="tot"><td>True spend</td><td class="n">'
        f'{sum(counts[c] for c in counts if c in TRUE_SPEND_CATEGORIES):,}</td>'
        f'<td class="n">{money(true_total)}</td><td></td></tr>'
        "</tbody></table>"
    )


def _spend_rows(txns: Iterable[Dict[str, Any]]) -> str:
    out = []
    for txn in txns:
        counted = bool(txn["is_true_spend"])
        cells = [
            f"<td>{fmt_date(txn['transaction_date'])}</td>",
            f"<td>{esc(txn['org_slug'])}</td>",
            f"<td>{esc(txn['spend_category'])}</td>",
            f"<td>{esc(txn['spend_bucket'])}</td>",
            f"<td>{esc(txn['transaction_type'])}</td>",
            f"<td class=\"memo\">{esc(txn['description'])}</td>",
        ]
        if INCLUDE_PERSONAL_FIELDS:
            cells.append(f"<td>{esc(txn['counterparty'])}</td>")
            cells.append(f"<td>{esc(txn['initiated_by_name'])}</td>")
        cells.append(f'<td class="n">{money(txn["outflow_dollars"])}</td>')
        cells.append(f"<td>{'yes' if counted else 'no'}</td>")
        cells.append(
            f'<td>{_link(txn["hcb_url"], txn["hcb_code"]) if txn["hcb_url"] else esc(txn["hcb_code"])}</td>'
        )
        klass = "" if counted else ' class="excluded"'
        out.append(f"<tr{klass}>" + "".join(cells) + "</tr>")
    return "".join(out)


def _spend_table(txns: List[Dict[str, Any]]) -> str:
    head = ["Date", "Org", "Cat", "Bucket", "Type", "Description"]
    if INCLUDE_PERSONAL_FIELDS:
        head += ["Counterparty", "Initiated by"]
    head += ["Amount", "Counted", "HCB"]
    header = "".join(
        f'<th class="n">{esc(h)}</th>' if h == "Amount" else f"<th>{esc(h)}</th>"
        for h in head
    )
    return (
        f"<table><thead><tr>{header}</tr></thead><tbody>{_spend_rows(txns)}</tbody></table>"
    )


def _revenue_table(txns: List[Dict[str, Any]]) -> str:
    head = ["Date", "Org", "Type", "Source", "Description", "Amount", "HCB"]
    header = "".join(
        f'<th class="n">{esc(h)}</th>' if h == "Amount" else f"<th>{esc(h)}</th>"
        for h in head
    )
    rows = "".join(
        "<tr>"
        f"<td>{fmt_date(txn['transaction_date'])}</td>"
        f"<td>{esc(txn['org_slug'])}</td>"
        f"<td>{esc(txn['transaction_type'])}</td>"
        f"<td>{esc(txn['source'])}</td>"
        f"<td class=\"memo\">{esc(txn['description'])}</td>"
        f'<td class="n">{money(txn["amount_dollars"])}</td>'
        f'<td>{_link(txn["hcb_url"], txn["hcb_code"]) if txn["hcb_url"] else esc(txn["hcb_code"])}</td>'
        "</tr>"
        for txn in txns
    )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>"


def _org_table(orgs: List[Dict[str, Any]]) -> str:
    rows = "".join(
        f'<tr id="org-{esc(o["org_slug"])}">'
        f"<td>{'&nbsp;' * 2 * int(o['depth'] or 0)}{esc(o['org_name'] or o['org_slug'])}</td>"
        f"<td>{_link(HCB_ORG_URL.format(slug=o['org_slug']), o['org_slug'])}</td>"
        f'<td class="n">{money(o["external_revenue_dollars"])}</td>'
        f'<td class="n">{money(o["true_spend_dollars"])}</td>'
        f'<td class="n">{money(o["balance_dollars"])}</td>'
        f'<td class="n">{int(o["transaction_count"] or 0):,}</td>'
        "</tr>"
        for o in sorted(
            orgs, key=lambda o: (int(o["depth"] or 0), -_dec(o["true_spend_dollars"]))
        )
    )
    return (
        '<table><thead><tr><th>Org</th><th>Slug</th><th class="n">Revenue</th>'
        '<th class="n">True spend</th><th class="n">Balance</th>'
        f'<th class="n">Outflow txns</th></tr></thead><tbody>{rows}</tbody></table>'
    )


def render_program_page(
    program: Dict[str, Any],
    orgs: List[Dict[str, Any]],
    spend_txns: List[Dict[str, Any]],
    revenue_txns: List[Dict[str, Any]],
    generated_at: datetime,
) -> str:
    slug = program["root_slug"]
    external_revenue = [t for t in revenue_txns if not t["is_intra_tree"]]
    intra_revenue = [t for t in revenue_txns if t["is_intra_tree"]]

    json_link = _link(
        "../data/programs/{}.json".format(page_slug(slug, program["root_event_id"])), "json"
    )
    hcb_link = _link(HCB_ORG_URL.format(slug=slug), "hcb.hackclub.com/" + str(slug))

    out = [
        f'<p>{_link("../index.html", "← all programs")}</p>',
        f"<h1>{esc(program['program_name'])}</h1>",
        f"<p>HCB: {hcb_link}"
        f" · {int(program['org_count'] or 0)} org(s) in tree"
        f" · outflows {fmt_date(program['first_outflow_date'])} to {fmt_date(program['last_outflow_date'])}"
        f" · {json_link}</p>",
        "<h2>Totals</h2>",
        _summary_table(program),
        "<h2>Where the money went</h2>",
        _category_table(spend_txns),
        f'<p class="note">{_link("../methodology.html", "How these categories are decided")}</p>',
        "<h2>HCB org tree</h2>",
        _org_table(orgs),
        "<details><summary>Same tree, nested</summary>",
        render_org_tree(orgs),
        "</details>",
        f"<h2>Spend transactions ({len(spend_txns):,})</h2>",
        '<p class="note">Every main-ledger outflow of every org in the tree, '
        "classified. Grey rows are not counted as this program's spend. Card-grant "
        "swipes are omitted: the grant is counted when the card is funded.</p>",
        _spend_table(spend_txns),
        f"<h2>Revenue transactions ({len(external_revenue):,})</h2>",
        '<p class="note">Money entering the tree from outside it — HQ funding, '
        "donations, refunds, transfers from other orgs.</p>",
        _revenue_table(external_revenue),
    ]
    if intra_revenue:
        out += [
            f"<details><summary>Inflows from inside this program's own tree "
            f"— sub-org transfers and card-grant funding "
            f"({len(intra_revenue):,}, not counted as revenue)</summary>",
            _revenue_table(intra_revenue),
            "</details>",
        ]
    # Deliberately no generated-at stamp on program pages: this repo is
    # rewritten on every warehouse refresh, and a timestamp in all 245 pages
    # would create a fresh blob for every program on every run even when its
    # numbers did not move. index.html carries the stamp for the whole site.
    return _page(f"{program['program_name']} — YSWS true spend", "\n".join(out))


# --- methodology ------------------------------------------------------------

def render_methodology() -> str:
    categories = "".join(
        f"<tr><td>{esc(CATEGORY_LABELS[c])}</td>"
        f'<td>{"counted as spend" if c in TRUE_SPEND_CATEGORIES else "not spend"}</td></tr>'
        for c in CATEGORY_ORDER
    )
    body = f"""
<p>{_link("index.html", "← all programs")}</p>
<h1>Methodology</h1>
<p class="note">HCB tells you what left an organization's bank account. For a YSWS
program that is not the same as what the program spent: a program routes money to
reviewer budget orgs, to authors' personal funds, to its own sub-organizations and
back to the fiscal host. Counting those as spend overstates per-program cost, and
double counts when you sum programs.</p>

<h2>Program trees</h2>
<p class="note">Each program is anchored to one canonical HCB organization, taken from
the YSWS programs Airtable record's HCB link. Its tree is every descendant
organization found through HCB's real sub-organization relationship, stopping at
another program's root and at personal author/reviewer pots. Revenue and spend are
summed over the whole tree, so a program with 250 city sub-organizations reports one
number.</p>

<h2>Spend categories</h2>
<p class="note">Every main-ledger outflow of every org in a tree lands in exactly one
category, and the categories add up to gross outflow.</p>
<table><thead><tr><th>Category</th><th>Treatment</th></tr></thead><tbody>{categories}</tbody></table>
<p class="note"><strong>True spend = A + C</strong>, plus negative M offsets so that a
program's marketing-funded budget is not counted twice when program and marketing
totals are added together. Card-grant funding counts the moment the card is funded,
including money still sitting unspent on the card; the individual card swipes are
therefore excluded to avoid double counting.</p>

<h2>Revenue</h2>
<p class="note">Revenue is every inflow to any org in the tree whose source is outside
that tree — HQ funding, donations, refunds, transfers from other programs. Inflows whose
source is inside the same tree (a sub-org transfer, or an organization funding its own
grant cards) are listed separately and never counted, for the same reason intra-tree
outflows are netted out of spend.</p>

<h2>Source</h2>
<p class="note">Generated from the Hack Club data warehouse
(<code>public_hcb_ysws_true_spend_analytics</code> dbt models, built from HCB's
database) by a Dagster asset in
{_link("https://github.com/hackclub/data-warehouse", "hackclub/data-warehouse")}, and
committed here.</p>
"""
    return _page("Methodology — YSWS true spend", body)


# --- README + JSON ----------------------------------------------------------

def render_readme(data: SiteData, generated_at: datetime) -> str:
    total_spend = sum((_dec(p["true_spend_dollars"]) for p in data.programs), Decimal(0))
    total_revenue = sum(
        (_dec(p["external_revenue_dollars"]) for p in data.programs), Decimal(0)
    )
    return f"""# ysws-true-spend

Static site showing the **true spend** of each Hack Club YSWS program: what the
program actually spent on the outside world, rather than everything that left its
HCB account (which includes transfers to reviewer budgets, author funds, its own
sub-organizations and the fiscal host).

- `index.html` — collapsible tree of every program and its HCB organizations, with
  revenue and true spend.
- `programs/<slug>.html` — one page per program: category breakdown, org tree, and
  every transaction counted (and every one deliberately not counted).
- `methodology.html` — the classification rules.
- `data/programs.json`, `data/programs/<slug>.json` — program totals and org
  trees as JSON (transaction-level detail is on the HTML pages).

{len(data.programs)} programs · revenue {money(total_revenue)} · true spend
{money(total_spend)} · generated {generated_at.strftime('%Y-%m-%d %H:%M UTC')}.

## Do not edit by hand

Everything in this repository is generated and force-refreshed by the
`ysws_true_spend_site` Dagster asset in
[hackclub/data-warehouse](https://github.com/hackclub/data-warehouse)
(`orpheus_engine/defs/ysws_true_spend_site/`). Edit the asset, not the output.
"""


def _program_json(program: Dict[str, Any], orgs: List[Dict[str, Any]]) -> Dict[str, Any]:
    keys = [
        "program_name", "bucket", "root_slug", "root_event_id", "org_count",
        "first_outflow_date", "last_outflow_date", "external_revenue_dollars",
        "intra_tree_revenue_dollars", "true_spend_dollars", "spent_on_event_dollars",
        "internal_cost_dollars", "author_fund_dollars", "returned_to_hq_dollars",
        "other_internal_dollars", "intra_tree_dollars", "gross_outflow_dollars",
        "stated_outflow_dollars", "stated_overstatement_pct",
        "funded_by_marketing_dollars", "balance_dollars", "card_grants_funded_dollars",
        "card_grants_unspent_dollars", "weighted_hours", "approved_project_count",
        "cost_per_weighted_hour",
    ]
    out = {k: program.get(k) for k in keys}
    out["hcb_url"] = HCB_ORG_URL.format(slug=program["root_slug"])
    out["orgs"] = [
        {
            "org_slug": o["org_slug"],
            "org_name": o["org_name"],
            "event_id": o["event_id"],
            "parent_id": o["parent_id"],
            "depth": o["depth"],
            "external_revenue_dollars": o["external_revenue_dollars"],
            "true_spend_dollars": o["true_spend_dollars"],
            "balance_dollars": o["balance_dollars"],
            "transaction_count": o["transaction_count"],
        }
        for o in orgs
    ]
    return out


def _dump(value: Any, indent: Optional[int] = None) -> str:
    return json.dumps(value, default=_json_default, indent=indent, sort_keys=False) + "\n"


def render_site(data: SiteData, generated_at: datetime) -> Dict[str, str]:
    """Build every file of the site, keyed by repo-relative path."""
    files: Dict[str, str] = {
        ".nojekyll": "",
        "README.md": render_readme(data, generated_at),
        "index.html": render_index(data, generated_at),
        "methodology.html": render_methodology(),
    }

    summaries = []
    for program in data.programs:
        slug = program["root_slug"]
        name = page_slug(slug, program["root_event_id"])
        orgs = data.orgs_by_program.get(slug, [])
        spend_txns = data.spend_by_program.get(slug, [])
        revenue_txns = data.revenue_by_program.get(slug, [])

        files[f"programs/{name}.html"] = render_program_page(
            program, orgs, spend_txns, revenue_txns, generated_at
        )

        detail = _program_json(program, orgs)
        # Transaction-level detail lives in the HTML page only: inlining ~77k
        # rows as JSON tripled the repository size, and this repo is rewritten
        # on every warehouse refresh.
        detail["spend_transaction_count"] = len(spend_txns)
        detail["revenue_transaction_count"] = len(
            [t for t in revenue_txns if not t["is_intra_tree"]]
        )
        detail["transactions_page"] = f"programs/{name}.html"
        files[f"data/programs/{name}.json"] = _dump(detail, indent=1)

        summary = {k: v for k, v in detail.items() if k != "orgs"}
        summary["page"] = f"programs/{name}.html"
        summary["org_count"] = program["org_count"]
        summaries.append(summary)

    files["data/programs.json"] = _dump(
        {
            "generated_at": generated_at.isoformat(timespec="seconds"),
            "program_count": len(summaries),
            "total_external_revenue_dollars": sum(
                (_dec(p["external_revenue_dollars"]) for p in data.programs), Decimal(0)
            ),
            "total_true_spend_dollars": sum(
                (_dec(p["true_spend_dollars"]) for p in data.programs), Decimal(0)
            ),
            "programs": summaries,
        },
        indent=1,
    )
    return files
