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
from .freshness import Freshness

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
td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; }
td.memo { white-space: normal; max-width: 34rem; }
td.wrap { white-space: normal; max-width: 26rem; }
tr.excluded td { color: #666; }
thead th { border-bottom: 1px solid #999; }
table.sortable thead th { cursor: pointer; user-select: none; }
table.sortable thead th:hover { text-decoration: underline; }
tr.detail > td { padding: .2em 0 .8em 1.4em; }
tr.prog:hover { background: #f4f4f4; }
button.tg { font: inherit; border: 0; background: none; cursor: pointer;
            padding: 0 .3em 0 0; color: #333; }
details { margin: .1em 0 .1em 1.1em; }
summary { cursor: pointer; }
summary h2 { display: inline; margin: 0; }
details > summary { margin: 1.2em 0 .3em; }
ul { list-style: none; padding-left: 1.1em; margin: .1em 0; }
.tot { border-top: 1px solid #999; font-weight: bold; }
.note { color: #555; max-width: 46rem; }
.stale { font-weight: bold; }
.sub td { color: #333; }
"""

# Sorting and row expansion. Vanilla, ~40 lines, no dependencies: the site is
# static files that must work from any host or a file:// path.
SCRIPT = """
(function () {
  function pairs(tb) {
    var out = [], rows = Array.prototype.slice.call(tb.rows);
    rows.forEach(function (r) {
      if (r.classList.contains('detail') && out.length) out[out.length - 1].push(r);
      else out.push([r]);
    });
    return out;
  }
  function value(row, i, type) {
    var cell = row.cells[i];
    if (!cell) return type === 'num' ? null : '';
    var raw = cell.dataset.v;
    if (type === 'num') {
      if (raw === undefined || raw === '') return null;
      var n = parseFloat(raw);
      return isNaN(n) ? null : n;
    }
    return (raw !== undefined ? raw : cell.textContent).trim().toLowerCase();
  }
  function sort(table, th) {
    var head = Array.prototype.slice.call(th.parentNode.cells);
    var i = head.indexOf(th), type = th.dataset.type || 'text';
    var dir = th.dataset.dir === 'asc' ? 'desc' : 'asc';
    head.forEach(function (h) {
      delete h.dataset.dir;
      h.textContent = h.textContent.replace(/ [\u25b2\u25bc]$/, '');
    });
    th.dataset.dir = dir;
    th.textContent = th.textContent + (dir === 'asc' ? ' \u25b2' : ' \u25bc');
    var tb = table.tBodies[0], group = pairs(tb);
    group.sort(function (a, b) {
      var x = value(a[0], i, type), y = value(b[0], i, type);
      if (x === null && y === null) return 0;
      if (x === null) return 1;   // blanks always last
      if (y === null) return -1;
      var c = x < y ? -1 : x > y ? 1 : 0;
      return dir === 'asc' ? c : -c;
    });
    group.forEach(function (rows) { rows.forEach(function (r) { tb.appendChild(r); }); });
  }
  function toggle(btn) {
    var row = btn.closest('tr');
    if (!row) return;
    var detail = row.nextElementSibling;
    if (!detail || !detail.classList.contains('detail')) return;
    var open = detail.hasAttribute('hidden');
    if (open) detail.removeAttribute('hidden'); else detail.setAttribute('hidden', '');
    btn.textContent = open ? '\u25be' : '\u25b8';
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  }
  document.addEventListener('click', function (e) {
    var th = e.target.closest('table.sortable thead th');
    if (th) { sort(th.closest('table'), th); return; }
    var btn = e.target.closest('button.tg');
    if (btn) { toggle(btn); }
  });
})();
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


def _page(title: str, body: str, script: bool = False) -> str:
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{esc(title)}</title>\n<style>{STYLE}</style>\n</head>\n<body>\n"
        f"{body}\n"
        + (f"<script>{SCRIPT}</script>\n" if script else "")
        + "</body>\n</html>\n"
    )


def ago(then: Optional[datetime], now: datetime) -> str:
    """'12 days ago' / '18 hours ago' / '4 minutes ago' / 'just now'."""
    if then is None:
        return ""
    seconds = (now - then).total_seconds()
    if seconds < 0:
        return "just now"
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60)):
        count = int(seconds // size)
        if count >= 1:
            return f"{count} {unit}{'s' if count != 1 else ''} ago"
    return "just now"


def fmt_stamp(value: Optional[datetime]) -> str:
    if value is None:
        return "unknown"
    return value.strftime("%Y-%m-%d %H:%M UTC")


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

def _freshness_table(data: SiteData, generated_at: datetime) -> str:
    """
    How old the numbers are, in two clocks: when we last pulled HCB, and when
    the spend was last recalculated from that pull. Either can be the reason a
    number looks wrong, so both are at the top of the page rather than buried.
    """
    fresh = data.freshness or Freshness()
    rows = [
        ("HCB data pulled", fresh.hcb_pulled_at,
         "last successful run of the HCB → warehouse mirror"),
        ("Newest HCB record held", fresh.hcb_data_through,
         "most recent HCB row in the warehouse"),
        ("Spend recalculated", fresh.recalculated_at,
         "last rebuild of the true-spend models"),
        ("This page built", generated_at, ""),
    ]
    body = "".join(
        f"<tr><td>{esc(label)}</td><td>{esc(fmt_stamp(value))}</td>"
        f"<td>{esc(ago(value, generated_at))}</td>"
        f'<td class="note">{esc(note)}</td></tr>'
        for label, value, note in rows
    )
    return f"<table>{body}</table>"


def _program_row(program: Dict[str, Any], page: str, tree: List[Dict[str, Any]]) -> str:
    """A program's table row, plus the hidden row holding its HCB org tree."""
    org_count = int(program["org_count"] or 0)
    name = esc(program["program_name"])
    cells = [
        '<td><button class="tg" aria-expanded="false">\u25b8</button> '
        + f'<a href="{esc(page)}">{name}</a></td>',
        f'<td class="n" data-v="{org_count}">{org_count:,}</td>',
        _num_cell(program["weighted_projects"], "{:,.1f}"),
        _num_cell(program["true_spend_dollars"], money),
        _num_cell(program["cost_per_weighted_hour"], money),
        _num_cell(program["balance_dollars"], money),
        f'<td>{_hcb_org_link(program["root_slug"])}</td>',
    ]
    detail = (
        '<tr class="detail" hidden><td colspan="7">'
        + _org_subtable(tree, page)
        + "</td></tr>"
    )
    return f'<tr class="prog">' + "".join(cells) + "</tr>" + detail


def _num_cell(value: Any, fmt) -> str:
    """Right-aligned numeric cell carrying its raw value for sorting."""
    if value is None:
        return '<td class="n"></td>'
    text = fmt(value) if callable(fmt) else fmt.format(float(value))
    return f'<td class="n" data-v="{_dec(value)}">{esc(text)}</td>'


def _org_subtable(orgs: List[Dict[str, Any]], page: str) -> str:
    """
    The program's HCB org tree, indented by depth, numbers still aligned.
    Ordered by walking the tree so parents sit above their children.
    """
    if not orgs:
        return '<span class="note">No orgs.</span>'
    children = _tree_children(orgs)
    ordered: List[Dict[str, Any]] = []

    def walk(parent, depth):
        for org in children.get(parent, []):
            ordered.append((org, depth))
            walk(org["event_id"], depth + 1)

    walk(None, 0)
    rows = "".join(
        f'<tr class="sub">'
        f'<td>{"&nbsp;" * 3 * depth}{esc(org["org_name"] or org["org_slug"])} '
        f'<span class="note">({esc(org["org_slug"])})</span></td>'
        f'<td class="n">{money(org["true_spend_dollars"])}</td>'
        f'<td class="n">{money(org["balance_dollars"])}</td>'
        f'<td class="n">{int(org["transaction_count"] or 0):,}</td>'
        f'<td>{_hcb_org_link(org["org_slug"])}</td>'
        f'<td>{_link(page + "#org-" + str(org["org_slug"]), "transactions")}</td>'
        "</tr>"
        for org, depth in ordered
    )
    return (
        '<table><thead><tr><th>HCB org</th><th class="n">True spend</th>'
        '<th class="n">Balance</th><th class="n">Txns</th><th></th><th></th>'
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )


def _programs_table(
    programs: List[Dict[str, Any]], data: SiteData, table_id: str
) -> str:
    headers = [
        ("Program", "text"),
        ("Orgs", "num"),
        ("Weighted projects", "num"),
        ("True spend", "num"),
        ("$ / weighted hour", "num"),
        ("Balance", "num"),
        ("HCB", "text"),
    ]
    head = "".join(
        f'<th class="n" data-type="{t}">{esc(h)}</th>' if t == "num"
        else f'<th data-type="{t}">{esc(h)}</th>'
        for h, t in headers
    )
    body = "".join(
        _program_row(
            program,
            f"programs/{page_slug(program['root_slug'], program['root_event_id'])}.html",
            data.orgs_by_program.get(program["root_slug"], []),
        )
        for program in programs
    )
    return (
        f'<table class="sortable" id="{esc(table_id)}"><thead><tr>{head}</tr></thead>'
        f"<tbody>{body}</tbody></table>"
    )


def render_index(data: SiteData, generated_at: datetime) -> str:
    # HQ marketing is tracked in the same models so its spend can be netted out
    # of the programs it funds, but it is not a YSWS program and does not belong
    # in a table of them.
    ysws = [p for p in data.programs if p.get("is_ysws_program", True)]
    other = [p for p in data.programs if not p.get("is_ysws_program", True)]

    out = [
        "<h1>YSWS true spend</h1>",
        _freshness_table(data, generated_at),
        _section(
            f"YSWS Programs w/ Linked HCBs ({len(ysws):,})",
            _programs_table(ysws, data, "programs"),
            open_by_default=True,
        ),
    ]
    if other:
        out.append(
            _section(
                "Marketing (HQ, not a YSWS program)",
                '<p class="note">HQ marketing spend, tracked here because budget it '
                "sends into a program is netted out of that program's true spend so "
                "the two are not counted twice.</p>"
                + _programs_table(other, data, "non-programs"),
            )
        )
    out += [
        render_unlinked_programs_section(data),
        render_unmatched_orgs_section(data),
        f'<p class="note">{_link("data/programs.json", "programs.json")} '
        "has the same numbers as JSON.</p>",
    ]
    return _page("YSWS true spend", "\n".join(out), script=True)


# --- unmatched --------------------------------------------------------------

UNMATCHED_REASONS = {
    "parent_of_mapped_root": "Its sub-org is a linked program, but it is not linked itself "
                             "(the Unified YSWS DB link probably points one level too deep)",
    "funded_by_mapped_program": "A mapped program sent it money, but no program claims it",
    "funds_mapped_program": "It sent money into a mapped program while belonging to none",
}

GAP_TYPES = {
    "no_hcb_link": "No HCB link on the Unified YSWS DB record",
    "unparseable_hcb": "The hcb field is not an hcb.hackclub.com org URL",
    "org_not_found": "The linked slug matches no HCB org",
    "org_deleted": "The linked HCB org is deleted",
}


def _related(value: Any, limit: int = 4) -> str:
    """Program lists can run to 200 names (the ysws umbrella); trim for reading."""
    names = [n.strip() for n in str(value or "").split(",") if n.strip()]
    if len(names) <= limit:
        return ", ".join(names)
    return ", ".join(names[:limit]) + f", +{len(names) - limit} more"


def _section(title: str, body: str, open_by_default: bool = False) -> str:
    """A collapsible top-level section. Native <details>, so it survives with JS off."""
    return (
        f"<details{' open' if open_by_default else ''}>"
        f"<summary><h2>{title}</h2></summary>\n{body}\n</details>"
    )


def render_unlinked_programs_section(data: SiteData) -> str:
    """YSWS programs whose Unified YSWS DB record has no usable HCB link."""
    rows = "".join(
        "<tr>"
        f'<td>{esc(g["program_name"])}</td>'
        f'<td>{esc(GAP_TYPES.get(g["gap_type"], g["gap_type"]))}</td>'
        f'<td class="wrap">{esc(g["hcb_field"] or "")}</td>'
        "</tr>"
        for g in data.unlinked_programs
    )
    body = (
        '<p class="note">No spend can be attributed to these until the link is '
        "fixed in the Unified YSWS DB.</p>"
        '<table class="sortable"><thead><tr>'
        '<th data-type="text">Program</th><th data-type="text">Problem</th>'
        '<th data-type="text">hcb field</th>'
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )
    return _section(
        f"YSWS Programs w/ No Linked HCBs ({len(data.unlinked_programs):,})", body
    )


def render_unmatched_orgs_section(data: SiteData) -> str:
    """HCB orgs that touch a mapped program but belong to none."""
    rows = "".join(
        "<tr>"
        f'<td>{_link(o["hcb_url"], o["org_slug"])}</td>'
        f'<td>{esc(o["org_name"])}</td>'
        f'<td>{esc(UNMATCHED_REASONS.get(o["reason"], o["reason"]))}</td>'
        f'<td>{esc(o["parent_slug"] or "")}</td>'
        f'{_num_cell(o["dollars_from_programs"], money)}'
        f'{_num_cell(o["dollars_to_programs"], money)}'
        f'{_num_cell(o["gross_outflow_dollars"], money)}'
        f'{_num_cell(o["balance_dollars"], money)}'
        f'<td class="wrap">{esc(_related(o["related_programs"]))}</td>'
        "</tr>"
        for o in data.unmatched_orgs
    )
    received = sum(
        (_dec(o["dollars_from_programs"]) for o in data.unmatched_orgs), Decimal(0)
    )
    body = (
        f'<p class="note">{money(received)} was sent from mapped programs into these '
        "orgs. Fix = a Unified YSWS DB link, or an HCB parent organization. "
        "Fiscal-host and HQ-operations orgs (hq, bank, fines, hq-usps-ops) are here "
        "too and are usually correct as-is.</p>"
        '<table class="sortable"><thead><tr>'
        '<th data-type="text">Org</th><th data-type="text">Name</th>'
        '<th data-type="text">Why it is here</th><th data-type="text">HCB parent</th>'
        '<th class="n" data-type="num">$ from programs</th>'
        '<th class="n" data-type="num">$ into programs</th>'
        '<th class="n" data-type="num">Its own outflow</th>'
        '<th class="n" data-type="num">Balance</th>'
        '<th data-type="text">Related programs</th>'
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )
    return _section(
        f"HCB orgs no program claims ({len(data.unmatched_orgs):,})", body
    )


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
        ("Grant cards funded (counted as spend above)",
         money(program["card_grants_funded_dollars"])),
        ("Still sitting on those cards", money(program["card_grants_remaining_dollars"])),
    ]
    if program.get("weighted_hours"):
        rows += [
            ("", ""),
            ("Weighted projects", f"{_dec(program['weighted_projects']):,.2f}"),
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

    # Which Airtable record(s) this program's projects and hours come from, and
    # how the HCB orgs were matched — the whole mapping in one line.
    if program.get("is_ysws_program", True):
        match_note = (
            "Mapped from this program's Unified YSWS DB record HCB link, plus every "
            "HCB sub-org beneath it."
        )
    else:
        match_note = (
            "Not a YSWS program: tracked separately (HQ marketing) so that "
            "marketing-funded program budget is not counted twice."
        )

    out = [
        f'<p>{_link("../index.html", "← all programs")}</p>',
        f"<h1>{esc(program['program_name'])}</h1>",
        f"<p>HCB: {hcb_link}"
        f" · {int(program['org_count'] or 0)} org(s) in tree"
        f" · outflows {fmt_date(program['first_outflow_date'])} to {fmt_date(program['last_outflow_date'])}"
        f" · {json_link}</p>",
        f'<p class="note">{esc(match_note)}</p>',
        "<h2>Totals</h2>",
        _summary_table(program),
        "<h2>Where the money went</h2>",
        _category_table(spend_txns),
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
- The bottom of `index.html` — HCB orgs no program claims, and Unified YSWS DB
  programs whose HCB link cannot be used, with the dollars at stake.
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
        "card_grants_active_face_dollars", "card_grants_remaining_dollars",
        "grant_card_count", "weighted_projects", "weighted_hours",
        "approved_project_count", "cost_per_weighted_hour", "is_ysws_program",
        "match_source",
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

    fresh = data.freshness or Freshness()
    files["data/unmatched.json"] = _dump(
        {
            "generated_at": generated_at.isoformat(timespec="seconds"),
            "unmatched_orgs": data.unmatched_orgs,
            "unlinked_programs": data.unlinked_programs,
        },
        indent=1,
    )
    files["data/programs.json"] = _dump(
        {
            "generated_at": generated_at.isoformat(timespec="seconds"),
            "hcb_pulled_at": fresh.hcb_pulled_at,
            "hcb_data_through": fresh.hcb_data_through,
            "recalculated_at": fresh.recalculated_at,
            "unmatched_org_count": len(data.unmatched_orgs),
            "unlinked_program_count": len(data.unlinked_programs),
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
