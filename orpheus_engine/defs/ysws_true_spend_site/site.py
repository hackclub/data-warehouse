"""
HTML for the YSWS true-spend site, rendered from the JSON documents.

The renderer takes documents.py output and nothing else -- no database rows, no
SiteData -- so every number on a page is a number in the published JSON by
construction. Adding a figure to a page means adding it to the document first.

Plain HTML: one <style> block, native <details> for the collapsible sections,
and ~60 lines of vanilla JS for table sorting and org-tree folding. No build
step, no framework; the output works from any static host or a file:// path.
"""

import html
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .data import SiteData
from .database import DUCKDB_FILENAME, build_duckdb
from .documents import build_documents

# Publication policy is enforced in documents.py (emails stripped, private orgs
# summarised) so the JSON and the HTML are redacted identically. This flag only
# controls whether the name columns are rendered at all.
#
# Measured 2026-08-18 against hcb.hackclub.com unauthenticated: every public
# transaction carries a user object with full_name, so names are within HCB's
# own disclosure; no email address appears anywhere on its public surface.
INCLUDE_PERSONAL_FIELDS = True

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
table.fixed { table-layout: fixed; width: 100%; max-width: 100%; }
table.fixed th, table.fixed td { white-space: normal; overflow-wrap: anywhere;
            padding: .15em .5em .15em 0; vertical-align: top; }
table.fixed td.n, table.fixed th.n { white-space: nowrap; }
tr.excluded td { color: #666; }
thead th { border-bottom: 1px solid #999; }
table.sortable thead th { cursor: pointer; user-select: none; }
table.sortable thead th:hover { text-decoration: underline; }
tr.detail > td { padding: .2em 0 .8em 1.4em; }
tr.prog:hover { background: #f4f4f4; }
button.tg, button.tgo { font: inherit; border: 0; background: none;
            cursor: pointer; padding: 0 .3em 0 0; color: #333; }
.tgo-spacer { display: inline-block; width: 1.1em; }
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
  // Org trees: collapsing a node hides its whole subtree. Rows are emitted in
  // depth-first order and carry their depth, so visibility is one linear pass:
  // skip everything deeper than a collapsed row until the depth comes back up.
  function refreshTree(table) {
    var hideDepth = null;
    Array.prototype.forEach.call(table.querySelectorAll('tr.sub'), function (row) {
      var depth = parseInt(row.dataset.depth, 10);
      if (hideDepth !== null && depth > hideDepth) { row.setAttribute('hidden', ''); return; }
      hideDepth = null;
      row.removeAttribute('hidden');
      if (row.dataset.collapsed === 'true') hideDepth = depth;
    });
  }
  // Relative ages are recomputed in the browser from each <time> element's
  // ISO timestamp. Baking them at build time made a page that had not rebuilt
  // in a week still claim it was built "just now" -- which is exactly when the
  // number matters. Text rendered server-side stays as the fallback if this
  // never runs.
  function ago(then, now) {
    var seconds = (now - then) / 1000;
    if (seconds < 0) return 'just now';
    var units = [['day', 86400], ['hour', 3600], ['minute', 60]];
    for (var i = 0; i < units.length; i++) {
      var count = Math.floor(seconds / units[i][1]);
      if (count >= 1) return count + ' ' + units[i][0] + (count !== 1 ? 's' : '') + ' ago';
    }
    return 'just now';
  }
  function refreshAges() {
    var now = new Date();
    Array.prototype.forEach.call(document.querySelectorAll('time[data-ago]'), function (el) {
      var then = new Date(el.getAttribute('datetime'));
      if (!isNaN(then.getTime())) el.textContent = ago(then, now);
    });
  }
  refreshAges();
  setInterval(refreshAges, 60000);
  document.addEventListener('click', function (e) {
    var th = e.target.closest('table.sortable thead th');
    if (th) { sort(th.closest('table'), th); return; }
    var org = e.target.closest('button.tgo');
    if (org) {
      var row = org.closest('tr');
      var collapsed = row.dataset.collapsed !== 'true';
      row.dataset.collapsed = collapsed ? 'true' : 'false';
      org.textContent = collapsed ? '\u25b8' : '\u25be';
      org.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      refreshTree(row.closest('table'));
      return;
    }
    var btn = e.target.closest('button.tg');
    if (btn) { toggle(btn); }
  });
})();
"""


# --- formatting helpers ------------------------------------------------------

def _dec(value: Any) -> Decimal:
    return Decimal(str(value if value is not None else 0))


def money(value: Any) -> str:
    """$1,234.56 / -$1,234.56 / blank when the figure does not exist."""
    if value is None:
        return ""
    d = _dec(value).quantize(Decimal("0.01"))
    return f"{'-' if d < 0 else ''}${abs(d):,.2f}"


def money0(value: Any) -> str:
    if value is None:
        return ""
    d = _dec(value).quantize(Decimal("1"))
    return f"{'-' if d < 0 else ''}${abs(d):,.0f}"


def esc(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def _parse(stamp: Optional[str]) -> Optional[datetime]:
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def ago(then: Optional[datetime], now: Optional[datetime]) -> str:
    """'12 days ago' / '2 hours ago' / 'just now'."""
    if then is None or now is None:
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
    return value.strftime("%Y-%m-%d %H:%M UTC") if value else "unknown"


def fmt_date(value: Optional[str]) -> str:
    return (value or "")[:10]


def _link(href: str, text: str) -> str:
    return f'<a href="{esc(href)}">{esc(text)}</a>'


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


def _section(title: str, body: str, open_by_default: bool = False) -> str:
    """A collapsible top-level section. Native <details>, so it survives JS off."""
    return (
        f"<details{' open' if open_by_default else ''}>"
        f"<summary><h2>{title}</h2></summary>\n{body}\n</details>"
    )


def _num_cell(value: Any, fmt=money) -> str:
    """Right-aligned numeric cell carrying its raw value for client-side sorting."""
    if value is None:
        return '<td class="n"></td>'
    return f'<td class="n" data-v="{_dec(value)}">{esc(fmt(value))}</td>'


# --- org tree ---------------------------------------------------------------

def _flatten(nodes: List[Dict[str, Any]], depth: int = 0) -> List[Any]:
    out = []
    for node in nodes:
        out.append((node, depth))
        out.extend(_flatten(node["children"], depth + 1))
    return out


def _org_tree_table(
    nodes: List[Dict[str, Any]],
    page: Optional[str] = None,
    show_revenue: bool = False,
    anchor_ids: bool = False,
) -> str:
    """
    A program's HCB org tree: depth-first, indented, numbers aligned, and
    collapsible at every level (Campfire is three layers deep and 256 orgs
    wide). Expanded by default; the script folds an entire subtree by hiding
    every row deeper than a collapsed one until the depth comes back up.
    """
    if not nodes:
        return '<span class="note">No orgs.</span>'
    rows = []
    for org, depth in _flatten(nodes):
        kids = len(org["children"])
        if kids:
            control = '<button class="tgo" aria-expanded="true">▾</button> '
            suffix = (f' <span class="note">({kids} sub-org'
                      f"{'s' if kids != 1 else ''})</span>")
        else:
            control = '<span class="tgo-spacer"></span> '
            suffix = ""
        cells = [
            f'<td>{"&nbsp;" * 3 * depth}{control}{esc(org["name"])} '
            f'<span class="note">({esc(org["slug"])})</span>{suffix}</td>'
        ]
        if show_revenue:
            cells.append(_num_cell(org["external_revenue_dollars"]).replace(' data-v', ' data-v'))
        cells += [
            f'<td class="n">{money(org["true_spend_dollars"])}</td>',
            f'<td class="n">{money(org["balance_dollars"])}</td>',
            f'<td class="n">{org["transaction_count"]:,}</td>',
            f'<td>{_link(org["hcb_url"], "hcb")}</td>',
        ]
        if page:
            cells.append(f'<td>{_link(page + "#org-" + org["slug"], "transactions")}</td>')
        row_id = f' id="org-{esc(org["slug"])}"' if anchor_ids else ""
        rows.append(
            f'<tr class="sub"{row_id} data-depth="{depth}" data-collapsed="false">'
            + "".join(cells) + "</tr>"
        )
    head = ["<th>HCB org</th>"]
    if show_revenue:
        head.append('<th class="n">Revenue</th>')
    head += ['<th class="n">True spend</th>', '<th class="n">Balance</th>',
             '<th class="n">Txns</th>', "<th></th>"]
    if page:
        head.append("<th></th>")
    return (f'<table><thead><tr>{"".join(head)}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody></table>')


# --- index ------------------------------------------------------------------

def _age_cell(stamp: Optional[str], built: Optional[datetime]) -> str:
    """
    An age the browser keeps current. The ISO timestamp rides in the datetime
    attribute; the server-rendered text is the age at build time, which is what
    a reader without JavaScript sees.
    """
    parsed = _parse(stamp)
    if parsed is None:
        return ""
    return (f'<time datetime="{esc(parsed.isoformat())}" data-ago>'
            f"{esc(ago(parsed, built))}</time>")


def _freshness_table(metadata: Dict[str, Any]) -> str:
    """
    How stale the numbers are, in two clocks: when HCB was last pulled and when
    the spend was last recalculated from that pull. Age leads; the wall-clock
    stamp is the follow-up.
    """
    built = _parse(metadata["page_built"])
    rows = [
        ("HCB data pulled", metadata["hcb_data_pulled"],
         "last successful run of the HCB → warehouse mirror"),
        ("Newest HCB record held", metadata["newest_hcb_record_held"],
         "most recent HCB row in the warehouse"),
        ("Spend recalculated", metadata["spend_recalculated"],
         "last rebuild of the true-spend models"),
        ("This page built", metadata["page_built"], ""),
    ]
    body = "".join(
        f"<tr><td>{esc(label)}</td>"
        f"<td>{_age_cell(stamp, built)}</td>"
        f'<td class="note">{esc(note)}</td>'
        f"<td>{esc(fmt_stamp(_parse(stamp)))}</td></tr>"
        for label, stamp, note in rows
    )
    return f"<table>{body}</table>"


def _program_row(summary: Dict[str, Any]) -> str:
    """A program's table row, plus the hidden row holding its HCB org tree."""
    cells = [
        '<td><button class="tg" aria-expanded="false">▸</button> '
        + f'<a href="{esc(summary["page"])}">{esc(summary["name"])}</a></td>',
        f'<td class="n" data-v="{summary["hcb_org_count"]}">{summary["hcb_org_count"]:,}</td>',
        _num_cell(summary["weighted_projects"], lambda v: f"{_dec(v):,.1f}"),
        _num_cell(summary["true_spend_dollars"]),
        _num_cell(summary["cost_per_weighted_hour"]),
        _num_cell(summary["balance_dollars"]),
        f'<td>{_link(summary["hcb_url"], "hcb")}</td>',
        f'<td>{_link(summary["json"], "json")}</td>',
    ]
    detail = ('<tr class="detail" hidden><td colspan="8">'
              + _org_tree_table(summary["orgs"], page=summary["page"])
              + "</td></tr>")
    return '<tr class="prog">' + "".join(cells) + "</tr>" + detail


def _programs_table(summaries: List[Dict[str, Any]], table_id: str) -> str:
    headers = [("Program", "text"), ("Orgs", "num"), ("Weighted projects", "num"),
               ("True spend", "num"), ("$ / weighted hour", "num"), ("Balance", "num"),
               ("HCB", "text"), ("JSON", "text")]
    head = "".join(
        f'<th class="n" data-type="{t}">{esc(h)}</th>' if t == "num"
        else f'<th data-type="{t}">{esc(h)}</th>'
        for h, t in headers
    )
    body = "".join(_program_row(s) for s in summaries)
    return (f'<table class="sortable" id="{esc(table_id)}"><thead><tr>{head}</tr></thead>'
            f"<tbody>{body}</tbody></table>")


def _unlinked_table(rows: List[Dict[str, Any]]) -> str:
    body = "".join(
        "<tr>"
        f'<td>{esc(r["name"])}</td>'
        f'<td>{esc(r["problem"])}</td>'
        f'<td class="wrap">{esc(r["hcb_field"] or "")}</td>'
        "</tr>"
        for r in rows
    )
    return ('<p class="note">No spend can be attributed to these until the link is '
            "fixed in the Unified YSWS DB.</p>"
            '<table class="sortable"><thead><tr>'
            '<th data-type="text">Program</th><th data-type="text">Problem</th>'
            '<th data-type="text">hcb field</th>'
            f"</tr></thead><tbody>{body}</tbody></table>")


def _related(names: List[str], limit: int = 2) -> str:
    """Program lists run to 200 names on the ysws umbrella; trim for reading."""
    if len(names) <= limit:
        return ", ".join(names)
    return ", ".join(names[:limit]) + f", +{len(names) - limit} more"


def _unmatched_table(rows: List[Dict[str, Any]]) -> str:
    body = "".join(
        "<tr>"
        f'<td class="wrap">{esc(r["name"])}<br>{_link(r["hcb_url"], r["slug"])}</td>'
        f'<td class="wrap">{esc(r["why"])}</td>'
        f'<td class="wrap">{esc(r["hcb_parent_slug"] or "")}</td>'
        f'{_num_cell(r["dollars_from_programs"], money0)}'
        f'{_num_cell(r["dollars_to_programs"], money0)}'
        f'{_num_cell(r["own_outflow_dollars"], money0)}'
        f'{_num_cell(r["balance_dollars"], money0)}'
        f'<td class="wrap">{esc(_related(r["related_programs"]))}</td>'
        "</tr>"
        for r in rows
    )
    return ('<table class="sortable fixed">'
            "<colgroup>"
            '<col style="width:22%"><col style="width:13%"><col style="width:11%">'
            '<col style="width:9%"><col style="width:9%"><col style="width:9%">'
            '<col style="width:9%"><col style="width:18%">'
            "</colgroup><thead><tr>"
            '<th data-type="text">Org</th><th data-type="text">Why</th>'
            '<th data-type="text">HCB parent</th>'
            '<th class="n" data-type="num">From programs</th>'
            '<th class="n" data-type="num">To programs</th>'
            '<th class="n" data-type="num">Outflow</th>'
            '<th class="n" data-type="num">Balance</th>'
            '<th data-type="text">Related programs</th>'
            f"</tr></thead><tbody>{body}</tbody></table>")


def render_index(index_document: Dict[str, Any]) -> str:
    linked = index_document["ysws_programs_with_linked_hcbs"]
    unlinked = index_document["ysws_programs_with_no_linked_hcbs"]
    marketing = index_document["ysws_marketing"]
    unmatched = index_document["hcb_orgs_no_program_claims"]

    out = [
        "<h1>YSWS true spend</h1>",
        _machine_readable_line(),
        _freshness_table(index_document["metadata"]),
        _section(f"YSWS Programs w/ Linked HCBs ({len(linked):,})",
                 _programs_table(linked, "programs"), open_by_default=True),
        _section(f"YSWS Programs w/ No Linked HCBs ({len(unlinked):,})",
                 _unlinked_table(unlinked)),
    ]
    if marketing:
        out.append(_section(
            "YSWS - Marketing",
            '<p class="note">HQ marketing spend. Not a YSWS program: it is tracked '
            "here because budget it sends into a program is netted out of that "
            "program's true spend so the two are not counted twice.</p>"
            + _programs_table(marketing, "non-programs"),
        ))
    out.append(_section(f"HCB orgs no program claims ({len(unmatched):,})",
                        _unmatched_table(unmatched)))
    return _page("YSWS true spend", "\n".join(out), script=True)


# --- program page -----------------------------------------------------------

def _summary_table(document: Dict[str, Any]) -> str:
    t = document["totals"]
    rows = [
        ("Revenue (in from outside the tree)", money(t["external_revenue_dollars"])),
        ("True spend (A + C + offsets)", money(t["true_spend_dollars"])),
        ("Balance still held", money(t["balance_dollars"])),
        ("", ""),
        ("A — spent on the event", money(t["spent_on_event_dollars"])),
        ("C — internal cost", money(t["internal_cost_dollars"])),
        ("M — funded by marketing (offset)",
         money(-_dec(t["funded_by_marketing_dollars"]))),
        ("B — into author/reviewer funds (not spend)", money(t["author_fund_dollars"])),
        ("D — returned to HQ (not spend)", money(t["returned_to_hq_dollars"])),
        ("X — other internal transfer (not spend)", money(t["other_internal_dollars"])),
        ("I — intra-tree transfer (netted)", money(t["intra_tree_dollars"])),
        ("Gross outflow (all of the above)", money(t["gross_outflow_dollars"])),
        ("", ""),
        ("Spend as HCB states it", money(t["stated_outflow_dollars"])),
        ("HCB overstates by",
         f'{t["stated_overstatement_pct"]}%' if t["stated_overstatement_pct"] is not None else "n/a"),
        ("Transfers inside the tree, incl. grant funding (not revenue)",
         money(t["intra_tree_revenue_dollars"])),
        ("Grant cards funded (counted as spend above)",
         money(t["card_grants_funded_dollars"])),
        ("Still sitting on those cards", money(t["card_grants_remaining_dollars"])),
    ]
    if document.get("weighted_hours"):
        rows += [
            ("", ""),
            ("Weighted projects", f'{_dec(document["weighted_projects"]):,.2f}'),
            ("Weighted hours shipped", f'{_dec(document["weighted_hours"]):,.0f}'),
            ("Approved projects", f'{int(document["approved_project_count"] or 0):,}'),
            ("True spend per weighted hour", money(document["cost_per_weighted_hour"])),
        ]
    body = "".join(
        '<tr><td colspan="2"></td></tr>' if not label
        else f'<tr><td>{esc(label)}</td><td class="n">{value}</td></tr>'
        for label, value in rows
    )
    return f"<table>{body}</table>"


def _category_table(breakdown: List[Dict[str, Any]]) -> str:
    rows = "".join(
        f'<tr><td>{esc(row["label"])}</td>'
        f'<td class="n">{row["transaction_count"]:,}</td>'
        f'<td class="n">{money(row["dollars"])}</td>'
        f'<td>{"counted" if row["counted_as_spend"] else "excluded"}</td></tr>'
        for row in breakdown
    )
    counted = [r for r in breakdown if r["counted_as_spend"]]
    total = sum((_dec(r["dollars"]) for r in counted), Decimal(0))
    return ('<table><thead><tr><th>Category</th><th class="n">Txns</th>'
            '<th class="n">Amount</th><th>Counted?</th></tr></thead><tbody>'
            f"{rows}"
            f'<tr class="tot"><td>True spend</td>'
            f'<td class="n">{sum(r["transaction_count"] for r in counted):,}</td>'
            f'<td class="n">{money(total)}</td><td></td></tr></tbody></table>')


def _withheld_note(document: Dict[str, Any], kind: str) -> str:
    """One line per org HCB keeps private, standing in for its hidden rows."""
    withheld = [w for w in document["withheld_orgs"] if w[f"{kind}_transaction_count"]]
    if not withheld:
        return ""
    label = "outflows" if kind == "spend" else "inflows"
    lines = "".join(
        f'<li>{esc(w["org_name"])} <span class="note">({esc(w["org_slug"])})</span>: '
        f'{w[f"{kind}_transaction_count"]:,} {label} totalling '
        f'{money(w[f"{kind}_dollars"])}</li>'
        for w in withheld
    )
    return ('<p class="note">Not listed below, because HCB does not publish them '
            "either — these orgs are not in transparency mode. Their dollars are "
            "still counted in every total on this page.</p>"
            f"<ul>{lines}</ul>")


def _spend_table(txns: List[Dict[str, Any]]) -> str:
    head = ["Date", "Org", "Cat", "Bucket", "Type", "Description"]
    if INCLUDE_PERSONAL_FIELDS:
        head += ["Counterparty", "Initiated by"]
    head += ["Amount", "Counted", "HCB"]
    header = "".join(
        f'<th class="n">{esc(h)}</th>' if h == "Amount" else f"<th>{esc(h)}</th>"
        for h in head
    )
    rows = []
    for txn in txns:
        cells = [
            f'<td>{fmt_date(txn["date"])}</td>',
            f'<td>{esc(txn["org_slug"])}</td>',
            f'<td>{esc(txn["category"])}</td>',
            f'<td>{esc(txn["bucket"])}</td>',
            f'<td>{esc(txn["type"])}</td>',
            f'<td class="memo">{esc(txn["description"])}</td>',
        ]
        if INCLUDE_PERSONAL_FIELDS:
            cells.append(f'<td>{esc(txn["counterparty"])}</td>')
            cells.append(f'<td>{esc(txn["initiated_by"])}</td>')
        cells += [
            f'<td class="n">{money(txn["amount_dollars"])}</td>',
            f'<td>{"yes" if txn["counted_as_spend"] else "no"}</td>',
            f'<td>{_link(txn["hcb_url"], txn["hcb_code"]) if txn["hcb_url"] else esc(txn["hcb_code"])}</td>',
        ]
        klass = "" if txn["counted_as_spend"] else ' class="excluded"'
        rows.append(f"<tr{klass}>" + "".join(cells) + "</tr>")
    return f'<table><thead><tr>{header}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


def _revenue_table(txns: List[Dict[str, Any]]) -> str:
    header = "".join(
        f'<th class="n">{esc(h)}</th>' if h == "Amount" else f"<th>{esc(h)}</th>"
        for h in ["Date", "Org", "Type", "Source", "Description", "Amount", "HCB"]
    )
    rows = "".join(
        "<tr>"
        f'<td>{fmt_date(txn["date"])}</td>'
        f'<td>{esc(txn["org_slug"])}</td>'
        f'<td>{esc(txn["type"])}</td>'
        f'<td>{esc(txn["source"])}</td>'
        f'<td class="memo">{esc(txn["description"])}</td>'
        f'<td class="n">{money(txn["amount_dollars"])}</td>'
        f'<td>{_link(txn["hcb_url"], txn["hcb_code"]) if txn["hcb_url"] else esc(txn["hcb_code"])}</td>'
        "</tr>"
        for txn in txns
    )
    return f'<table><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>'


def render_program_page(document: Dict[str, Any]) -> str:
    match_note = (
        "Mapped from this program's Unified YSWS DB record HCB link, plus every "
        "HCB sub-org beneath it."
        if document["is_ysws_program"] else
        "Not a YSWS program: tracked separately (HQ marketing) so that "
        "marketing-funded program budget is not counted twice."
    )
    out = [
        f'<p>{_link("../index.html", "← all programs")}</p>',
        f'<h1>{esc(document["name"])}</h1>',
        f'<p>HCB: {_link(document["hcb_url"], "hcb.hackclub.com/" + document["root_slug"])}'
        f' · {document["hcb_org_count"]} org(s) in tree'
        f' · outflows {fmt_date(document["first_outflow_date"])} to '
        f'{fmt_date(document["last_outflow_date"])}'
        f' · {_link("../" + document["json"], "json")}</p>',
        f'<p class="note">{esc(match_note)}</p>',
        "<h2>Totals</h2>",
        _summary_table(document),
        "<h2>Where the money went</h2>",
        _category_table(document["category_breakdown"]),
        "<h2>HCB org tree</h2>",
        _org_tree_table(document["orgs"], show_revenue=True, anchor_ids=True),
        f'<h2>Spend transactions ({len(document["spend_transactions"]):,})</h2>',
        '<p class="note">Every main-ledger outflow of every org in the tree, '
        "classified. Grey rows are not counted as this program's spend. Card-grant "
        "swipes are omitted: the grant is counted when the card is funded.</p>",
        _withheld_note(document, "spend"),
        _spend_table(document["spend_transactions"]),
        f'<h2>Revenue transactions ({len(document["revenue_transactions"]):,})</h2>',
        '<p class="note">Money entering the tree from outside it — HQ funding, '
        "donations, refunds, transfers from other orgs.</p>",
        _withheld_note(document, "revenue"),
        _revenue_table(document["revenue_transactions"]),
    ]
    if document["intra_tree_transactions"]:
        out += [
            "<details><summary>Inflows from inside this program's own tree "
            "— sub-org transfers and card-grant funding "
            f'({len(document["intra_tree_transactions"]):,}, not counted as revenue)</summary>',
            _revenue_table(document["intra_tree_transactions"]),
            "</details>",
        ]
    return _page(f'{document["name"]} — YSWS true spend', "\n".join(out), script=True)


# --- llms.txt, README, assembly ---------------------------------------------

def render_llms_txt(index_document: Dict[str, Any], example_slug: str) -> str:
    """
    Barebones map for machines, per the llms.txt convention: what this is, and
    where the JSON is. The JSON is self-describing, so this does not restate its
    fields -- a field list here would be one more thing to drift.
    """
    meta = index_document["metadata"]
    return f"""# YSWS true spend

What each Hack Club YSWS program actually spent, published from the Hack Club
data warehouse. HCB reports a program's spend as everything that left its
account, which counts transfers to reviewer budgets, author funds and the fiscal
host; this site classifies every outflow and counts only what left for the
outside world.

Every page is rendered from the JSON below, so the two never disagree. Static
files, no auth. Amounts are US dollars, dates ISO-8601, timestamps UTC.

## Data

/index.json
    Metadata (when HCB was last pulled, when spend was last recalculated) plus
    the four sections of the site: ysws_programs_with_linked_hcbs,
    ysws_programs_with_no_linked_hcbs, ysws_marketing, and
    hcb_orgs_no_program_claims. Each program carries its totals and its nested
    HCB org tree, and links to its own document.

/programs/{{program_name}}.json   e.g. /programs/{example_slug}.json
    One program: totals, category breakdown, HCB org tree, and every
    transaction counted -- the whole of its HTML page.

/{DUCKDB_FILENAME}
    The same data as a DuckDB database, for querying rather than walking the
    JSON. Tables: programs, program_orgs, spend_transactions,
    revenue_transactions, withheld_orgs, unmatched_orgs, unlinked_programs,
    metadata.

        duckdb {DUCKDB_FILENAME}
        SELECT name, true_spend_dollars FROM programs ORDER BY 2 DESC LIMIT 10;

## Mapping contract

A program owns the HCB organization its Unified YSWS DB record links to, plus
every HCB sub-organization beneath it. Nothing else is matched; the gaps are in
index.json under ysws_programs_with_no_linked_hcbs and
hcb_orgs_no_program_claims.

## Transaction detail

{meta["transaction_detail"]}

Site: https://github.com/hackclub/ysws-true-spend
Source: https://github.com/hackclub/data-warehouse (asset ysws_true_spend_site)
"""


def _machine_readable_line() -> str:
    return (f'<p>Machine-readable: {_link("llms.txt", "llms.txt")} · '
            f'{_link("index.json", "index.json")} · '
            f'{_link(DUCKDB_FILENAME, "duckdb")}</p>')


def render_readme(index_document: Dict[str, Any]) -> str:
    linked = index_document["ysws_programs_with_linked_hcbs"]
    spend = sum((_dec(p["true_spend_dollars"]) for p in linked), Decimal(0))
    return f"""# ysws-true-spend

Static site showing the **true spend** of each Hack Club YSWS program: what the
program actually spent on the outside world, rather than everything that left its
HCB account (which includes transfers to reviewer budgets, author funds, its own
sub-organizations and the fiscal host).

Built as JSON first, then rendered to HTML from that JSON, so the two cannot
disagree.

- `index.json` / `index.html` — metadata, programs with linked HCBs, programs
  without, marketing, and HCB orgs no program claims.
- `programs/<root_slug>.json` / `.html` — one program: totals, category
  breakdown, HCB org tree, and every transaction counted.
- `llms.txt` — the JSON layout and what the numbers mean.

{len(linked)} programs · true spend {money(spend)} · built
{index_document["metadata"]["page_built"]}.

## Do not edit by hand

Everything here is generated and force-refreshed by the `ysws_true_spend_site`
Dagster asset in
[hackclub/data-warehouse](https://github.com/hackclub/data-warehouse)
(`orpheus_engine/defs/ysws_true_spend_site/`). Edit the asset, not the output.
"""


def _dump(value: Any) -> str:
    return json.dumps(value, indent=1, sort_keys=False, default=str) + "\n"


def render_site(data: SiteData, generated_at: datetime) -> Dict[str, Any]:
    """
    Build every file of the site: JSON documents first, then the HTML rendered
    from those documents.
    """
    documents = build_documents(data, generated_at)
    index_document = documents["index.json"]
    program_documents = [d for path, d in documents.items() if path != "index.json"]

    files: Dict[str, str] = {".nojekyll": ""}
    for path, document in documents.items():
        files[path] = _dump(document)
    for document in program_documents:
        files[document["page"]] = render_program_page(document)

    files[DUCKDB_FILENAME] = build_duckdb(index_document, program_documents)

    example = program_documents[0]["root_slug"] if program_documents else "flavortown"
    files["llms.txt"] = render_llms_txt(index_document, example)
    files["index.html"] = render_index(index_document)
    files["README.md"] = render_readme(index_document)
    return files
