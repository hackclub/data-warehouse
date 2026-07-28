#!/usr/bin/env python3
"""
ysws_true_expense.py — Compute the true EVENT EXPENSE of a YSWS program from the
HCB ledger, categorizing every dollar so it reconciles exactly.

THE MODEL
---------
The "stated spend" methodology (HCB API `(total_raised - balance)/100`, mirrored
by the org's gross main-ledger OUTFLOW) counts every outflow as spend. That's
wrong: money leaving a program org falls into distinct categories, and only some
of them are an expense *of this event*. Every outflow dollar is assigned to one:

  A  Spent on the event .................... EXPENSE   (grants to makers incl.
                                                        unspent grant cards,
                                                        hardware/prizes, cash,
                                                        vendors, contractors)
  C  Internal cost ......................... EXPENSE   (money to an HQ/service
                                                        org for postage,
                                                        fulfillment, warehouse,
                                                        printing, hosting; also
                                                        fines swept out — a real
                                                        cost the event bears)
  B  Into a YSWS author fund ............... not expense (reviewer/author budget
                                                        transfers -> future events)
  D  Returned to HQ (overfunding) .......... not expense (money pulled back to
                                                        the fiscal host: closing
                                                        accounts, unused funds,
                                                        overdisbursement reversals)
  X  Other internal ........................ not expense (round-trip washes,
                                                        transfers to sibling
                                                        programs)

  EVENT EXPENSE = A + C.

Distinguishing the three HQ-bound cases (B author funds / C internal payments /
D returned overfunding) is the whole point — they all leave to another HCB org
but only C is an expense.

CHILD-ORG TREES
---------------
Many programs route real spend through child HCB orgs (ysws-siege-framework-grants,
stardance-hardware, horizons-* houses, daydream/campfire city orgs). A single-org
view calls those transfers X and understates spend. This tool consolidates the
org TREE using HCB's actual sub-organization relationship (hcb.events.parent_id),
descended recursively — naming-independent, so it also catches sub-orgs that
don't share the parent's slug prefix (blueprint has 46, scrapyard 93).
Parent->child transfers are netted out (category I, neither expense nor
not-expense) and the children's own external outflows are classified normally.
Personal author/reviewer pots (ysws-resolution-<name>, ysws-budget-*) are never
joined even when they are sub-orgs — transfers into them stay category B.
Slug-prefix lookalikes that are NOT sub-orgs in HCB are reported; --children all
also joins those (plus use --include-orgs for related orgs with neither link,
e.g. som-sticker-shipments); --children none reproduces the single-org view.

Notes:
  - Unspent grant cards ARE an expense: the card-grant *funding* is a main-ledger
    outflow (category A) the moment the card is loaded, regardless of whether the
    maker ever spends it. Only cash still sitting in the org's main balance is
    "not yet an expense" (reported separately, never counted).
  - cost/hr uses weighted hours = SUM(ysws_weighted_project_contribution) * 10
    from approved_projects (works even while the ysws_programs sync is down).

USAGE
-----
  bin/ysws_true_expense.py <program-name-or-slug> [--ledger] [--json]
  bin/ysws_true_expense.py "Summer of Making" --include-orgs som-sticker-shipments
  bin/ysws_true_expense.py campfire                     # full sub-org tree (regions + cities)
  bin/ysws_true_expense.py siege --children none        # single-org view
  bin/ysws_true_expense.py fallout --hours-name "Fallout"
  bin/ysws_true_expense.py --list

Requires `psql` on PATH and WAREHOUSE_COOLIFY_URL in .env (read-only enforced).
"""
import argparse
import csv
import io
import json
import os
import subprocess
import sys

# ---------------------------------------------------------------------------
# Classification config — the only domain assumptions; tweak here.
# ---------------------------------------------------------------------------

# HQ / service orgs a program pays for a REAL cost (postage, fulfillment, etc.).
# Money here is an internal payment (category C = expense), not a return.
SERVICE_ORGS = {
    "hq-usps-ops",      # Theseus USPS Operating Account -> postage/shipping
    "printing-legion",  # PCB / sticker printing
    "sprig",            # hardware kits
    "nest",             # hosting
    "hackpad",          # hardware fulfillment
}

# The fiscal host. Money sent here is a RETURN of overfunding (category D)
# unless the memo names a real cost (then it's an internal payment, category C).
HOST_ORGS = {"hq", "bank", "hcb"}

FINES_ORGS = {"fines"}

# If a disbursement memo names one of these, the money bought a real thing/service
# -> internal payment (C = expense), even if it went to the fiscal host.
INTERNAL_PAYMENT_KEYWORDS = (
    "postage", "warehouse", "shipping", "fedex", "freight", "ups ", "dhl",
    "import", "fulfil", "invoice", "contractor", "reimburs", "sticker",
    "print", "label", "envelope", "server", "hosting", "stamp", "package",
)

# If a disbursement to the host names one of these (and no payment keyword),
# it's overfunding coming back -> returned to HQ (D = not an expense).
RETURN_KEYWORDS = (
    "clos", "return", "overfund", "overdisburs", "unused", "unspent",
    "sweep", "giving back", "repayment", "reversal",
)

# Program local chapters -> part of the event footprint (A = expense).
CHAPTER_PREFIXES = ("build-guild-",)

# ---- Category system --------------------------------------------------------
# code -> (label, is_expense). is_expense None = intra-tree, excluded from both
# sides of the expense split (it nets against a child org's classified spend).
CATEGORIES = {
    "A": ("A - Spent on the event", True),
    "C": ("C - Internal cost (HQ postage / fulfillment / services / fines)", True),
    "B": ("B - Into YSWS author funds (future events)", False),
    "D": ("D - Returned to HQ (overfunding)", False),
    "X": ("X - Other internal (washes / other programs)", False),
    "I": ("I - Intra-tree transfer (netted out, child spend counted directly)", None),
}

# Fine-grained bucket -> (label, category code). Buckets give --ledger detail;
# categories are the headline rollup the report is built around.
BUCKETS = {
    "grants":           ("Grants to makers (card-grant funding, incl. unspent cards)", "A"),
    "external_spend":   ("Direct external spend (card / ACH / wire / check)", "A"),
    "other_external":   ("Other external payments (Column ACH / bank / Wise)", "A"),
    "program_chapters": ("Program local chapters (e.g. build guilds)", "A"),
    "internal_payment": ("Internal payment for a real cost (HQ postage / services)", "C"),
    "author_fund":      ("Moved to YSWS author funds (future events)", "B"),
    "host_return":      ("Returned to HQ / other HQ funds (overfunding)", "D"),
    "fines":            ("Fines swept to central account (real cost to the event)", "C"),
    "wash_roundtrip":   ("Round-trip wash with another org", "X"),
    "inter_org":        ("Transfer to another program / org", "X"),
    "intra_tree":       ("Transfer to a child org of this program (netted)", "I"),
}


def bucket_cat(bucket):
    return BUCKETS[bucket][1]


# ---------------------------------------------------------------------------
# DB plumbing (psql + CSV, no driver dependency)
# ---------------------------------------------------------------------------

def _repo_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_url():
    url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    env_path = os.path.join(_repo_root(), ".env")
    if not url and os.path.exists(env_path):
        with open(env_path) as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("WAREHOUSE_COOLIFY_URL="):
                    url = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not url:
        sys.exit("Error: WAREHOUSE_COOLIFY_URL not set (env or .env).")
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}options=-c%20default_transaction_read_only%3Don"


_URL = None


def query(sql):
    global _URL
    if _URL is None:
        _URL = _load_url()
    wrapped = f"COPY ({sql.rstrip().rstrip(';')}) TO STDOUT WITH CSV HEADER"
    proc = subprocess.run(
        ["psql", _URL, "-v", "ON_ERROR_STOP=1", "-q", "-c", wrapped],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"SQL error:\n{proc.stderr.strip()}\n--- query ---\n{sql}")
    rows = list(csv.DictReader(io.StringIO(proc.stdout)))
    for r in rows:
        for k, v in r.items():
            if v == "":
                r[k] = None
    return rows


def f(x):
    return float(x) if x not in (None, "") else 0.0


def sql_str(s):
    return "'" + str(s).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Program resolution
# ---------------------------------------------------------------------------

SLUG_EXPR = r"regexp_replace(p.hcb,'^https://hcb\.hackclub\.com/([^/]+).*$','\1')"


def list_programs():
    return query(f"""
        SELECT p.name AS program_name, {SLUG_EXPR} AS slug, e.id AS event_id,
               o.total_outflow_cents, p.weighted_total, p.budget_per_hour
        FROM airtable_unified_ysws_projects_db.ysws_programs p
        JOIN hcb.events e ON e.slug = {SLUG_EXPR}
        JOIN public_hcb_analytics.orgs o ON o.event_id = e.id
        WHERE p.hcb ~ '^https://hcb\\.hackclub\\.com/'
    """)


def resolve_slug(token, programs):
    """Return a minimal org dict for a program name or bare HCB slug.

    Precedence: exact program name/slug > exact HCB slug > partial program
    match. Exact-HCB before partial matters for tree roots: "campfire" must hit
    the parent org `campfire`, not partial-match the "Campfire Flagship" program.
    """
    t = token.strip().lower()
    for p in programs:
        if (p["slug"] or "").lower() == t or (p["program_name"] or "").lower() == t:
            return p
    hcb = query(f"""
        SELECT e.slug, e.name AS program_name, e.id AS event_id, o.total_outflow_cents,
               NULL::text AS weighted_total, NULL::text AS budget_per_hour
        FROM hcb.events e JOIN public_hcb_analytics.orgs o ON o.event_id=e.id
        WHERE lower(e.slug)={sql_str(t)}
    """)
    if hcb:
        sys.stderr.write(f'note: using HCB org "{hcb[0]["slug"]}" (exact slug match; '
                         f'not an exact ysws_programs name).\n')
        return hcb[0]
    partial = [p for p in programs
               if t in (p["program_name"] or "").lower() or t in (p["slug"] or "").lower()]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        names = ", ".join(f'{p["program_name"]} ({p["slug"]})' for p in partial[:12])
        sys.exit(f'Ambiguous "{token}" -> {names}')
    sys.exit(f'No program matching "{token}". Try --list.')


def weighted_hours(program_name, hours_name, weighted_total_hint):
    """Weighted hours from approved_projects (authoritative); fall back to the
    ysws_programs hint. weighted hours == SUM(weighted_project_contribution)*10."""
    name = hours_name or program_name
    rows = query(f"""
        SELECT COALESCE(SUM(ap.ysws_weighted_project_contribution::numeric),0)*10 AS wh,
               COUNT(*) AS n
        FROM airtable_unified_ysws_projects_db.approved_projects ap
        JOIN airtable_unified_ysws_projects_db.approved_projects__ysws_name yn
          ON yn._dlt_parent_id = ap._dlt_id
        WHERE yn.value = {sql_str(name)}
    """)
    wh, n = f(rows[0]["wh"]), int(rows[0]["n"]) if rows else 0
    if wh > 0:
        return wh, n, f"approved_projects[{name}]"
    if weighted_total_hint:
        return f(weighted_total_hint) * 10, None, "ysws_programs.weighted_total"
    return 0.0, 0, "unavailable"


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def is_personal_pot(slug, name):
    """Narrow author-fund test for TREE MEMBERSHIP: personal reviewer/author pots
    only. A program-prefixed '<program>-something-fund' org is a program sub-fund
    (e.g. daydream-kindness-fund) and belongs IN the tree; '-fund' suffixes only
    mark personal pots for external-dest classification (is_author_fund)."""
    s = slug or ""
    if s.startswith(("ysws-budget-", "ysws-resolution-")) or s.endswith(("-earnings", "-jemoney")):
        return True
    n = (name or "").lower()
    return "budget" in n or "earnings" in n


def is_author_fund(dest_slug, dest_name, memo):
    ds = dest_slug or ""
    if ds.startswith(("ysws-budget-", "ysws-resolution-")) or ds.endswith(("-fund", "-earnings", "-jemoney")):
        return True
    dn = (dest_name or "").lower()
    if "budget" in dn or "earnings" in dn:
        return True
    if "personal budget transfer" in (memo or ""):
        return True
    return False


def classify(row, own_slug, tree_slugs, net_by_slug):
    """Assign one main-ledger OUTFLOW row to a fine-grained bucket."""
    ttype = row["transaction_type"]
    internal_flag = row.get("is_internal_transfer") in ("t", "true", True)

    if ttype != "disbursement":
        # Non-disbursement: external bank rails. Safety net — if HCB flags the
        # same hcb_code in >1 event, it's an internal leg, not spend.
        if internal_flag:
            return "inter_org"
        if ttype in ("card_transaction", "ach_transfer", "wire", "check"):
            return "external_spend"
        if (row.get("transaction_source_type") or "") in ("Wire", "WiseTransfer"):
            return "external_spend"
        return "other_external"  # raw Column ACH / bank debit

    dest = row["dest_org_slug"]
    dname = row.get("dest_org_name")
    memo = (row["disbursement_name"] or "").lower()

    if dest is None or dest == own_slug:
        return "grants"                              # self -> grant-card funding
    if dest in tree_slugs:
        return "intra_tree"                          # I -> netted; child spend counted
    if is_author_fund(dest, dname, memo):
        return "author_fund"                         # B
    if any(k in memo for k in INTERNAL_PAYMENT_KEYWORDS) or dest in SERVICE_ORGS:
        return "internal_payment"                    # C
    if dest.startswith(CHAPTER_PREFIXES):
        return "program_chapters"                    # A
    if dest in HOST_ORGS:
        return "host_return"                         # D (payment keywords handled above)
    if dest in FINES_ORGS:
        return "fines"                               # C (real cost to the event)
    out_c, in_c = net_by_slug.get(dest, (0.0, 0.0))
    if out_c > 0 and in_c >= 0.99 * out_c:
        return "wash_roundtrip"                      # X
    return "inter_org"                               # X


# ---------------------------------------------------------------------------
# Child-org tree discovery
# ---------------------------------------------------------------------------

def discover_tree(root, includes, children_mode, excludes=()):
    """Build the org tree: root + --include-orgs + HCB sub-organizations.

    Children come from the DB's actual sub-org relationship (hcb.events.parent_id),
    descended recursively. Personal author/reviewer pots (is_personal_pot, e.g.
    ysws-resolution-<name>) never join and are not descended into — transfers
    into them must stay category B. Slug-prefix lookalikes that are NOT sub-orgs
    are reported (and joined only under --children all) so nothing drops silently.
    Returns (members, child_notes).
    """
    members = {int(root["event_id"]): dict(root)}
    for o in includes:
        members[int(o["event_id"])] = dict(o)

    notes = {"joined": [], "prefix_not_suborg": [], "author_fund_kids": [], "excluded": []}
    if children_mode == "none":
        return list(members.values()), notes

    root_ids_csv = ",".join(str(i) for i in members)
    desc = query(f"""
        WITH RECURSIVE tree AS (
          SELECT e.id, e.slug, e.name, e.parent_id FROM hcb.events e
          WHERE e.id IN ({root_ids_csv})
          UNION
          SELECT e.id, e.slug, e.name, e.parent_id FROM hcb.events e
          JOIN tree t ON e.parent_id = t.id)
        SELECT t.id AS event_id, t.slug, t.name, t.parent_id, o.total_outflow_cents
        FROM tree t LEFT JOIN public_hcb_analytics.orgs o ON o.event_id = t.id
    """)
    by_parent = {}
    for d in desc:
        if d["parent_id"] is not None:
            by_parent.setdefault(int(d["parent_id"]), []).append(d)
    # BFS from the roots, pruning personal-pot subtrees.
    frontier = list(members)
    while frontier:
        nxt = []
        for pid in frontier:
            for c in by_parent.get(pid, []):
                eid = int(c["event_id"])
                if eid in members:
                    continue
                if c["slug"] in excludes:
                    notes["excluded"].append(c["slug"])   # a different program's
                    continue                              # fund — subtree detached
                if is_personal_pot(c["slug"], c.get("name")):
                    notes["author_fund_kids"].append(c["slug"])
                    continue
                members[eid] = c
                notes["joined"].append(c["slug"])
                nxt.append(eid)
        frontier = nxt

    # Diagnostic: slug-prefix lookalikes that are not sub-orgs in HCB.
    lookalikes = query(f"""
        SELECT e.id AS event_id, e.slug, e.name, o.total_outflow_cents
        FROM hcb.events e JOIN public_hcb_analytics.orgs o ON o.event_id = e.id
        WHERE e.slug LIKE {sql_str(root["slug"] + "-")} || '%'
    """)
    lookalikes = [c for c in lookalikes if int(c["event_id"]) not in members
                  and c["slug"] not in excludes
                  and not is_personal_pot(c["slug"], c.get("name"))]
    if children_mode == "all":
        for c in lookalikes:
            members[int(c["event_id"])] = c
            notes["joined"].append(c["slug"])
    else:
        notes["prefix_not_suborg"] = [
            (c["slug"], -f(c["total_outflow_cents"]) / 100.0) for c in lookalikes
            if f(c["total_outflow_cents"]) != 0
        ]
    return list(members.values()), notes


# ---------------------------------------------------------------------------
# Tree analysis
# ---------------------------------------------------------------------------

def analyze(prog, program_name, includes, hours_name, children_mode="auto", excludes=()):
    members, child_notes = discover_tree(prog, includes, children_mode, excludes)
    ids = [int(m["event_id"]) for m in members]
    ids_csv = ",".join(str(i) for i in ids)
    slug_by_id = {int(m["event_id"]): m["slug"] for m in members}
    tree_slugs = set(slug_by_id.values())

    # Wash detection: tree-wide out/in vs each EXTERNAL org.
    net_rows = query(f"""
        WITH fl AS (
          SELECT d.event_id AS other_id, SUM(d.amount) AS out_cents, 0 AS in_cents
          FROM hcb.disbursements d
          WHERE d.aasm_state='deposited' AND d.source_event_id IN ({ids_csv})
            AND d.event_id NOT IN ({ids_csv})
          GROUP BY 1
          UNION ALL
          SELECT d.source_event_id, 0, SUM(d.amount)
          FROM hcb.disbursements d
          WHERE d.aasm_state='deposited' AND d.event_id IN ({ids_csv})
            AND d.source_event_id NOT IN ({ids_csv})
          GROUP BY 1)
        SELECT ev.slug AS other_slug, SUM(fl.out_cents) AS out_cents, SUM(fl.in_cents) AS in_cents
        FROM fl LEFT JOIN hcb.events ev ON ev.id = fl.other_id
        GROUP BY 1
    """)
    net_by_slug = {r["other_slug"]: (f(r["out_cents"]) / 100.0, f(r["in_cents"]) / 100.0)
                   for r in net_rows if r["other_slug"]}

    ledger = query(f"""
        SELECT org_id, transaction_date, transaction_type, amount_dollars, display_memo,
               dest_org_slug, dest_org_name, disbursement_name,
               is_internal_transfer, transaction_source_type
        FROM public_hcb_analytics.ledger
        WHERE org_id IN ({ids_csv}) AND flow_direction='outflow' AND subledger_id IS NULL
          AND transaction_source_type <> 'CardGrant'
        ORDER BY transaction_date, amount_dollars
    """)
    buckets = {}
    for row in ledger:
        row["_org"] = slug_by_id.get(int(row["org_id"]), "?")
        row["_bucket"] = classify(row, row["_org"], tree_slugs, net_by_slug)
        row["_amt"] = -f(row["amount_dollars"])
        b = buckets.setdefault(row["_bucket"], {"amount": 0.0, "n": 0})
        b["amount"] += row["_amt"]
        b["n"] += 1

    stats = query(f"""
        SELECT COALESCE(SUM(balance_cents),0) AS bal,
               COALESCE(SUM(card_grants_total_cents),0) AS cg_total,
               COALESCE(SUM(card_grants_active_cents),0) AS cg_active,
               COALESCE(SUM(total_outflow_cents),0) AS outflow
        FROM public_hcb_analytics.orgs WHERE event_id IN ({ids_csv})
    """)[0]

    cat_totals = {c: 0.0 for c in CATEGORIES}
    for k, v in buckets.items():
        cat_totals[bucket_cat(k)] += v["amount"]

    event_cost = sum(v for c, v in cat_totals.items() if CATEGORIES[c][1] is True)
    not_expense = sum(v for c, v in cat_totals.items() if CATEGORIES[c][1] is False)
    intra_tree = cat_totals.get("I", 0.0)
    gross = sum(v["amount"] for v in buckets.values())   # categorized total (reconciles)
    stated = -f(stats["outflow"]) / 100.0                # orgs-stats cross-check
    wh, n_proj, wh_src = weighted_hours(program_name, hours_name, prog.get("weighted_total"))

    return {
        "program": program_name, "slug": prog["slug"],
        "tree_orgs": sorted(tree_slugs - {prog["slug"]}),
        "child_notes": child_notes,
        "children_mode": children_mode,
        "gross_outflow": gross,
        "stated_outflow": stated,
        "categories": cat_totals,
        "event_cost": event_cost,
        "not_an_expense": not_expense,
        "intra_tree": intra_tree,
        "cost_per_hour": (event_cost / wh) if wh else None,
        "weighted_hours": wh, "weighted_hours_source": wh_src, "approved_projects": n_proj,
        "balance_not_yet_expense": f(stats["bal"]) / 100.0,
        "grants_funded": f(stats["cg_total"]) / 100.0,
        "grants_unspent": f(stats["cg_active"]) / 100.0,
        "budget_per_hour": f(prog.get("budget_per_hour")) or None,
        "stated_vs_categorized": stated - gross,
        "buckets": buckets, "ledger": ledger,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def money(x):
    return f"${x:>13,.2f}"


def print_report(a, show_ledger=False):
    wh = a["weighted_hours"]
    cph = (lambda x: f"${x/wh:6.2f}/hr") if wh else (lambda x: "     n/a")

    print("=" * 80)
    print(f"  {a['program']}  ({a['slug']})")
    print("=" * 80)
    tree = a["tree_orgs"]
    if tree:
        shown = ", ".join(tree[:6]) + (f", … +{len(tree)-6} more" if len(tree) > 6 else "")
        print(f"  Org tree ({len(tree)+1} orgs, --children {a['children_mode']}): + {shown}")
    notes = a["child_notes"]
    if notes.get("prefix_not_suborg"):
        d = sorted(notes["prefix_not_suborg"], key=lambda t: -t[1])
        shown = ", ".join(f"{s} (${amt:,.0f})" for s, amt in d[:5])
        more = f", … +{len(d)-5} more" if len(d) > 5 else ""
        print(f"  Slug lookalikes NOT sub-orgs in HCB (--children all to add): {shown}{more}")
    if notes.get("author_fund_kids"):
        print(f"  Author-fund child orgs kept OUT of tree (transfers stay B): "
          f"{', '.join(notes['author_fund_kids'][:6])}")
    if notes.get("excluded"):
        print(f"  Sub-orgs DETACHED via --exclude-orgs (another program's fund; "
          f"transfers become X): {', '.join(notes['excluded'])}")
    print(f"  EVENT EXPENSE (A + C)              : {money(a['event_cost'])}   {cph(a['event_cost'])}")
    print(f"  Weighted hours                    : {wh:,.0f}   (source: {a['weighted_hours_source']})")
    if a["budget_per_hour"]:
        print(f"  Program budget target             : ${a['budget_per_hour']:.2f}/hr")
    print()
    print("  Where every outflow dollar went:")
    for c, (label, is_exp) in CATEGORIES.items():
        amt = a["categories"].get(c, 0.0)
        if amt == 0 and c in ("X", "I"):
            continue
        tag = "EXPENSE" if is_exp is True else ("not exp" if is_exp is False else "netted ")
        print(f"    [{tag}] {label:<52} {money(amt)}")
    print(f"    {'':>9}{'EVENT EXPENSE (A+C)':<52} {money(a['event_cost'])}")
    print()
    print(f"  Not yet an expense (cash still in tree balances) : {money(a['balance_not_yet_expense'])}")
    print(f"  (grant cards funded {money(a['grants_funded'])}, of which unspent "
          f"{money(a['grants_unspent'])} — already counted as expense)")
    recon = f"  Gross outflow {money(a['gross_outflow'])}  =  event {money(a['event_cost'])}" \
            f"  +  not-an-expense {money(a['not_an_expense'])}"
    if a["intra_tree"]:
        recon += f"  +  intra-tree {money(a['intra_tree'])}"
    print(recon)
    if abs(a["stated_vs_categorized"]) > 1:
        print(f"  (note: orgs-stats outflow {money(a['stated_outflow'])} differs from "
              f"categorized total by {money(a['stated_vs_categorized'])} — ledger/stats mismatch)")
    print()
    print("  Detail (sub-buckets):")
    order = sorted(a["buckets"].items(), key=lambda kv: (bucket_cat(kv[0]), -kv[1]["amount"]))
    for key, v in order:
        label, cat = BUCKETS[key]
        print(f"    [{cat}] {label:<54} {money(v['amount'])}  ({v['n']})")
    print()

    if show_ledger:
        print("  Full ledger (main-ledger outflows, grouped by sub-bucket):")
        for key, _ in order:
            label, cat = BUCKETS[key]
            rows = [r for r in a["ledger"] if r["_bucket"] == key]
            print(f"\n  --- [{cat}] {label} : {money(sum(r['_amt'] for r in rows))} ---")
            for r in sorted(rows, key=lambda r: -r["_amt"]):
                memo = (r["disbursement_name"] or r["display_memo"] or "")[:40]
                dest = f" -> {r['dest_org_slug']}" if r["dest_org_slug"] else ""
                print(f"      {r['transaction_date']}  {money(r['_amt'])}  "
                      f"{r['transaction_type']:<15}{dest:<24} {memo}")
        print()


def main():
    ap = argparse.ArgumentParser(description="True YSWS event expense from the HCB ledger.")
    ap.add_argument("program", nargs="?", help="program name or HCB slug")
    ap.add_argument("--include-orgs", default="",
                    help="comma-separated extra HCB slugs to fold into the org tree "
                         "(e.g. a fulfillment org funded by HQ that doesn't share "
                         "the slug prefix). Transfers between tree orgs are netted.")
    ap.add_argument("--exclude-orgs", default="",
                    help="comma-separated sub-org slugs to DETACH from the tree "
                         "(their whole subtree), for sub-orgs that actually hold a "
                         "different program's money (e.g. stardance-hardware is "
                         "Outpost). Transfers to them classify as X / inter-org.")
    ap.add_argument("--children", choices=["auto", "all", "none"], default="auto",
                    help="child-org discovery: 'auto' descends HCB's real sub-org "
                         "tree (hcb.events.parent_id, default); 'all' additionally "
                         "joins slug-prefix lookalikes that aren't linked sub-orgs; "
                         "'none' = single-org view.")
    ap.add_argument("--hours-name", help="ysws_name to match in approved_projects "
                    "for weighted hours (defaults to the program name)")
    ap.add_argument("--ledger", action="store_true", help="print the full itemized ledger")
    ap.add_argument("--json", action="store_true", help="emit JSON")
    ap.add_argument("--list", action="store_true", help="list programs with HCB orgs")
    args = ap.parse_args()

    programs = list_programs()
    if args.list or not args.program:
        print(f"{'PROGRAM':<28} {'SLUG':<28} {'GROSS OUTFLOW':>15}")
        for p in sorted(programs, key=lambda p: f(p["total_outflow_cents"])):
            print(f"{(p['program_name'] or '')[:27]:<28} {(p['slug'] or '')[:27]:<28} "
                  f"{-f(p['total_outflow_cents'])/100.0:>15,.2f}")
        return

    prog = resolve_slug(args.program, programs)
    includes = [resolve_slug(s.strip(), programs)
                for s in args.include_orgs.split(",") if s.strip()]
    excludes = tuple(s.strip() for s in args.exclude_orgs.split(",") if s.strip())
    a = analyze(prog, prog["program_name"], includes, args.hours_name, args.children, excludes)

    if args.json:
        out = {k: v for k, v in a.items() if k not in ("ledger",)}
        out["buckets"] = {k: {"amount": round(v["amount"], 2), "n": v["n"],
                              "category": bucket_cat(k)} for k, v in a["buckets"].items()}
        out["categories"] = {c: {"amount": round(a["categories"].get(c, 0.0), 2),
                                 "label": CATEGORIES[c][0], "is_expense": CATEGORIES[c][1]}
                             for c in CATEGORIES}
        print(json.dumps(out, indent=2, default=lambda o: round(o, 2) if isinstance(o, float) else o))
    else:
        print_report(a, show_ledger=args.ledger)


if __name__ == "__main__":
    main()
