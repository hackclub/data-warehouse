"""
Warehouse queries behind the YSWS true-spend static site.

Everything here reads the dbt models in `public_hcb_ysws_true_spend_analytics`
(see orpheus_engine_dbt/models/hcb_ysws_true_spend_analytics/) plus the HCB
ledger, and returns plain Python dicts. No Dagster imports, so the site can be
generated locally with `python -m orpheus_engine.defs.ysws_true_spend_site`.

Revenue is deliberately NOT "HCB total raised". It is money entering the
program's org tree from OUTSIDE that tree (HQ funding, donations, refunds),
with transfers between a program's own sub-orgs excluded — the same
intra-tree netting the spend side does, so a 258-org program like Campfire
isn't credited twice for its own internal plumbing.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .freshness import Freshness, hcb_data_through

SPEND_SCHEMA = "public_hcb_ysws_true_spend_analytics"
HCB_SCHEMA = "public_hcb_analytics"

# One row per program (canonical root HCB org), straight from the dbt rollup.
PROGRAMS_SQL = f"""
SELECT
    program_name,
    bucket,
    is_ysws_program,
    match_source,
    root_event_id,
    root_slug,
    org_count,
    first_outflow_date,
    last_outflow_date,
    true_spend_dollars,
    spent_on_event_dollars,
    internal_cost_dollars,
    author_fund_dollars,
    returned_to_hq_dollars,
    other_internal_dollars,
    intra_tree_dollars,
    gross_outflow_dollars,
    stated_outflow_dollars,
    stated_overstatement_pct,
    funded_by_marketing_dollars,
    balance_dollars,
    card_grants_funded_dollars,
    card_grants_active_face_dollars,
    card_grants_remaining_dollars,
    grant_card_count,
    weighted_projects,
    weighted_hours,
    approved_project_count,
    cost_per_weighted_hour
FROM {SPEND_SCHEMA}.ysws_spend_by_program
ORDER BY true_spend_dollars DESC NULLS LAST
"""

# Every HCB org in every program tree, with its own revenue / spend / balance
# and the parent link the site uses to render the nesting.
TREE_SQL = f"""
WITH tree AS (
    SELECT
        t.root_event_id,
        t.root_slug,
        t.program_name,
        t.bucket,
        t.event_id,
        t.org_slug,
        t.org_name,
        t.depth,
        t.balance_cents,
        o.parent_id,
        -- Whether HCB itself publishes this org's ledger. Orgs outside
        -- transparency mode show nothing publicly, so the site aggregates
        -- their transactions instead of listing them.
        o.is_public
    FROM {SPEND_SCHEMA}.ysws_spend_org_tree t
    JOIN {HCB_SCHEMA}.orgs o ON o.event_id = t.event_id
),

inflow_rows AS (
    SELECT
        t.root_event_id,
        t.event_id AS org_id,
        l.amount_dollars,
        EXISTS (
            SELECT 1 FROM tree s
            WHERE s.root_event_id = t.root_event_id
              AND s.org_slug = l.source_org_slug
        ) AS is_intra_tree
    FROM {HCB_SCHEMA}.ledger l
    JOIN tree t ON t.event_id = l.org_id
    WHERE l.flow_direction = 'inflow'
      AND l.subledger_id IS NULL
      AND l.transaction_source_type IS DISTINCT FROM 'CardGrant'
),

revenue AS (
    SELECT
        root_event_id,
        org_id,
        ROUND(COALESCE(SUM(amount_dollars) FILTER (WHERE NOT is_intra_tree), 0)::numeric, 2)
            AS external_revenue_dollars,
        ROUND(COALESCE(SUM(amount_dollars) FILTER (WHERE is_intra_tree), 0)::numeric, 2)
            AS intra_tree_revenue_dollars,
        COUNT(*) FILTER (WHERE NOT is_intra_tree) AS external_revenue_count
    FROM inflow_rows
    GROUP BY 1, 2
),

spend AS (
    SELECT
        root_event_id,
        org_id,
        ROUND(COALESCE(SUM(outflow_dollars) FILTER (WHERE is_true_spend), 0)::numeric, 2)
            AS true_spend_dollars,
        ROUND(COALESCE(SUM(outflow_dollars) FILTER (WHERE NOT is_synthetic_offset), 0)::numeric, 2)
            AS gross_outflow_dollars,
        COUNT(*) AS transaction_count
    FROM {SPEND_SCHEMA}.ysws_spend_ledger
    GROUP BY 1, 2
)

SELECT
    t.root_event_id,
    t.root_slug,
    t.program_name,
    t.bucket,
    t.event_id,
    t.parent_id,
    t.org_slug,
    t.org_name,
    t.depth,
    t.is_public,
    ROUND(t.balance_cents / 100.0, 2) AS balance_dollars,
    COALESCE(r.external_revenue_dollars, 0) AS external_revenue_dollars,
    COALESCE(r.intra_tree_revenue_dollars, 0) AS intra_tree_revenue_dollars,
    COALESCE(r.external_revenue_count, 0) AS external_revenue_count,
    COALESCE(s.true_spend_dollars, 0) AS true_spend_dollars,
    COALESCE(s.gross_outflow_dollars, 0) AS gross_outflow_dollars,
    COALESCE(s.transaction_count, 0) AS transaction_count
FROM tree t
LEFT JOIN revenue r ON r.root_event_id = t.root_event_id AND r.org_id = t.event_id
LEFT JOIN spend s ON s.root_event_id = t.root_event_id AND s.org_id = t.event_id
ORDER BY t.root_slug, t.depth, lower(t.org_name)
"""

# Every classified outflow the true-spend model counted (or deliberately did
# not count) for each program.
SPEND_TRANSACTIONS_SQL = f"""
SELECT
    root_slug,
    org_slug,
    org_name,
    transaction_date,
    spend_category,
    spend_bucket,
    spend_bucket_label,
    transaction_type,
    is_true_spend,
    is_synthetic_offset,
    ROUND(outflow_dollars::numeric, 2) AS outflow_dollars,
    COALESCE(disbursement_name, display_memo) AS description,
    COALESCE(dest_org_name, counterparty_name, transfer_recipient_name) AS counterparty,
    initiated_by_name,
    transfer_recipient_email,
    transfer_purpose,
    hcb_code,
    hcb_url,
    receipt_count
FROM {SPEND_SCHEMA}.ysws_spend_ledger
ORDER BY root_slug, transaction_date DESC NULLS LAST, hcb_code
"""

# Inflows for the same trees, split into external revenue and intra-tree
# plumbing (shown separately, never counted as revenue).
REVENUE_TRANSACTIONS_SQL = f"""
WITH tree AS (
    SELECT root_event_id, root_slug, event_id, org_slug, org_name
    FROM {SPEND_SCHEMA}.ysws_spend_org_tree
)
SELECT
    t.root_slug,
    t.org_slug,
    t.org_name,
    l.transaction_date,
    l.transaction_type,
    ROUND(l.amount_dollars::numeric, 2) AS amount_dollars,
    COALESCE(l.display_memo, l.disbursement_name) AS description,
    COALESCE(l.source_org_name, l.counterparty_name, l.donor_name) AS source,
    l.source_org_slug,
    l.hcb_code,
    CASE WHEN l.hcb_code LIKE 'HCB-%'
         THEN 'https://hcb.hackclub.com/hcb/' || l.hcb_code END AS hcb_url,
    EXISTS (
        SELECT 1 FROM tree s
        WHERE s.root_event_id = t.root_event_id
          AND s.org_slug = l.source_org_slug
    ) AS is_intra_tree
FROM {HCB_SCHEMA}.ledger l
JOIN tree t ON t.event_id = l.org_id
WHERE l.flow_direction = 'inflow'
  AND l.subledger_id IS NULL
  AND l.transaction_source_type IS DISTINCT FROM 'CardGrant'
ORDER BY t.root_slug, l.transaction_date DESC NULLS LAST, l.hcb_code
"""


# The fix-it lists behind the mapping contract: HCB orgs no YSWS program
# claims, and Unified YSWS DB programs whose HCB link cannot be used.
UNMATCHED_ORGS_SQL = f"""
SELECT
    event_id,
    org_slug,
    org_name,
    reason,
    related_programs,
    parent_slug,
    parent_is_mapped,
    is_hq,
    plan_category,
    dollars_from_programs,
    dollars_to_programs,
    gross_outflow_dollars,
    balance_dollars,
    hcb_url
FROM {SPEND_SCHEMA}.ysws_unmatched_orgs
ORDER BY GREATEST(dollars_from_programs, dollars_to_programs) DESC, org_slug
"""

UNLINKED_PROGRAMS_SQL = f"""
SELECT program_id, program_name, hcb_field, linked_slug, gap_type
FROM {SPEND_SCHEMA}.ysws_unlinked_programs
ORDER BY gap_type, program_name
"""


@dataclass
class SiteData:
    """Everything the renderer needs, already grouped by program root slug."""

    programs: List[Dict[str, Any]] = field(default_factory=list)
    orgs_by_program: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    spend_by_program: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    revenue_by_program: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    unmatched_orgs: List[Dict[str, Any]] = field(default_factory=list)
    unlinked_programs: List[Dict[str, Any]] = field(default_factory=list)
    # Filled by the caller: the asset reads the Dagster instance, the preview
    # script reads the prod Dagster database. See freshness.py.
    freshness: Optional["Freshness"] = None

    @property
    def transaction_count(self) -> int:
        return sum(len(v) for v in self.spend_by_program.values()) + sum(
            len(v) for v in self.revenue_by_program.values()
        )


def _rows(conn, sql: str) -> List[Dict[str, Any]]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]


def _group(rows: List[Dict[str, Any]], key: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(row[key], []).append(row)
    return out


def _d(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def fetch_site_data(conn) -> SiteData:
    """Run the four queries and roll tree revenue up onto each program."""
    programs = _rows(conn, PROGRAMS_SQL)
    orgs = _rows(conn, TREE_SQL)
    spend_txns = _rows(conn, SPEND_TRANSACTIONS_SQL)
    revenue_txns = _rows(conn, REVENUE_TRANSACTIONS_SQL)
    unmatched_orgs = _rows(conn, UNMATCHED_ORGS_SQL)
    unlinked_programs = _rows(conn, UNLINKED_PROGRAMS_SQL)

    orgs_by_program = _group(orgs, "root_slug")

    # Program revenue = sum of its orgs' external revenue. Rolled up here
    # rather than in SQL so the per-org numbers on the page and the program
    # headline can never disagree.
    for program in programs:
        tree = orgs_by_program.get(program["root_slug"], [])
        program["external_revenue_dollars"] = sum(
            (_d(o["external_revenue_dollars"]) for o in tree), Decimal(0)
        )
        program["intra_tree_revenue_dollars"] = sum(
            (_d(o["intra_tree_revenue_dollars"]) for o in tree), Decimal(0)
        )
        program["gross_inflow_dollars"] = (
            program["external_revenue_dollars"] + program["intra_tree_revenue_dollars"]
        )
        program["transaction_count"] = sum(int(o["transaction_count"] or 0) for o in tree)

    return SiteData(
        programs=programs,
        orgs_by_program=orgs_by_program,
        spend_by_program=_group(spend_txns, "root_slug"),
        revenue_by_program=_group(revenue_txns, "root_slug"),
        unmatched_orgs=unmatched_orgs,
        unlinked_programs=unlinked_programs,
        freshness=Freshness(hcb_data_through=hcb_data_through(conn)),
    )
