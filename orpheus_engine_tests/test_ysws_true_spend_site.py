"""
Tests for the YSWS true-spend static site renderer.

The site is generated from the warehouse and force-pushed to a PUBLIC repo, so
the invariants worth pinning are the ones that would either publish something
wrong or break browsing: totals that disagree with the transactions behind
them, HTML that is not escaped, page names that escape their directory, and
links that point at files the renderer never emits.

No network or database access; the renderer is pure.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from orpheus_engine.defs.ysws_true_spend_site.data import SiteData
from orpheus_engine.defs.ysws_true_spend_site.definitions import (
    COMMIT_EMAIL,
    COMMIT_NAME,
)
from orpheus_engine.defs.ysws_true_spend_site.freshness import Freshness
from orpheus_engine.defs.ysws_true_spend_site.site import (
    ago,
    money,
    page_slug,
    redact,
    render_site,
)

GENERATED_AT = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)


def _program(**overrides):
    program = {
        "program_name": "Fallout",
        "bucket": "program",
        "root_event_id": 1,
        "root_slug": "fallout",
        "org_count": 2,
        "first_outflow_date": date(2026, 3, 14),
        "last_outflow_date": date(2026, 8, 6),
        "true_spend_dollars": Decimal("300.00"),
        "spent_on_event_dollars": Decimal("250.00"),
        "internal_cost_dollars": Decimal("50.00"),
        "author_fund_dollars": Decimal("10.00"),
        "returned_to_hq_dollars": Decimal("5.00"),
        "other_internal_dollars": Decimal("0.00"),
        "intra_tree_dollars": Decimal("20.00"),
        "gross_outflow_dollars": Decimal("335.00"),
        "stated_outflow_dollars": Decimal("335.00"),
        "stated_overstatement_pct": Decimal("10.4"),
        "funded_by_marketing_dollars": Decimal("0.00"),
        "balance_dollars": Decimal("100.00"),
        "card_grants_funded_dollars": Decimal("75.00"),
        "card_grants_active_face_dollars": Decimal("60.00"),
        "card_grants_remaining_dollars": Decimal("25.00"),
        "grant_card_count": 3,
        "is_ysws_program": True,
        "match_source": "unified_ysws_hcb_link",
        "weighted_projects": Decimal("4.00"),
        "weighted_hours": Decimal("40"),
        "approved_project_count": 4,
        "cost_per_weighted_hour": Decimal("7.50"),
        "external_revenue_dollars": Decimal("400.00"),
        "intra_tree_revenue_dollars": Decimal("20.00"),
        "gross_inflow_dollars": Decimal("420.00"),
        "transaction_count": 3,
    }
    program.update(overrides)
    return program


def _org(slug, event_id, parent_id, depth, spend, revenue, is_public=True):
    return {
        "is_public": is_public,
        "root_event_id": 1,
        "root_slug": "fallout",
        "program_name": "Fallout",
        "bucket": "program",
        "event_id": event_id,
        "parent_id": parent_id,
        "org_slug": slug,
        "org_name": slug.title(),
        "depth": depth,
        "balance_dollars": Decimal("50.00"),
        "external_revenue_dollars": Decimal(revenue),
        "intra_tree_revenue_dollars": Decimal("0.00"),
        "external_revenue_count": 1,
        "true_spend_dollars": Decimal(spend),
        "gross_outflow_dollars": Decimal(spend),
        "transaction_count": 1,
    }


def _spend_txn(**overrides):
    txn = {
        "root_slug": "fallout",
        "org_slug": "fallout",
        "org_name": "Fallout",
        "transaction_date": date(2026, 8, 6),
        "spend_category": "A",
        "spend_bucket": "grants",
        "spend_bucket_label": "A - Grants to makers",
        "transaction_type": "disbursement",
        "is_true_spend": True,
        "is_synthetic_offset": False,
        "outflow_dollars": Decimal("250.00"),
        "description": "Grant to a maker",
        "counterparty": "Fallout",
        "initiated_by_name": "Sam Liu",
        "transfer_recipient_email": None,
        "transfer_purpose": None,
        "hcb_code": "HCB-500-64019",
        "hcb_url": "https://hcb.hackclub.com/hcb/HCB-500-64019",
        "receipt_count": 0,
    }
    txn.update(overrides)
    return txn


def _revenue_txn(**overrides):
    txn = {
        "root_slug": "fallout",
        "org_slug": "fallout",
        "org_name": "Fallout",
        "transaction_date": date(2026, 3, 14),
        "transaction_type": "disbursement",
        "amount_dollars": Decimal("400.00"),
        "description": "Program funding",
        "source": "Hack Club HQ",
        "source_org_slug": "hq",
        "hcb_code": "HCB-500-1",
        "hcb_url": "https://hcb.hackclub.com/hcb/HCB-500-1",
        "is_intra_tree": False,
    }
    txn.update(overrides)
    return txn


def _unmatched_org(**overrides):
    org = {
        "event_id": 99,
        "org_slug": "som-sticker-shipments",
        "org_name": "SoM Sticker Shipments",
        "reason": "funded_by_mapped_program",
        "related_programs": "Fallout, Summer of Making",
        "parent_slug": None,
        "parent_is_mapped": False,
        "is_hq": False,
        "plan_category": "standard",
        "dollars_from_programs": Decimal("25000.00"),
        "dollars_to_programs": Decimal("0.00"),
        "gross_outflow_dollars": Decimal("24000.00"),
        "balance_dollars": Decimal("1000.00"),
        "hcb_url": "https://hcb.hackclub.com/som-sticker-shipments",
    }
    org.update(overrides)
    return org


def _unlinked_program(**overrides):
    program = {
        "program_id": "recABC",
        "program_name": "Outpost",
        "hcb_field": None,
        "linked_slug": None,
        "gap_type": "no_hcb_link",
    }
    program.update(overrides)
    return program


def _site_data(**overrides) -> SiteData:
    data = SiteData(
        programs=[_program()],
        orgs_by_program={
            "fallout": [
                _org("fallout", 1, None, 0, "250.00", "400.00"),
                _org("fallout-sub", 2, 1, 1, "50.00", "0.00"),
            ]
        },
        spend_by_program={
            "fallout": [
                _spend_txn(),
                _spend_txn(
                    spend_category="C",
                    spend_bucket="internal_payment",
                    outflow_dollars=Decimal("50.00"),
                    org_slug="fallout-sub",
                ),
                _spend_txn(
                    spend_category="B",
                    spend_bucket="author_fund",
                    is_true_spend=False,
                    outflow_dollars=Decimal("10.00"),
                ),
            ]
        },
        revenue_by_program={
            "fallout": [
                _revenue_txn(),
                _revenue_txn(is_intra_tree=True, amount_dollars=Decimal("20.00")),
            ]
        },
        unmatched_orgs=[_unmatched_org()],
        unlinked_programs=[_unlinked_program()],
        freshness=Freshness(
            hcb_pulled_at=GENERATED_AT - timedelta(hours=5),
            hcb_data_through=GENERATED_AT - timedelta(hours=5),
            recalculated_at=GENERATED_AT - timedelta(minutes=30),
        ),
    )
    for key, value in overrides.items():
        setattr(data, key, value)
    return data


def _render(data=None):
    return render_site(data or _site_data(), GENERATED_AT)


def _site_data_with_marketing() -> SiteData:
    """The real data always carries the HQ marketing row alongside the programs."""
    data = _site_data()
    data.programs = data.programs + [
        _program(
            program_name="Marketing (HQ)",
            root_slug="ysws-marketing",
            root_event_id=2,
            is_ysws_program=False,
            match_source="manual_non_program",
            weighted_projects=None,
            weighted_hours=None,
            cost_per_weighted_hour=None,
        )
    ]
    data.orgs_by_program["ysws-marketing"] = [
        _org("ysws-marketing", 20, None, 0, "100.00", "0.00")
    ]
    return data


def test_commits_are_authored_by_the_warehouse():
    """The public repo's history should name the system that writes it."""
    assert COMMIT_NAME == "Hack Club Data Warehouse"
    assert COMMIT_EMAIL.endswith("@hackclub.com")


def test_money_formats_negatives_and_thousands():
    assert money(Decimal("1234.5")) == "$1,234.50"
    assert money(Decimal("-1234.5")) == "-$1,234.50"
    assert money(None) == "$0.00"


def test_page_slug_rejects_path_traversal():
    assert page_slug("fallout", 1) == "fallout"
    assert page_slug("../../etc/passwd", 7) == "program-7"
    assert page_slug("", 7) == "program-7"


def test_expected_files_are_emitted():
    files = _render()
    for expected in (
        "index.html",
        "programs/fallout.html",
        "data/programs.json",
        "data/programs/fallout.json",
        "data/unmatched.json",
        ".nojekyll",
        "README.md",
    ):
        assert expected in files, expected


def test_every_internal_link_points_at_an_emitted_file():
    import posixpath
    import re

    files = _render()
    for path, content in files.items():
        if not path.endswith(".html"):
            continue
        base = posixpath.dirname(path)
        for href in re.findall(r'href="([^"]+)"', content):
            if href.startswith(("http://", "https://", "#")):
                continue
            target = posixpath.normpath(posixpath.join(base, href.split("#")[0]))
            assert target in files, f"{path} links to missing {target}"


def test_program_page_totals_match_its_transactions():
    """The category table is the audit trail for the headline number."""
    files = _render()
    page = files["programs/fallout.html"]
    # A $250 + C $50 = $300 true spend; B $10 is shown but excluded.
    assert "$300.00" in page
    assert "<td>excluded</td>" in page
    assert "Revenue (in from outside the tree)</td><td class=\"n\">$400.00" in page


def test_index_shows_every_program_and_its_org_nesting():
    files = _render()
    index = files["index.html"]
    assert "Fallout" in index
    assert 'href="programs/fallout.html"' in index
    # the org tree rides in a hidden detail row under the program's row
    assert '<tr class="detail" hidden>' in index
    assert "fallout-sub" in index


def test_org_trees_collapse_at_every_level():
    """A 3-deep, 256-org program needs each layer foldable, open by default."""
    files = _render()
    index = files["index.html"]
    # depth is what the script folds on, and nothing starts collapsed
    assert 'data-depth="0" data-collapsed="false"' in index
    assert 'data-depth="1" data-collapsed="false"' in index
    # the parent gets a toggle and a child count; the leaf gets a spacer
    assert 'button class="tgo"' in index
    assert "(1 sub-org)" in index
    assert "tgo-spacer" in index

    # the program page shows the same tree, with the anchors the index links to
    page = files["programs/fallout.html"]
    assert 'id="org-fallout-sub"' in page
    assert 'data-collapsed="false"' in page


def test_program_table_is_sortable_and_numbers_carry_raw_values():
    """Sorting is client-side on data-v, so every numeric cell needs one."""
    index = _render()["index.html"]
    assert 'class="sortable" id="programs"' in index
    for header in ("True spend", "$ / weighted hour", "Weighted projects", "Orgs"):
        assert f'data-type="num">{header}' in index, header
    # spend/balance/$-per-hour raw values present for the one program
    assert 'data-v="300.00"' in index
    assert 'data-v="7.50"' in index
    assert 'data-v="4.00"' in index


def test_index_has_no_revenue_column():
    index = _render()["index.html"]
    header_row = index.split("<thead>")[1].split("</thead>")[0]
    assert "Revenue" not in header_row


def test_freshness_is_shown_with_both_clocks():
    index = _render()["index.html"]
    assert "HCB data pulled" in index
    assert "Spend recalculated" in index
    assert "5 hours ago" in index
    assert "30 minutes ago" in index


def test_stale_mirror_still_readable_from_the_clocks():
    """No prose warning any more; the pulled-at row has to carry the age."""
    data = _site_data()
    data.freshness.hcb_pulled_at = GENERATED_AT - timedelta(days=12)
    index = render_site(data, GENERATED_AT)["index.html"]
    assert "12 days ago" in index


def test_ago_formats_the_units_it_uses():
    now = GENERATED_AT
    assert ago(now - timedelta(days=12), now) == "12 days ago"
    assert ago(now - timedelta(hours=1), now) == "1 hour ago"
    assert ago(now - timedelta(seconds=5), now) == "just now"
    assert ago(None, now) == ""


def test_homepage_sections_are_collapsible_and_in_order():
    """Linked programs, unlinked programs, marketing, orgs nobody claims."""
    files = _render(_site_data_with_marketing())
    assert "unmatched.html" not in files
    index = files["index.html"]
    linked = index.index("YSWS Programs w/ Linked HCBs")
    unlinked = index.index("YSWS Programs w/ No Linked HCBs")
    marketing = index.index("<h2>YSWS - Marketing</h2>")
    orgs = index.index("HCB orgs no program claims")
    assert linked < unlinked < marketing < orgs
    # every section header is a <summary>, and only the programs one starts open
    assert index.count("<summary><h2>") == 4
    assert "<details open><summary><h2>YSWS Programs w/ Linked HCBs" in index
    assert "<details><summary><h2>HCB orgs no program claims" in index
    # the content is all still there
    assert "som-sticker-shipments" in index
    assert "took money from programs" in index
    assert "Outpost" in index
    assert "No HCB link on the Unified YSWS DB record" in index


def test_marketing_is_out_of_the_ysws_program_table():
    """It is not a YSWS program, so it does not sit in a table of them."""
    index = render_site(_site_data_with_marketing(), GENERATED_AT)["index.html"]

    programs_table = index.split('id="programs"')[1].split("</table>")[0]
    assert "Fallout" in programs_table
    assert "ysws-marketing" not in programs_table
    assert "Marketing (HQ)" not in programs_table

    marketing_table = index.split('id="non-programs"')[1].split("</table>")[0]
    assert "Marketing (HQ)" in marketing_table
    assert "<h2>YSWS - Marketing</h2>" in index
    # the section note carries the caveat, so the row no longer repeats it
    assert "(not a YSWS program)</span>" not in index

    # a program count of 1 in the heading, marketing excluded
    assert "YSWS Programs w/ Linked HCBs (1)" in index


def test_unmatched_table_is_width_constrained():
    """9 nowrap columns ran off the screen; it is a fixed-layout table now."""
    index = _render()["index.html"]
    section = index.split("HCB orgs no program claims")[1]
    assert 'class="sortable fixed"' in section
    assert "<colgroup>" in section
    widths = [int(w.split("%")[0]) for w in section.split('style="width:')[1:9]]
    assert sum(widths) == 100, widths


def test_homepage_carries_no_explanatory_prose_blocks():
    """Zach stripped the intro, the sort hint and the totals table."""
    index = _render()["index.html"]
    for gone in (
        "was sent from mapped programs into these orgs",
        "usually correct as-is",
        "What each YSWS program actually spent",
        "Click a column heading to sort",
        "expand all",
        "<h2>Totals</h2>",
        "Spend as HCB states it",
    ):
        assert gone not in index, gone


def test_no_methodology_page_anywhere():
    files = _render()
    assert "methodology.html" not in files
    for path, content in files.items():
        assert "methodology.html" not in content, path


def test_publication_mirrors_hcb_transparency():
    """
    Measured against HCB's public API: names are published, emails never are.
    """
    assert redact("Grant to person@example.com") == "Grant to [email hidden]"
    assert redact("Grant to Youssef Ayman") == "Grant to Youssef Ayman"

    data = _site_data()
    data.spend_by_program["fallout"][0]["description"] = "Grant to maker@gmail.com"
    data.spend_by_program["fallout"][0]["counterparty"] = "maker@gmail.com"
    page = render_site(data, GENERATED_AT)["programs/fallout.html"]
    assert "maker@gmail.com" not in page
    assert "[email hidden]" in page
    # the name columns stay: HCB publishes full_name on every transaction
    assert "Sam Liu" in page


def test_private_orgs_are_summarised_not_listed():
    """An org outside transparency mode publishes no ledger, so neither do we."""
    data = _site_data()
    data.orgs_by_program["fallout"][1]["is_public"] = False
    data.spend_by_program["fallout"][1]["org_slug"] = "fallout-sub"
    data.spend_by_program["fallout"][1]["description"] = "SECRET LINE ITEM"
    page = render_site(data, GENERATED_AT)["programs/fallout.html"]

    assert "SECRET LINE ITEM" not in page
    assert "not in transparency mode" in page
    assert "1 outflows totalling" in page
    # withholding detail must not move a single total
    assert "True spend (A + C + offsets)</td><td class=\"n\">$300.00" in page
    assert "Spend transactions (2)" in page


def test_html_is_escaped():
    data = _site_data()
    data.spend_by_program["fallout"][0]["description"] = '<script>alert("x")</script>'
    files = render_site(data, GENERATED_AT)
    page = files["programs/fallout.html"]
    assert "<script>" not in page
    assert "&lt;script&gt;" in page


def test_intra_tree_inflows_are_excluded_from_revenue_but_still_listed():
    files = _render()
    page = files["programs/fallout.html"]
    assert "Revenue transactions (1)" in page
    assert "not counted as revenue" in page


def test_grant_cards_are_labelled_as_committed_spend_not_leftovers():
    """The old column exposed active face value as if it were unspent money."""
    page = _render()["programs/fallout.html"]
    assert "Grant cards funded (counted as spend above)" in page
    assert "Still sitting on those cards" in page
    assert "Card grants unspent" not in page


def test_program_json_carries_the_match_provenance():
    import json

    detail = json.loads(_render()["data/programs/fallout.json"])
    assert detail["match_source"] == "unified_ysws_hcb_link"
    assert detail["is_ysws_program"] is True
    assert detail["weighted_projects"] == 4.0


def test_program_json_omits_transaction_arrays_but_keeps_counts():
    import json

    files = _render()
    detail = json.loads(files["data/programs/fallout.json"])
    assert "spend_transactions" not in detail
    assert detail["spend_transaction_count"] == 3
    assert detail["revenue_transaction_count"] == 1
    assert detail["true_spend_dollars"] == 300.0
    assert len(detail["orgs"]) == 2

    summary = json.loads(files["data/programs.json"])
    assert summary["program_count"] == 1
    assert summary["programs"][0]["page"] == "programs/fallout.html"
