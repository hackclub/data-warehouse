"""Synthetic fixtures only: cost sync must preserve the website's accounting."""
from types import SimpleNamespace

import polars as pl
import pytest

from orpheus_engine.defs.unified_ysws_db.true_spend_sync import (
    PROGRAMS, SPEND_FIELD, SOURCE_ASSET, URL_FIELD, TOTAL_SPEND_FORMULA,
    build_true_spend_updates, write_true_spend_updates, ysws_programs_hcb_stats,
)


def costs(**overrides):
    row = dict(root_event_id=123, root_slug="synthetic-root", member_ids=["record-a", "record-b"],
               true_spend_dollars=120.25, weighted_hours=30.0, cost_per_weighted_hour=4.01)
    row.update(overrides)
    return pl.DataFrame([row])


def programs(*ids, url="https://hcb.hackclub.com/synthetic-root"):
    return pl.DataFrame({"id": ids, PROGRAMS.hcb: [url] * len(ids)})


def test_shared_root_copies_canonical_spend_without_reallocating():
    rows = build_true_spend_updates(programs("record-a", "record-b"), costs()).to_dicts()
    assert len(rows) == 2
    for r in rows:
        assert r[URL_FIELD] == "https://ysws-true-spend.hackclub.com/programs/synthetic-root.html"
        assert r[SPEND_FIELD] == 120.25
        assert PROGRAMS.cost_per_hour not in r


def test_unknown_program_is_null_not_zero_or_old_gross_spend():
    r = build_true_spend_updates(programs("unmatched"), costs()).row(0, named=True)
    assert all(r[f] is None for f in (SPEND_FIELD, URL_FIELD))


@pytest.mark.parametrize('url', [None, '', 'https://example.invalid/synthetic-root',
                                'https://hcb.hackclub.com/changed-root'])
def test_removed_or_remapped_hcb_link_clears_old_values(url):
    r = build_true_spend_updates(programs("record-a", url=url), costs()).row(0, named=True)
    assert r[SPEND_FIELD] is None
    assert r[URL_FIELD] is None


def test_transaction_url_matches_root_and_record_id_not_program_name():
    r = build_true_spend_updates(programs("record-a", url="https://hcb.hackclub.com/synthetic-root/transactions?x=1"), costs())
    assert r[SPEND_FIELD][0] == 120.25


def test_zero_spend_and_null_rate_are_preserved():
    r = build_true_spend_updates(programs("record-a"), costs(true_spend_dollars=0, weighted_hours=0, cost_per_weighted_hour=None))
    assert r[SPEND_FIELD][0] == 0
    assert PROGRAMS.cost_per_hour not in r.columns


def test_empty_source_fails_without_clearing_everything():
    with pytest.raises(ValueError, match="source is empty"):
        build_true_spend_updates(programs("record-a"), costs().head(0))


def test_duplicate_source_mapping_fails_instead_of_double_counting():
    with pytest.raises(ValueError, match="multiple canonical"):
        build_true_spend_updates(programs("record-a"), pl.concat([costs(), costs()]))


def test_duplicate_airtable_ids_fail():
    with pytest.raises(ValueError, match="Duplicate Airtable"):
        build_true_spend_updates(programs("record-a", "record-a"), costs())


def test_dependency_is_canonical_mart():
    assert SOURCE_ASSET in ysws_programs_hcb_stats.asset_deps[ysws_programs_hcb_stats.key]


class FakeTable:
    def __init__(self, missing=False):
        self.records = None
        self.missing = missing
    def schema(self, force=False):
        assert force
        return SimpleNamespace(fields=[] if self.missing else [
            SimpleNamespace(name="Total Spent From HCB Fund", id=SPEND_FIELD, type="currency"),
            SimpleNamespace(name="Cost Per Hour", id=PROGRAMS.cost_per_hour, type="formula"),
            SimpleNamespace(name=URL_FIELD, id="synthetic-url-field", type="url"),
            SimpleNamespace(name="Total Spend", id=PROGRAMS.total_spend, type="formula",
                            options=SimpleNamespace(referenced_field_ids=[SPEND_FIELD],
                                                    formula=TOTAL_SPEND_FORMULA, is_valid=True)),
        ])
    def batch_update(self, records):
        self.records = records
        return records


def test_writer_preserves_explicit_nulls_and_only_writes_owned_fields():
    table = FakeTable()
    updates = build_true_spend_updates(programs("unmatched"), costs())
    assert write_true_spend_updates(table, updates) == 1
    assert table.records == [{"id": "unmatched", "fields": {
        SPEND_FIELD: None, "synthetic-url-field": None,
    }}]


def test_missing_schema_blocks_writes():
    table = FakeTable(missing=True)
    with pytest.raises(ValueError, match="writable currency"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
    assert table.records is None


def test_legacy_formulas_block_cutover_before_any_writes():
    table = FakeTable()
    schema = table.schema(force=True)
    schema.fields[-1].options.referenced_field_ids.append("synthetic-postage-field")
    table.schema = lambda **kwargs: schema
    with pytest.raises(ValueError, match="formula"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
    assert table.records is None


def test_existing_cost_per_hour_formula_is_untouched():
    table = FakeTable()
    assert write_true_spend_updates(
        table, build_true_spend_updates(programs("record-a"), costs()),
    ) == 1
    assert PROGRAMS.cost_per_hour not in table.records[0]["fields"]


def test_link_uses_same_safe_filename_as_site():
    row = build_true_spend_updates(
        programs("record-a", url="https://hcb.hackclub.com/unsafe%20slug"),
        costs(root_slug="unsafe%20slug"),
    ).row(0, named=True)
    assert row[URL_FIELD].endswith("/programs/program-123.html")


@pytest.mark.parametrize("field_type", ["singleLineText", "formula", None])
def test_url_column_must_exist_and_be_writable_url(field_type):
    table = FakeTable()
    schema = table.schema(force=True)
    schema.fields[2].type = field_type
    table.schema = lambda **kwargs: schema
    with pytest.raises(ValueError, match="URL column"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
    assert table.records is None


def test_writer_resolves_url_field_id_and_does_not_write_rates_or_budgets():
    table = FakeTable()
    updates = build_true_spend_updates(
        programs("record-a"), costs(cost_per_weighted_hour=15.40),
    )
    assert write_true_spend_updates(table, updates) == 1
    assert table.records[0]["fields"] == {
        SPEND_FIELD: 120.25,
        "synthetic-url-field": "https://ysws-true-spend.hackclub.com/programs/synthetic-root.html",
    }


def test_empty_programs_produces_typed_empty_updates():
    updates = build_true_spend_updates(programs("record-a").head(0), costs())
    assert updates.is_empty()
    assert updates.schema[URL_FIELD] == pl.String
    table = FakeTable()
    assert write_true_spend_updates(table, updates) == 0
    assert table.records is None


@pytest.mark.parametrize("formula", [
    "{wrong-field}",
    "{" + SPEND_FIELD + "} + 100",
    "IF({" + SPEND_FIELD + "}, {" + SPEND_FIELD + "})",
])
def test_total_spend_formula_must_preserve_exact_amount_and_zero(formula):
    table = FakeTable()
    schema = table.schema(force=True)
    schema.fields[-1].options.formula = formula
    table.schema = lambda **kwargs: schema
    with pytest.raises(ValueError, match="formula"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
    assert table.records is None


def test_incomplete_batch_response_fails():
    table = FakeTable()
    table.batch_update = lambda records: []
    with pytest.raises(RuntimeError, match="Incomplete"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
