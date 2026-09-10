"""Synthetic fixtures only: cost sync must preserve the website's accounting."""
from types import SimpleNamespace

import polars as pl
import pytest

from orpheus_engine.defs.unified_ysws_db.true_spend_sync import (
    PROGRAMS, SPEND_FIELD, HOURS_FIELD, RATE_FIELD, SOURCE_ASSET,
    build_true_spend_updates, write_true_spend_updates, ysws_programs_hcb_stats,
)


def costs(**overrides):
    row = dict(root_slug="synthetic-root", member_ids=["record-a", "record-b"],
               true_spend_dollars=120.25, weighted_hours=30.0, cost_per_weighted_hour=4.01)
    row.update(overrides)
    return pl.DataFrame([row])


def programs(*ids, url="https://hcb.hackclub.com/synthetic-root"):
    return pl.DataFrame({"id": ids, PROGRAMS.hcb: [url] * len(ids)})


def test_shared_root_copies_canonical_rate_without_reallocating_or_recalculating():
    rows = build_true_spend_updates(programs("record-a", "record-b"), costs()).to_dicts()
    assert len(rows) == 2
    for r in rows:
        assert r[SPEND_FIELD] == 120.25
        assert r[HOURS_FIELD] == 30.0
        assert r[RATE_FIELD] == 4.01  # Direct rounded mart value, not 120.25 / 30.


def test_unknown_program_is_null_not_zero_or_old_gross_spend():
    r = build_true_spend_updates(programs("unmatched"), costs()).row(0, named=True)
    assert all(r[f] is None for f in (SPEND_FIELD, HOURS_FIELD, RATE_FIELD))


@pytest.mark.parametrize('url', [None, '', 'https://example.invalid/synthetic-root',
                                'https://hcb.hackclub.com/changed-root'])
def test_removed_or_remapped_hcb_link_clears_old_values(url):
    r = build_true_spend_updates(programs("record-a", url=url), costs()).row(0, named=True)
    assert r[SPEND_FIELD] is None
    assert r[RATE_FIELD] is None


def test_transaction_url_matches_root_and_record_id_not_program_name():
    r = build_true_spend_updates(programs("record-a", url="https://hcb.hackclub.com/synthetic-root/transactions?x=1"), costs())
    assert r[SPEND_FIELD][0] == 120.25


def test_zero_spend_and_null_rate_are_preserved():
    r = build_true_spend_updates(programs("record-a"), costs(true_spend_dollars=0, weighted_hours=0, cost_per_weighted_hour=None))
    assert r[SPEND_FIELD][0] == 0
    assert r[RATE_FIELD][0] is None


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
            SimpleNamespace(name=HOURS_FIELD, id="synthetic-hours-field"),
            SimpleNamespace(name=RATE_FIELD, id="synthetic-rate-field"),
            SimpleNamespace(name="Cost Per Hour", id=PROGRAMS.cost_per_hour,
                            options=SimpleNamespace(referenced_field_ids=["synthetic-hours-field", "synthetic-rate-field"])),
            SimpleNamespace(name="Total Spend", id=PROGRAMS.total_spend,
                            options=SimpleNamespace(referenced_field_ids=[SPEND_FIELD])),
        ])
    def batch_update(self, records):
        self.records = records
        return records


def test_writer_preserves_explicit_nulls_and_only_writes_owned_fields():
    table = FakeTable()
    updates = build_true_spend_updates(programs("unmatched"), costs())
    assert write_true_spend_updates(table, updates) == 1
    assert table.records == [{"id": "unmatched", "fields": {
        SPEND_FIELD: None, "synthetic-hours-field": None, "synthetic-rate-field": None,
    }}]


def test_missing_schema_blocks_writes():
    table = FakeTable(missing=True)
    with pytest.raises(ValueError, match="approved Airtable field"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
    assert table.records is None


def test_legacy_formulas_block_cutover_before_any_writes():
    table = FakeTable()
    schema = table.schema(force=True)
    schema.fields[-1].options.referenced_field_ids.append("synthetic-postage-field")
    table.schema = lambda **kwargs: schema
    with pytest.raises(ValueError, match="formulas"):
        write_true_spend_updates(table, build_true_spend_updates(programs("record-a"), costs()))
    assert table.records is None
