"""
Tests for the Hackatime application-index mirroring on the warehouse.

The specs are a hand-maintained snapshot of hackclub/hackatime's db/schema.rb;
these tests pin the invariants that keep them safe to apply blindly on every
sync: they must be well-formed, target the hackatime schema, only reference
tables the Sling replication incrementally syncs (full-refresh tables are
dropped and recreated, losing their indexes), not duplicate the PK/cursor
indexes the sync already ensures, and produce names under postgres's
63-character identifier limit.

No network or database access; pure helpers only.
"""

import re

from orpheus_engine.defs.sling.assets import (
    _HACKATIME_APP_INDEXES,
    _hackatime_app_index_specs,
    _replication_index_specs,
    _safe_index_name,
    hackatime_replication_config,
)

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def _incremental_tables() -> set[str]:
    tables = set()
    for stream, config in hackatime_replication_config["streams"].items():
        if (config or {}).get("mode") == "incremental":
            tables.add(stream.split(".", 1)[-1])
    return tables


def test_app_index_tables_are_incrementally_synced():
    assert set(_HACKATIME_APP_INDEXES) <= _incremental_tables()


def test_app_index_specs_are_well_formed():
    specs = _hackatime_app_index_specs()
    assert specs
    for schema_name, table_name, columns in specs:
        assert schema_name == "hackatime"
        assert _IDENT.match(table_name)
        assert columns
        assert all(_IDENT.match(column) for column in columns)
        assert len(set(columns)) == len(columns)


def test_app_index_specs_do_not_repeat_sync_indexes():
    derived = set(_replication_index_specs(hackatime_replication_config))
    assert not derived & set(_hackatime_app_index_specs())


def test_app_index_names_fit_postgres_identifier_limit():
    for schema_name, table_name, columns in _hackatime_app_index_specs():
        assert len(_safe_index_name(schema_name, table_name, *columns)) <= 63
