from types import SimpleNamespace

from orpheus_engine.defs.airtable.resources import AirtableResource


def _kinds_for(fields):
    schema_info = SimpleNamespace(fields=fields)
    return AirtableResource._declared_scalar_kinds(None, schema_info)


def _field(field_id, field_type, options=None):
    return SimpleNamespace(id=field_id, type=field_type, options=options)


def _result_options(result_type):
    """Typed pydantic-style options: options.result.type"""
    return SimpleNamespace(result=SimpleNamespace(type=result_type))


def test_direct_scalar_types_are_pinned():
    kinds = _kinds_for(
        [
            _field("fld_num", "number"),
            _field("fld_cur", "currency"),
            _field("fld_pct", "percent"),
            _field("fld_dur", "duration"),
            _field("fld_cnt", "count"),
            _field("fld_auto", "autoNumber"),
            _field("fld_rate", "rating"),
            _field("fld_chk", "checkbox"),
        ]
    )
    assert kinds == {
        "fld_num": "float",
        "fld_cur": "float",
        "fld_pct": "float",
        "fld_dur": "float",
        "fld_cnt": "integer",
        "fld_auto": "integer",
        "fld_rate": "integer",
        "fld_chk": "boolean",
    }


def test_wrapper_types_resolve_declared_result_type():
    kinds = _kinds_for(
        [
            _field("fld_formula_num", "formula", _result_options("number")),
            _field("fld_rollup_cur", "rollup", _result_options("currency")),
            _field("fld_formula_chk", "formula", _result_options("checkbox")),
            _field("fld_lookup_num", "multipleLookupValues", _result_options("number")),
        ]
    )
    assert kinds == {
        "fld_formula_num": "float",
        "fld_rollup_cur": "float",
        "fld_formula_chk": "boolean",
        "fld_lookup_num": "float",
    }


def test_wrapper_types_with_dict_options():
    # pyairtable's UnknownFieldSchema fallback keeps options as a plain dict
    kinds = _kinds_for(
        [
            _field("fld_rollup", "rollup", {"result": {"type": "number", "options": {"precision": 1}}}),
            _field("fld_formula", "formula", {"result": {"type": "checkbox"}}),
            _field("fld_no_result", "formula", {}),
        ]
    )
    assert kinds == {
        "fld_rollup": "float",
        "fld_formula": "boolean",
    }


def test_non_scalar_types_fall_back_to_value_inference():
    kinds = _kinds_for(
        [
            _field("fld_text", "singleLineText"),
            _field("fld_formula_text", "formula", _result_options("singleLineText")),
            _field("fld_lookup_date", "multipleLookupValues", _result_options("date")),
            _field("fld_links", "multipleRecordLinks"),
            _field("fld_formula_no_result", "formula", None),
        ]
    )
    assert kinds == {}
