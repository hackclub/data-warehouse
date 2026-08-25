"""Generate Python/YAML code for Airtable-backed programs matching orpheus-engine patterns."""

from __future__ import annotations
import re

from .dau_sql import (
    DauSource,
    Flow,
    GeneratorError,
    Quoted,
    custom_hourly,
    dump_source,
    ht_claims,
    opt,
    program_window,
    py_str,
)


class AirtableCodeGenerator:
    def __init__(self, wizard_state: dict):
        self.state = wizard_state
        self.program = wizard_state.get("program_name", "")
        self.base_id = wizard_state.get("airtable_base_id", "")

    def generate(self) -> dict:
        tables = self._selected_tables()
        self._reject_sensitive(tables)
        src = self._dau_source()
        dau = self.state.get("dau_config") or {}

        custom_sql = (opt(dau, "custom_sql") or "").strip() if dau.get("use_custom_sql") else ""

        pieces = {
            "source_type": "airtable",
            "base_config": self._base_config(tables),
            "dlt_sync_assets": self._dlt_sync_call(tables),
            "sources_yml": self._sources_yml(tables),
            "table_names": [self._table_name(key) for key in tables],
            "airtable_schema": [
                info for key in tables if (info := self._table_info(key))
            ],
            "dau_program_window": program_window(dau, src),
            # Custom SQL replaces these CTEs outright, so building them would only
            # validate answers the user's SQL makes irrelevant — and reject wizards
            # the DAU step considers complete.
            "dau_ht_claims": None if custom_sql else ht_claims(dau, src),
            "dau_custom_hourly": None if custom_sql else custom_hourly(dau, src),
        }

        if custom_sql:
            pieces["dau_sql"] = "\n\n".join(
                filter(None, [pieces["dau_program_window"], custom_sql])
            ) or None
        else:
            pieces["dau_sql"] = "\n\n".join(
                filter(None, [
                    pieces["dau_program_window"],
                    pieces["dau_ht_claims"],
                    pieces["dau_custom_hourly"],
                ])
            ) or None

        return pieces

    def _dau_source(self) -> DauSource:
        return DauSource(
            program=self.program,
            source_name=f"airtable_{self.program}",
            columns_for=self._columns,
            tables=self._selected_tables(),
            table_name=self._table_name,
            column_name=sanitize_name,
        )

    def _table_info(self, key: str) -> dict | None:
        """Resolve a "schema.table" or bare "table" key against the introspected base."""
        schema = self.state.get("schema") or []
        exact = next((t for t in schema if t["name"] == key), None)
        if exact or "." not in key:
            return exact
        base, name = key.split(".", 1)
        return next(
            (t for t in schema if t["name"] == name and (t.get("schema") or "airtable") == base),
            None,
        )

    def _source_name(self, key: str) -> str:
        info = self._table_info(key)
        return info["name"] if info else key.split(".", 1)[-1]

    def _ident(self, key: str) -> tuple[str, str]:
        info = self._table_info(key)
        if info:
            return info.get("schema") or "airtable", info["name"]
        if "." in key:
            schema, name = key.split(".", 1)
            return schema, name
        return "airtable", key

    def _table_name(self, key: str) -> str:
        return sanitize_name(self._source_name(key))

    def _columns(self, key: str) -> list[dict]:
        info = self._table_info(key)
        return info.get("columns", []) if info else []

    def _for_key(self, mapping: dict, key: str):
        """Look a table key up in a per-table map that may be keyed either way."""
        if key in mapping:
            return mapping[key]
        info = self._table_info(key)
        if not info:
            return None
        qualified = f'{info.get("schema") or "airtable"}.{info["name"]}'
        for alt in (qualified, info["name"]):
            if alt in mapping:
                return mapping[alt]
        return None

    def _selected_tables(self) -> list[str]:
        keys: list[str] = []
        claimed: dict[str, str] = {}
        for key in self.state.get("selected_tables") or []:
            if key in keys:
                continue
            name = self._table_name(key)
            if name in claimed:
                raise GeneratorError(
                    f'"{claimed[name]}" and "{key}" both sanitize to '
                    f'"{name}"; rename one in Airtable or select only one of them.',
                    step="wizard.tables",
                )
            claimed[name] = key
            keys.append(key)
        return keys

    def _reject_sensitive(self, tables: list[str]) -> None:
        """Refuse to generate a sync that would carry fields the user excluded.

        AirtableTableConfig carries a table_id and nothing else, and the source
        asset calls get_all_records_as_polars without its `fields` allowlist, so
        an Airtable sync is all-fields or no table. There is no exclusion to
        generate, which leaves refusing.
        """
        sensitive = self.state.get("sensitive_columns") or {}
        leaked = {
            self._source_name(key): list(fields)
            for key in tables
            if (fields := self._for_key(sensitive, key))
        }
        if not leaked:
            return

        detail = "; ".join(
            f'{table}: {", ".join(fields)}' for table, fields in sorted(leaked.items())
        )
        raise GeneratorError(
            "The Airtable loader syncs every field of every table it is given — there "
            "is no per-field allowlist to generate — so these fields cannot be excluded "
            f"the way the table picker says they are: {detail}. Deselect those tables, "
            "delete the fields from the base, or click each field in the table picker to "
            "include it deliberately.",
            step="wizard.tables",
        )

    def _base_config(self, tables: list[str]) -> str:
        lines = [
            f'        {py_str(self.program)}: AirtableBaseConfig(',
            f'            base_id={py_str(self.base_id)},',
            f'            tables={{',
        ]
        for key in tables:
            info = self._table_info(key) or {}
            table_id = opt(info, "table_id", "tblXXXXXXXXXXXXXX")
            lines.append(
                f'                {py_str(self._table_name(key))}: AirtableTableConfig('
            )
            lines.append(f'                    table_id={py_str(table_id)}')
            lines.append(f'                ),')
        lines.extend([
            f'            }}',
            f'        ),',
        ])
        return "\n".join(lines)

    def _dlt_sync_call(self, tables: list[str]) -> str:
        table_list = ", ".join(py_str(self._table_name(k)) for k in tables)
        description = py_str(
            f"Loads {self.program} data into the "
            f"warehouse.airtable_{self.program} schema."
        )
        return (
            f'{self.program}_assets = create_airtable_sync_assets(\n'
            f'    base_name={py_str(self.program)},\n'
            f'    tables=[{table_list}],\n'
            f'    description={description}\n'
            f')'
        )

    def _sources_yml(self, tables: list[str]) -> str:
        source_name = f"airtable_{self.program}"
        title = " ".join(w.capitalize() for w in self.program.split("_"))
        source = {
            "name": source_name,
            "schema": source_name,
            "tables": [
                {
                    "name": self._table_name(key),
                    "description": Quoted(f"{title} {self._table_name(key)} table."),
                    "meta": Flow(
                        dagster=Flow(
                            deps=[f"{self.program}_{self._table_name(key)}_warehouse"]
                        )
                    ),
                }
                for key in tables
            ],
        }
        return dump_source(source)


def sanitize_name(name: str) -> str:
    """Identical to orpheus_engine scripts/generate_airtable_ids.py::sanitize_name.

    That script names the generated_ids class and field constants the dlt sync
    asset looks up, so the warehouse table/column names follow it exactly.
    """
    s = re.sub(r"\W|^(?=\d)", "_", name)
    s = re.sub(r"([A-Z]+)", r"_\1", s).lower().strip("_")
    s = re.sub(r"_+", "_", s)
    if s and s[0].isdigit():
        s = f"_{s}"
    return s
