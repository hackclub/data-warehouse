"""Generate Python/YAML/SQL code that matches existing orpheus-engine patterns exactly."""

from __future__ import annotations

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
    py_list,
    py_str,
)


class CodeGenerator:
    def __init__(self, wizard_state: dict):
        self.state = wizard_state
        self.program = wizard_state.get("program_name", "")
        self.program_upper = self.program.upper()
        self.program_title = _to_title(self.program)
        self.target_schema = opt(wizard_state, "target_schema", self.program)
        self._names: dict[tuple[str, str], str] | None = None

    def generate(self) -> dict:
        src = self._dau_source()
        dau = self.state.get("dau_config") or {}

        custom_sql = (opt(dau, "custom_sql") or "").strip() if dau.get("use_custom_sql") else ""

        pieces = {
            "source_type": "postgres",
            "env_var_name": f"{self.program_upper}_DATABASE_URL",
            "env_var_list_entry": f'    "{self.program_upper}_DATABASE_URL",',
            "connection_resource": self._connection_resource(),
            "connection_variable_name": f"{self.program}_db_connection",
            "replication_config": self._replication_config(),
            "asset_function": self._asset_function(),
            "definitions_import": f"{self.program}_warehouse_mirror",
            "definitions_asset_entry": f"    {self.program}_warehouse_mirror,",
            "sources_yml": self._sources_yml(),
            "table_names": [self._table_name(key) for key in self._selected_tables()],
            "dau_program_window": program_window(dau, src),
            # Custom SQL replaces these CTEs outright, so building them would only
            # validate answers the user's SQL makes irrelevant — and reject wizards
            # the DAU step considers complete.
            "dau_ht_claims": None if custom_sql else ht_claims(dau, src),
            "dau_custom_hourly": None if custom_sql else custom_hourly(dau, src),
        }

        pieces["assets_py"] = "\n\n".join(
            filter(None, [
                f"# Added to _SLING_CONNECTION_URL_ENV_VARS:\n{pieces['env_var_list_entry']}",
                pieces["connection_resource"],
                pieces["replication_config"],
                pieces["asset_function"],
            ])
        )

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
            source_name=self.program,
            columns_for=self._columns,
            tables=self._selected_tables(),
            table_name=self._table_name,
        )

    def _table_info(self, key: str) -> dict | None:
        """Resolve a "schema.table" or bare "table" key against the introspected schema."""
        schema = self.state.get("schema") or []
        exact = next((t for t in schema if t["name"] == key), None)
        if exact or "." not in key:
            return exact
        src_schema, name = key.split(".", 1)
        return next(
            (
                t for t in schema
                if t["name"] == name and (t.get("schema") or "public") == src_schema
            ),
            None,
        )

    def _ident(self, key: str) -> tuple[str, str]:
        """The (schema, table) a "schema.table" or bare "table" key names at the source."""
        info = self._table_info(key)
        if info:
            return info.get("schema") or "public", info["name"]
        if "." in key:
            schema, name = key.split(".", 1)
            return schema, name
        return "public", key

    def _table_schema(self, key: str) -> str:
        return self._ident(key)[0]

    def _source_table(self, key: str) -> str:
        return self._ident(key)[1]

    def _warehouse_names(self) -> dict[tuple[str, str], str]:
        """Source table -> the name it lands under, schema-prefixed where two collide.

        Sling writes every stream to `{target_schema}.{stream_table}`, so
        public.users and analytics.users would overwrite each other; the parent
        overrides `object` per stream for exactly this (auth_replication_config).
        """
        if self._names is None:
            idents = [self._ident(key) for key in self._selected_tables()]
            clashing = {
                name for name in (n for _, n in idents)
                if sum(1 for _, other in idents if other == name) > 1
            }
            self._names = {
                (schema, name): f"{schema}_{name}" if name in clashing else name
                for schema, name in idents
            }
            landed = list(self._names.values())
            dupes = [n for n in landed if landed.count(n) > 1]
            if dupes:
                raise GeneratorError(
                    f"{', '.join(sorted(set(dupes)))} would be written by more than one "
                    f"source table even after qualifying by schema; select only one."
                )
        return self._names

    def _table_name(self, key: str) -> str:
        ident = self._ident(key)
        return self._warehouse_names().get(ident, ident[1])

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
        qualified = f'{info.get("schema") or "public"}.{info["name"]}'
        for alt in (qualified, info["name"]):
            if alt in mapping:
                return mapping[alt]
        return None

    def _selected_tables(self) -> list[str]:
        keys: list[str] = []
        seen: set[tuple[str, str]] = set()
        for key in self.state.get("selected_tables") or []:
            ident = self._ident(key)
            if ident in seen:
                continue
            seen.add(ident)
            keys.append(key)
        return keys

    def _connection_resource(self) -> str:
        return (
            f'{self.program}_db_connection = SlingConnectionResource(\n'
            f'    name="{self.program_upper}_DB",\n'
            f'    type="postgres",\n'
            f'    connection_string=EnvVar("{self.program_upper}_DATABASE_URL"),\n'
            f')'
        )

    def _replication_config(self) -> str:
        lines = [
            f'{self.program}_replication_config = {{',
            f'    "source": "{self.program_upper}_DB",',
            f'    "target": "WAREHOUSE_DB",',
            f'',
            f'    "defaults": {{',
            f'        "mode": "full-refresh",',
            f'        "object": {py_str(f"{self.target_schema}.{{stream_table}}")},',
            f'    }},',
            f'',
            f'    "streams": {{',
        ]

        for key in self._selected_tables():
            lines.extend(self._stream_entry(key))

        lines.extend([
            f'    }},',
            f'}}',
        ])

        return "\n".join(lines)

    def _asset_function(self) -> str:
        configs = self.state.get("sync_configs") or {}
        has_incremental = any(
            (self._for_key(configs, key) or {}).get("mode") == "incremental"
            for key in self._selected_tables()
        )

        lines = [
            f'@dg.asset(',
            f'    name="{self.program}_warehouse_mirror",',
            f'    group_name="sling",',
            f'    compute_kind="sling",',
            f')',
            f'def {self.program}_warehouse_mirror(',
            f'    context: dg.AssetExecutionContext,',
            f'    sling: SlingResource,',
            f') -> Nothing:',
            f'    """Replicates the entire {self.program_title} DB → warehouse in a single shot."""',
            f'    context.log.info("Starting {self.program_title} → warehouse Sling replication")',
        ]

        if has_incremental:
            lines.append(
                f'    _ensure_incremental_target_indexes(context, {self.program}_replication_config)'
            )

        lines.extend([
            f'',
            f'    for _ in sling.replicate(',
            f'        context=context,',
            f'        replication_config={self.program}_replication_config,',
            f'    ):',
            f'        pass',
            f'',
            f'    context.log.info("Replication finished")',
            f'    context.add_output_metadata({{"replicated": True}})',
            f'    return None',
        ])

        return "\n".join(lines)

    def _sources_yml(self) -> str:
        keys = self._selected_tables()
        has_camel = any(
            any(c.isupper() for c in self._table_name(key))
            or any(
                any(c.isupper() for c in col["name"]) for col in self._columns(key)
            )
            for key in keys
        )

        source = {"name": self.program, "schema": self.target_schema}
        if has_camel:
            source["quoting"] = {"identifier": True}
        source["tables"] = [
            {
                "name": self._table_name(key),
                "description": Quoted(
                    f"{self.program_title} {self._table_name(key)} table."
                ),
                "meta": Flow(
                    dagster=Flow(deps=[f"{self.program}_warehouse_mirror"])
                ),
            }
            for key in keys
        ]

        return dump_source(source)

    def _stream_entry(self, key: str) -> list[str]:
        sensitive = self._for_key(self.state.get("sensitive_columns") or {}, key) or []
        config = self._for_key(self.state.get("sync_configs") or {}, key) or {}

        all_cols = [c["name"] for c in self._columns(key)]
        safe_cols = [c for c in all_cols if c not in sensitive]
        source_table = self._source_table(key)
        warehouse_table = self._table_name(key)
        stream = py_str(f"{self._table_schema(key)}.{source_table}")

        has_select = len(safe_cols) < len(all_cols)
        is_incremental = config.get("mode") == "incremental"
        renamed = warehouse_table != source_table
        pk = config.get("primary_key") if is_incremental else None
        uk = config.get("update_key") if is_incremental else None

        if not has_select and not is_incremental and not renamed:
            return [f'        {stream}: None,']

        indent = " " * 12
        lines = [f'        {stream}: {{']

        if renamed:
            lines.append(
                f'{indent}"object": {py_str(f"{self.target_schema}.{warehouse_table}")},'
            )
        if is_incremental:
            lines.append(f'{indent}"mode": "incremental",')
        if pk:
            lines.append(f'{indent}"primary_key": {py_list(pk)},')
        if uk:
            lines.append(f'{indent}"update_key": {py_str(uk)},')
        if has_select:
            lines.append(f'{indent}"select": [')
            chunk = []
            line_len = 0
            for col in safe_cols:
                item = py_str(col)
                if line_len + len(item) > 60 and chunk:
                    lines.append(f'{indent}    {", ".join(chunk)},')
                    chunk = []
                    line_len = 0
                chunk.append(item)
                line_len += len(item) + 2
            if chunk:
                lines.append(f'{indent}    {", ".join(chunk)},')
            lines.append(f'{indent}],')

        lines.append(f'        }},')
        return lines


def _to_title(snake: str) -> str:
    return " ".join(w.capitalize() for w in snake.split("_"))
