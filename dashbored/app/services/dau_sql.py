"""Shared quoting, sources.yml and DAU CTE emitters for the sling/Airtable generators.

Both generators emit the same CTEs into the same dbt model; the only differences
are which dbt source the tables live in and how table/column names are spelled
once they land in the warehouse.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Callable

import yaml

# looksLikeId() in frontend/components/DauWizard.svelte mirrors is_id_column();
# this module decides, and refuses rather than guessing when the wizard disagreed
# and so never asked which table maps ids to emails.
ID_COLUMN_TYPES = {"int4", "int8", "integer", "bigint", "serial"}

EMAIL_COLUMN_HINTS = ("email", "mail")


class GeneratorError(Exception):
    """A wizard selection the generator cannot honour; the message is user-facing.

    `step` is the wizard endpoint the selection was made on, so the flash lands
    somewhere the user can actually act on it — a sensitive field is un-picked on
    the tables step, not the DAU one.
    """

    def __init__(self, message, step="wizard.dau"):
        super().__init__(message)
        self.step = step


def opt(mapping: dict, key: str, default=None):
    """dict.get that treats a present-but-empty value as absent."""
    value = mapping.get(key)
    return default if value in (None, "") else value


def py_str(value) -> str:
    """A double-quoted Python string literal for `value`."""
    return json.dumps(str(value), ensure_ascii=False)


def py_list(items) -> str:
    return "[" + ", ".join(py_str(i) for i in items) + "]"


def sql_ident(name) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sql_str(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def jinja_str(value) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


class Flow(dict):
    """A mapping yaml.dump renders inline, the way sources.yml writes `meta`."""


class Quoted(str):
    """A string yaml.dump always double-quotes, the way sources.yml writes prose."""


class _SourcesDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


_SourcesDumper.add_representer(
    Flow,
    lambda dumper, data: dumper.represent_mapping(
        "tag:yaml.org,2002:map", data, flow_style=True
    ),
)

_SourcesDumper.add_representer(
    Quoted,
    lambda dumper, data: dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"'),
)


def dump_source(source: dict) -> str:
    """Render one dbt `sources:` list entry, indented to sit under `sources:`."""
    text = yaml.dump(
        [source],
        Dumper=_SourcesDumper,
        sort_keys=False,
        allow_unicode=True,
        width=10**6,
    )
    return "\n".join(
        f"  {line}" if line else line for line in text.rstrip().split("\n")
    )


@dataclass
class DauSource:
    """How one program's tables are addressed from the dbt DAU model."""

    program: str
    source_name: str
    columns_for: Callable[[str], list[dict]]
    tables: list[str] = field(default_factory=list)
    table_name: Callable[[str], str] = lambda t: t
    column_name: Callable[[str], str] = lambda c: c

    def ref(self, table: str) -> str:
        return (
            "{{ source("
            f"{jinja_str(self.source_name)}, {jinja_str(self.table_name(table))}"
            ") }}"
        )

    def col(self, alias: str, column: str) -> str:
        return f"{alias}.{sql_ident(self.column_name(column))}"

    def declared(self, table: str) -> bool:
        """Is `table` one of the tables this program's sources.yml declares?"""
        name = self.table_name(table)
        return any(self.table_name(t) == name for t in self.tables)

    def is_id_column(self, table: str, column: str) -> bool:
        """True when `column` keys rows by user id rather than by email.

        Both the source spelling and the warehouse one count, because Airtable's
        "User ID" only looks like an id after sanitize_name. The type check reads
        the introspected column under its source spelling and lowercases
        udt-or-type before comparing it to ID_COLUMN_TYPES.

        Text-typed columns ending in _id (slack_id, hackatime_id, …) are natural
        identifiers, not foreign keys — they only count as IDs when their database
        type is integer/bigint/serial/uuid.
        """
        if not column:
            return False
        col = next((c for c in self.columns_for(table) if c["name"] == column), None)
        col_type = (col.get("udt") or col.get("type") or "").lower() if col else ""
        is_id_type = col_type in ID_COLUMN_TYPES or col_type in ("uuid",)
        for spelling in (column, self.column_name(column)):
            if spelling == "id":
                return True
            if spelling.endswith("_id"):
                return not col or is_id_type
        return is_id_type

    def id_column(self, table: str) -> str:
        """The column an id-typed user column points at; Airtable always lands `id`."""
        return next(
            (
                c["name"] for c in self.columns_for(table)
                if self.column_name(c["name"]) == "id"
            ),
            "id",
        )

    def email_column(self, table: str) -> str | None:
        return next(
            (
                c["name"] for c in self.columns_for(table)
                if any(hint in c["name"].lower() for hint in EMAIL_COLUMN_HINTS)
            ),
            None,
        )


def email_normalize_expr(col_expr: str) -> str:
    return (
        f"CASE WHEN POSITION('@' IN LOWER(BTRIM({col_expr}))) > 0\n"
        f"             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM({col_expr})), '@', 1), '+', 1)\n"
        f"                  || '@' || SPLIT_PART(LOWER(BTRIM({col_expr})), '@', 2)\n"
        f"             ELSE SPLIT_PART(LOWER(BTRIM({col_expr})), '+', 1)\n"
        f"        END"
    )


def duration_expr(dau: dict, src: DauSource) -> str | None:
    """Hours-per-row expression, or None when rows carry no duration."""
    column = opt(dau, "duration_column")
    if not column:
        return None
    ref = src.col("a", column)
    unit = opt(dau, "duration_unit", "seconds")
    if unit == "minutes":
        return f"({ref} / 60.0)"
    if unit == "hours":
        return ref
    return f"({ref} / 3600.0)"


def _email_source(dau: dict, src: DauSource, keys: dict, table: str) -> tuple[str, str]:
    """The (table, column) holding emails for an id-keyed user column.

    Only tables this program's sources.yml declares are usable: anything else
    compiles to a dbt source that does not exist. Every failure here is a
    question for the user — a JOIN they never picked is not a default.
    """
    user_ref = f"{src.table_name(table)}.{src.column_name(opt(dau, keys['user']))}"
    chosen = opt(dau, keys["email_table"])
    if not chosen:
        raise GeneratorError(
            f"{user_ref} holds user ids, so the DAU model has to join to a table with "
            f"an email column, and the wizard was not given one. Pick the {src.program} "
            f"table that maps those ids to emails, or key the activity on an email "
            f"column instead."
        )

    if not src.declared(chosen):
        raise GeneratorError(
            f'"{chosen}" was picked as the email table for {user_ref}, but it is not '
            f"one of the {src.program} tables being synced. Select it as well, or pick "
            f"a table that is."
        )

    column = opt(dau, keys["email_column"]) or src.email_column(chosen)
    if not column:
        raise GeneratorError(
            f'"{chosen}" was picked as the email table for {user_ref}, but it has no '
            f"email column to join on. Pick a table that has one, or key the activity "
            f"on an email column instead."
        )
    return chosen, column


def _user_email(dau: dict, src: DauSource, table: str, alias: str, keys: dict) -> tuple[str, str]:
    """(user_email expression, JOIN clause) for a table keyed by email or by id."""
    user_col = opt(dau, keys["user"])
    if not src.is_id_column(table, user_col):
        return email_normalize_expr(src.col(alias, user_col)), ""

    email_table, email_col = _email_source(dau, src, keys, table)
    join = (
        f"    JOIN {src.ref(email_table)} u "
        f"ON {src.col('u', src.id_column(email_table))} = {src.col(alias, user_col)}\n"
    )
    return email_normalize_expr(src.col("u", email_col)), join


def _ts_literal(date_str: str, tz: str | None) -> str:
    suffix = f" {tz}" if tz else "+00"
    return f"TIMESTAMP WITH TIME ZONE {sql_str(f'{date_str} 00:00:00{suffix}')}"


def program_window(dau: dict, src: DauSource) -> str | None:
    if not dau:
        return None

    start = opt(dau, "start_date")
    if not start:
        return None

    tz = opt(dau, "timezone")
    end = opt(dau, "end_date")
    end_expr = _ts_literal(end, tz) if end else "NULL::timestamptz"

    return (
        f"-- program_windows entry:\n"
        f"({sql_str(src.program)}, {_ts_literal(start, tz)},\n"
        f"                   {end_expr}),"
    )


def ht_claims(dau: dict, src: DauSource) -> str | None:
    if not dau or not dau.get("uses_hackatime"):
        return None

    mapping_table = opt(dau, "ht_mapping_table")
    user_col = opt(dau, "ht_user_column")
    alias_col = opt(dau, "ht_alias_column")
    if not mapping_table or not user_col or not alias_col:
        return None

    if not src.declared(mapping_table):
        raise GeneratorError(
            f'"{mapping_table}" is the Hackatime mapping table but is not one of the '
            f"{src.program} tables being synced. Select it as well, or pick one that is.",
            step="wizard.dau",
        )

    user_email_expr, join_clause = _user_email(
        dau, src, mapping_table, "hp",
        {"user": "ht_user_column", "email_table": "ht_email_table", "email_column": "ht_email_column"},
    )

    alias_ref = src.col("hp", alias_col)
    alias_format = opt(dau, "ht_alias_format", "single")
    if alias_format == "single":
        alias_expr = f"LOWER(BTRIM({alias_ref}))"
        lateral_clause = ""
        alias_filter = f"{alias_ref} IS NOT NULL AND {alias_ref} <> ''"
    else:
        alias_expr = "LOWER(BTRIM(alias_val))"
        alias_filter = "alias_val IS NOT NULL AND alias_val <> ''"
        if alias_format == "text_array":
            unnest_fn = f"unnest({alias_ref}::text[])"
        elif alias_format == "json_array":
            unnest_fn = f"jsonb_array_elements_text({alias_ref}::jsonb)"
        else:
            unnest_fn = f"unnest(string_to_array({alias_ref}, ','))"
        lateral_clause = f"    CROSS JOIN LATERAL {unnest_fn} AS alias_val\n"

    claim_start_date = opt(dau, "claim_start_date")
    tz = opt(dau, "timezone")
    if opt(dau, "claim_start_source", "column") == "fixed_date" and claim_start_date:
        ts_expr = f"{_ts_literal(claim_start_date, tz)} AS claim_start_ts"
    else:
        claim_start_column = opt(dau, "claim_start_column", "created_at")
        ts_expr = f"{src.col('hp', claim_start_column)} AT TIME ZONE 'UTC' AS claim_start_ts"

    return (
        f"{src.program}_ht_claims AS (\n"
        f"    SELECT {sql_str(src.program)}::text AS program_name,\n"
        f"        {user_email_expr} AS user_email,\n"
        f"        {alias_expr} AS hackatime_alias,\n"
        f"        NULL::text AS project_name,\n"
        f"        NULL::text AS code_url,\n"
        f"        {ts_expr}\n"
        f"    FROM {src.ref(mapping_table)} hp\n"
        + join_clause
        + lateral_clause
        + f"    WHERE {alias_filter}\n"
        f"),"
    )


def custom_hourly(dau: dict, src: DauSource) -> str | None:
    if not dau or not dau.get("has_custom_time"):
        return None

    table = opt(dau, "custom_table")
    if not table or not opt(dau, "user_column"):
        return None

    if not src.declared(table):
        raise GeneratorError(
            f'"{table}" is the custom activity table but is not one of the '
            f"{src.program} tables being synced. Select it as well, or pick one that is.",
            step="wizard.dau",
        )

    ts_col = opt(dau, "timestamp_column", "created_at")
    dur_col = opt(dau, "duration_column")
    dur_expr = duration_expr(dau, src)
    cap_24h = dau.get("cap_24h", False) and dur_expr is not None

    user_email_expr, join_clause = _user_email(
        dau, src, table, "a",
        {"user": "user_column", "email_table": "email_table", "email_column": "email_column"},
    )

    detail = f"{src.program}.{src.table_name(table)}"
    if dur_col:
        detail += f".{src.column_name(dur_col)}"
    source_detail = f"({sql_str(detail + '; entries=')} || COUNT(*)::text) AS source_detail"

    if dur_expr is None:
        hours_expr = "0::numeric AS raw_hours_logged"
        where_clause = ""
    else:
        summed = f"LEAST({dur_expr}, 24)" if cap_24h else dur_expr
        hours_expr = f"ROUND(SUM({summed})::numeric, 4) AS raw_hours_logged"
        where_clause = f"    WHERE {src.col('a', dur_col)} > 0\n"

    name = f"{src.program}_custom_hourly{'_uncapped' if cap_24h else ''}"
    hourly = (
        f"{name} AS (\n"
        f"    SELECT\n"
        f"        DATE_TRUNC('hour', {src.col('a', ts_col)} AT TIME ZONE 'UTC') AS activity_hour,\n"
        f"        {sql_str(src.program)}::text AS program_name,\n"
        f"        {user_email_expr} AS user_email,\n"
        f"        NULL::text AS project_name,\n"
        f"        NULL::text AS code_url,\n"
        f"        {hours_expr},\n"
        f"        'custom'::text AS logging_method,\n"
        f"        {source_detail}\n"
        f"    FROM {src.ref(table)} a\n"
        + join_clause
        + where_clause
        + f"    GROUP BY 1, 2, 3, 4, 5\n"
        f"),"
    )

    if not cap_24h:
        return hourly

    capped = (
        f"{src.program}_custom_hourly AS (\n"
        f"    SELECT\n"
        f"        activity_hour,\n"
        f"        program_name,\n"
        f"        user_email,\n"
        f"        project_name,\n"
        f"        code_url,\n"
        f"        ROUND((raw_hours_logged * LEAST(1.0, 24.0 / NULLIF(\n"
        f"            SUM(raw_hours_logged) OVER (\n"
        f"                PARTITION BY user_email, activity_hour::date\n"
        f"            ), 0)))::numeric, 4) AS raw_hours_logged,\n"
        f"        logging_method,\n"
        f"        source_detail\n"
        f"    FROM {src.program}_custom_hourly_uncapped\n"
        f"),"
    )

    return hourly + "\n\n" + capped
