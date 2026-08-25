"""Apply the dashbored transforms to the real orpheus-engine files on disk.

Run from dashbored/: `pytest test_transforms.py`. pytest puts this file's
directory on sys.path itself, so `app` imports without any help from us.
Collected from anywhere else (e.g. a bare pytest at the monorepo root, where
libcst and flask are not installed) the module skips instead of erroring.
"""

import ast
import builtins
from pathlib import Path

import pytest

pytest.importorskip(
    "app.services.github_pr_creator",
    reason="dashbored-only deps (libcst, flask); run pytest from dashbored/",
)

import yaml

from app.services.airtable_code_generator import AirtableCodeGenerator, sanitize_name
from app.services.code_generator import CodeGenerator
from app.services.dau_sql import GeneratorError
from app.services.github_pr_creator import (
    AIRTABLE_DEFINITIONS_PATH,
    DLT_ASSETS_PATH,
    GENERATED_IDS_PATH,
    SOURCES_YML_PATH,
    AirtablePrCreator,
    ConflictError,
    check_conflicts,
    patch_airtable_definitions,
    patch_assets_py,
    patch_definitions_py,
    patch_dlt_assets,
    patch_generated_ids,
    patch_sources_yml,
    patch_dau_sql,
    validate_locally,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROGRAM = "test_program"

# Both patched files already carry this base, so a second one is a repoint of a
# production sync, not an addition.
EXISTING_PROGRAM = "flavortown"

REFUSALS = (ConflictError, GeneratorError)

WIZARD_STATE = {
    "program_name": PROGRAM,
    "selected_tables": ["users", "projects", "devlogs"],
    "sensitive_columns": {"users": ["password_digest", "reset_token"]},
    "sync_configs": {
        "users": {"mode": "full-refresh"},
        "projects": {"mode": "incremental", "primary_key": ["id"], "update_key": "updated_at"},
        "devlogs": {"mode": "incremental", "primary_key": ["id"], "update_key": "created_at"},
    },
    "schema": [
        {
            "name": "users",
            "columns": [
                {"name": "id", "type": "integer", "udt": "int4"},
                {"name": "email", "type": "text", "udt": "text"},
                {"name": "hackatime_alias", "type": "text", "udt": "text"},
                {"name": "password_digest", "type": "text", "udt": "text"},
                {"name": "reset_token", "type": "text", "udt": "text"},
                {"name": "created_at", "type": "timestamp", "udt": "timestamptz"},
            ],
            "primary_key": ["id"],
            "row_count": 500,
        },
        {
            "name": "projects",
            "columns": [
                {"name": "id", "type": "integer", "udt": "int4"},
                {"name": "title", "type": "text", "udt": "text"},
                {"name": "repo_url", "type": "text", "udt": "text"},
                {"name": "updated_at", "type": "timestamp", "udt": "timestamptz"},
            ],
            "primary_key": ["id"],
            "row_count": 150000,
        },
        {
            "name": "devlogs",
            "columns": [
                {"name": "id", "type": "integer", "udt": "int4"},
                {"name": "user_email", "type": "text", "udt": "text"},
                {"name": "duration_seconds", "type": "integer", "udt": "int4"},
                {"name": "created_at", "type": "timestamp", "udt": "timestamptz"},
            ],
            "primary_key": ["id"],
            "row_count": 80000,
        },
    ],
    "dau_config": {
        "uses_hackatime": True,
        "ht_mapping_table": "users",
        "ht_user_column": "email",
        "ht_alias_column": "hackatime_alias",
        "ht_alias_format": "single",
        "claim_start_source": "column",
        "claim_start_column": "created_at",
        "has_custom_time": True,
        "custom_table": "devlogs",
        "timestamp_column": "created_at",
        "user_column": "user_email",
        "duration_column": "duration_seconds",
        "duration_unit": "seconds",
        "start_date": "2026-06-01",
    },
}



def airtable_state(program=PROGRAM, **overrides):
    """An Airtable wizard state whose table and field names need sanitizing."""
    state = {
        "program_name": program,
        "source_type": "airtable",
        "airtable_base_id": "appAAAAAAAAAAAAAA",
        "airtable_base_name": "Test Program",
        "selected_tables": ["Users", "Time Logs"],
        "sensitive_columns": {},
        "schema": [
            {
                "name": "Users",
                "table_id": "tblAAAAAAAAAAAAAA",
                "schema": "airtable",
                "columns": [
                    {"name": "Email", "type": "text", "udt": "email", "field_id": "fldA1"},
                    {"name": "Hackatime Alias", "type": "text", "udt": "singleLineText", "field_id": "fldA2"},
                    {"name": "Secret Token", "type": "text", "udt": "singleLineText", "field_id": "fldA3"},
                    {"name": "Created At", "type": "text", "udt": "createdTime", "field_id": "fldA4"},
                ],
                "primary_key": ["id"],
            },
            {
                "name": "Time Logs",
                "table_id": "tblBBBBBBBBBBBBBB",
                "schema": "airtable",
                "columns": [
                    {"name": "User Email", "type": "text", "udt": "email", "field_id": "fldB1"},
                    {"name": "Hours Logged", "type": "double", "udt": "number", "field_id": "fldB2"},
                    {"name": "Logged At", "type": "text", "udt": "createdTime", "field_id": "fldB3"},
                ],
                "primary_key": ["id"],
            },
        ],
        "dau_config": {
            "uses_hackatime": True,
            "ht_mapping_table": "Users",
            "ht_user_column": "Email",
            "ht_alias_column": "Hackatime Alias",
            "ht_alias_format": "single",
            "claim_start_source": "column",
            "claim_start_column": "Created At",
            "has_custom_time": True,
            "custom_table": "Time Logs",
            "timestamp_column": "Logged At",
            "user_column": "User Email",
            "duration_column": "Hours Logged",
            "duration_unit": "hours",
            "start_date": "2026-06-01",
        },
    }
    state.update(overrides)
    return state


def repo_file(relpath):
    path = REPO_ROOT / relpath
    if not path.exists():
        pytest.skip(f"{relpath} is not in this checkout")
    return path.read_text()


@pytest.fixture(scope="module")
def generated():
    return CodeGenerator(WIZARD_STATE).generate()


@pytest.fixture(scope="module")
def assets_source():
    return repo_file("orpheus_engine/defs/sling/assets.py")


@pytest.fixture(scope="module")
def definitions_source():
    return repo_file("orpheus_engine/defs/sling/definitions.py")


@pytest.fixture(scope="module")
def sources_source():
    return repo_file("orpheus_engine_dbt/models/sources.yml")


@pytest.fixture(scope="module")
def dau_source():
    return repo_file(
        "orpheus_engine_dbt/models/summer_2026_analytics/summer_unified_time_log.sql"
    )


@pytest.fixture(scope="module")
def airtable_generated():
    return AirtableCodeGenerator(airtable_state()).generate()


@pytest.fixture(scope="module")
def airtable_definitions_source():
    return repo_file(AIRTABLE_DEFINITIONS_PATH)


@pytest.fixture(scope="module")
def dlt_assets_source():
    return repo_file(DLT_ASSETS_PATH)


def assigned_value(source, name):
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return node.value
    raise AssertionError(f"{name} is not assigned at module level")


def assignment_order(source, name):
    for i, node in enumerate(ast.parse(source).body):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return i
    raise AssertionError(f"{name} is not assigned at module level")


def call_kwarg_names(call, kwarg):
    for kw in call.keywords:
        if kw.arg == kwarg:
            return [e.id for e in kw.value.elts if isinstance(e, ast.Name)]
    raise AssertionError(f"no {kwarg}= in call")


def call_kwarg(call, kwarg):
    for kw in call.keywords:
        if kw.arg == kwarg:
            return kw.value
    raise AssertionError(f"no {kwarg}= in call")


def dict_entry(node, key):
    for k, v in zip(node.keys, node.values):
        if isinstance(k, ast.Constant) and k.value == key:
            return v
    raise AssertionError(f"no {key!r} key")


def dict_keys(node):
    """Keys as written, so a duplicate one shows up instead of collapsing."""
    return [ast.literal_eval(k) for k in node.keys]


def undefined_names(source):
    """Names the module loads that nothing anywhere in it ever binds.

    Deliberately scope-blind: a generated statement referring to a collector the
    parent file never defines is the failure worth catching, and it survives any
    amount of leniency about where a name was bound.
    """
    tree = ast.parse(source)
    bound = {"__name__", "__file__", "__doc__", *dir(builtins)}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, ast.Import):
            bound.update((a.asname or a.name).split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            bound.update(a.asname or a.name for a in node.names)
        elif isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
            bound.add(node.id)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            bound.update(node.names)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
    return {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    } - bound


def airtable_base_entries(source):
    """[(base_name, base_id, {table: {kwarg: literal}})] as written, dupes and all."""
    bases = call_kwarg(assigned_value(source, "airtable_config"), "bases")
    entries = []
    for key, value in zip(bases.keys, bases.values):
        tables = call_kwarg(value, "tables")
        entries.append((
            ast.literal_eval(key),
            ast.literal_eval(call_kwarg(value, "base_id")),
            {
                ast.literal_eval(tk): {
                    kw.arg: ast.literal_eval(kw.value) for kw in tv.keywords
                }
                for tk, tv in zip(tables.keys, tables.values)
            },
        ))
    return entries


def base_config_module(base_config):
    """Wrap a bases={} fragment so the same reader works before it is patched in."""
    return f"airtable_config = AirtableServiceConfig(\n    bases={{\n{base_config}\n    }}\n)"


def custom_dau(generated, block):
    """The wizard state of a user who replaced our CTEs with hand-written ones.

    Both generators fold the override into dau_sql and leave ours beside it, so
    a dau_sql that is not the window plus our two CTEs is what marks it custom.
    """
    return {**generated, "dau_sql": generated["dau_program_window"] + "\n\n" + block}


def airtable_pr_creator(program, generated):
    """An AirtablePrCreator that reads the parent repo off disk instead of GitHub."""
    creator = AirtablePrCreator("", program, generated, {"github_username": "nobody"})
    creator._require_file = lambda path, repo=None: repo_file(path)
    return creator


def test_generated_names(generated):
    assert generated["env_var_name"] == "TEST_PROGRAM_DATABASE_URL"
    assert generated["connection_variable_name"] == "test_program_db_connection"
    assert generated["definitions_import"] == "test_program_warehouse_mirror"
    assert 'EnvVar("TEST_PROGRAM_DATABASE_URL")' in generated["connection_resource"]
    assert 'name="TEST_PROGRAM_DB"' in generated["connection_resource"]


def test_replication_config_excludes_sensitive_columns(generated):
    config = generated["replication_config"]
    assert '"source": "TEST_PROGRAM_DB"' in config
    assert '"target": "WAREHOUSE_DB"' in config
    assert '"email"' in config
    assert "password_digest" not in config
    assert "reset_token" not in config
    assert '"mode": "incremental"' in config
    assert '"update_key": "updated_at"' in config


def test_asset_function_is_valid_python(generated):
    fn = generated["asset_function"]
    compile(fn, "asset_function.py", "exec")
    assert "def test_program_warehouse_mirror(" in fn
    assert "replication_config=test_program_replication_config," in fn
    assert "_ensure_incremental_target_indexes(context, test_program_replication_config)" in fn


def test_sources_yml_fragment_parses(generated):
    parsed = yaml.safe_load("sources:\n" + generated["sources_yml"])
    entry = parsed["sources"][0]
    assert entry["name"] == PROGRAM
    assert entry["schema"] == PROGRAM
    assert [t["name"] for t in entry["tables"]] == ["users", "projects", "devlogs"]
    assert entry["tables"][0]["meta"]["dagster"]["deps"] == ["test_program_warehouse_mirror"]


def test_dau_ctes(generated):
    window = generated["dau_program_window"]
    assert "('test_program', TIMESTAMP WITH TIME ZONE '2026-06-01 00:00:00+00'," in window
    assert "NULL::timestamptz)," in window

    claims = generated["dau_ht_claims"]
    assert claims.startswith("test_program_ht_claims AS (")
    assert "source('test_program', 'users')" in claims

    hourly = generated["dau_custom_hourly"]
    assert hourly.startswith("test_program_custom_hourly AS (")
    assert "source('test_program', 'devlogs')" in hourly
    assert "duration_seconds" in hourly
    assert "/ 3600.0)" in hourly


def test_check_conflicts(assets_source):
    assert check_conflicts(assets_source, "stardance")
    assert check_conflicts(assets_source, PROGRAM) is None


def test_patch_assets_py(assets_source, generated):
    patched = patch_assets_py(assets_source, PROGRAM, generated)
    compile(patched, "assets.py", "exec")

    env_vars = [
        e.value for e in assigned_value(patched, "_SLING_CONNECTION_URL_ENV_VARS").elts
    ]
    assert "TEST_PROGRAM_DATABASE_URL" in env_vars

    connections = call_kwarg_names(
        assigned_value(patched, "sling_replication_resource"), "connections"
    )
    assert "test_program_db_connection" in connections
    assert connections.index("test_program_db_connection") < connections.index(
        "warehouse_db_connection"
    )

    assert assignment_order(patched, "test_program_db_connection") < assignment_order(
        patched, "sling_replication_resource"
    )
    assert assigned_value(patched, "test_program_replication_config")
    assert "def test_program_warehouse_mirror(" in patched


def test_patch_definitions_py(definitions_source, generated):
    patched = patch_definitions_py(definitions_source, PROGRAM, generated)
    compile(patched, "definitions.py", "exec")

    imported = {
        alias.name
        for node in ast.walk(ast.parse(patched))
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "test_program_warehouse_mirror" in imported

    assets = call_kwarg_names(assigned_value(patched, "defs"), "assets")
    assert "test_program_warehouse_mirror" in assets


def test_patch_sources_yml(sources_source, generated):
    patched = patch_sources_yml(sources_source, generated)
    before = yaml.safe_load(sources_source)["sources"]
    after = yaml.safe_load(patched)["sources"]
    assert len(after) == len(before) + 1

    entry = next(s for s in after if s["name"] == PROGRAM)
    assert [t["name"] for t in entry["tables"]] == ["users", "projects", "devlogs"]


def test_patch_dau_sql(dau_source, generated):
    patched = patch_dau_sql(dau_source, PROGRAM, generated)

    assert "test_program_ht_claims AS (" in patched
    assert "test_program_custom_hourly AS (" in patched
    assert "UNION ALL SELECT * FROM test_program_ht_claims" in patched
    assert "UNION ALL SELECT * FROM test_program_custom_hourly" in patched

    window = "('test_program', TIMESTAMP WITH TIME ZONE '2026-06-01 00:00:00+00',"
    assert window in patched
    assert patched.index(window) < patched.index("AS t(program_name")
    assert patched.count(window) == 1


def test_custom_dau_block_ending_in_a_comment(dau_source, generated):
    """The comma separating the block from the next CTE goes after the SQL.

    Inside the trailing `--` it is invisible to Postgres, and the model it breaks
    is summer_unified_time_log for every program in the warehouse, not just this one.
    """
    block = "test_program_ht_claims AS (\n    SELECT 1 AS x\n)  -- hand-written, review me"
    patched = patch_dau_sql(dau_source, PROGRAM, custom_dau(generated, block))

    assert "),  -- hand-written, review me\n" in patched
    assert "review me," not in patched
    assert "UNION ALL SELECT * FROM test_program_ht_claims" in patched


def test_indented_custom_dau_block_is_wired_into_a_union(dau_source, generated):
    """Paste-indented CTEs are CTEs: they have to reach a UNION like any other."""
    block = (
        "    test_program_hours AS (\n        SELECT 1 AS x\n    ),\n"
        "    test_program_ht_claims AS (\n        SELECT * FROM test_program_hours\n    )"
    )
    patched = patch_dau_sql(dau_source, PROGRAM, custom_dau(generated, block))

    assert "UNION ALL SELECT * FROM test_program_ht_claims" in patched
    assert patched.count("test_program_hours AS (") == 1


def test_indented_custom_dau_block_the_model_cannot_read_is_refused(dau_source, generated):
    block = "    -- pasted out of the copy prompt\n    test_program_hours AS (\n        SELECT 1 AS x\n    )"
    with pytest.raises(REFUSALS) as exc:
        patch_dau_sql(dau_source, PROGRAM, custom_dau(generated, block))
    assert "test_program_hours" in str(exc.value)


def test_dau_post_patch_check(dau_source, generated):
    """The check has to fail on both ways a block can look fine and ship broken."""
    eaten = (
        "test_program_hours AS (\n    SELECT 1 AS x\n) -- helper\n"
        "test_program_ht_claims AS (\n    SELECT * FROM test_program_hours\n)"
    )
    with pytest.raises(REFUSALS) as exc:
        patch_dau_sql(dau_source, PROGRAM, custom_dau(generated, eaten))
    assert "comma" in str(exc.value)

    unreachable = (
        "test_program_hours AS (\n    SELECT 1 AS x\n),\n"
        "test_program_ht_claims AS (\n    SELECT 2 AS x\n)"
    )
    with pytest.raises(REFUSALS) as exc:
        patch_dau_sql(dau_source, PROGRAM, custom_dau(generated, unreachable))
    assert "test_program_hours" in str(exc.value)

    fine = (
        "test_program_hours AS (\n    SELECT 1 AS x\n),\n"
        "test_program_ht_claims AS (\n    SELECT * FROM test_program_hours\n)"
    )
    patched = patch_dau_sql(dau_source, PROGRAM, custom_dau(generated, fine))
    assert "UNION ALL SELECT * FROM test_program_ht_claims" in patched


def test_dau_model_refuses_a_program_it_already_carries(dau_source, generated):
    """A second run must raise, not append a WITH query name Postgres rejects."""
    patched = patch_dau_sql(dau_source, PROGRAM, generated)
    assert patched.count("test_program_ht_claims AS (") == 1

    with pytest.raises(REFUSALS) as exc:
        patch_dau_sql(patched, PROGRAM, generated)
    assert "test_program_ht_claims" in str(exc.value)


def test_validate_locally(generated):
    results = validate_locally(REPO_ROOT, PROGRAM, generated)
    assert set(results) >= {"assets.py", "definitions.py", "sources.yml"}
    assert all(r["ok"] for r in results.values()), results


def test_airtable_generated_names(airtable_generated):
    assert airtable_generated["source_type"] == "airtable"
    assert airtable_generated["dlt_sync_assets"].startswith(
        "test_program_assets = create_airtable_sync_assets("
    )

    _, base_id, tables = airtable_base_entries(
        base_config_module(airtable_generated["base_config"])
    )[0]
    assert base_id == "appAAAAAAAAAAAAAA"
    assert tables == {
        "users": {"table_id": "tblAAAAAAAAAAAAAA"},
        "time_logs": {"table_id": "tblBBBBBBBBBBBBBB"},
    }


def test_patch_airtable_definitions_survives_a_second_program(
    airtable_definitions_source, airtable_generated
):
    """The entry this patch inserts has to be a legal neighbour for the next one.

    A single pass only proves the *existing* last entry got its comma, which is
    the bug that shipped. The comma on the entry we just wrote needs the raw
    splice below to show up at all: a second patch would re-terminate it for us.
    """
    once = patch_airtable_definitions(airtable_definitions_source, airtable_generated)
    compile(once, AIRTABLE_DEFINITIONS_PATH, "exec")

    second = AirtableCodeGenerator(airtable_state(program="test_program_two")).generate()
    at = once.index("\n    }\n)")
    compile(
        once[:at] + "\n" + second["base_config"].rstrip() + once[at:],
        AIRTABLE_DEFINITIONS_PATH,
        "exec",
    )

    twice = patch_airtable_definitions(once, second)
    compile(twice, AIRTABLE_DEFINITIONS_PATH, "exec")

    before = [name for name, _, _ in airtable_base_entries(airtable_definitions_source)]
    after = [name for name, _, _ in airtable_base_entries(twice)]
    assert after == before + [PROGRAM, "test_program_two"]
    assert len(after) == len(set(after))


def test_patch_dlt_assets(dlt_assets_source, airtable_generated):
    patched = patch_dlt_assets(dlt_assets_source, airtable_generated)
    compile(patched, DLT_ASSETS_PATH, "exec")

    assert undefined_names(patched) == undefined_names(dlt_assets_source) == set()
    assert "all_dlt_airtable_assets" not in patched

    call = assigned_value(patched, "test_program_assets")
    assert call.func.id == "create_airtable_sync_assets"
    assert ast.literal_eval(call_kwarg(call, "base_name")) == PROGRAM
    assert ast.literal_eval(call_kwarg(call, "tables")) == ["users", "time_logs"]


def test_airtable_duplicate_program_is_refused(airtable_definitions_source):
    """A duplicate dict key is legal Python that silently repoints a live base."""
    generated = AirtableCodeGenerator(airtable_state(program=EXISTING_PROGRAM)).generate()
    assert EXISTING_PROGRAM in [
        name for name, _, _ in airtable_base_entries(airtable_definitions_source)
    ]

    with pytest.raises(REFUSALS):
        creator = airtable_pr_creator(EXISTING_PROGRAM, generated)
        creator._check_conflicts()
        creator._build_tree()


def test_airtable_new_program_passes_the_same_gate(airtable_generated):
    creator = airtable_pr_creator(PROGRAM, airtable_generated)
    creator._check_conflicts()
    assert {e["path"] for e in creator._build_tree()} >= {
        AIRTABLE_DEFINITIONS_PATH,
        GENERATED_IDS_PATH,
        DLT_ASSETS_PATH,
        SOURCES_YML_PATH,
    }


def test_airtable_sensitive_fields_are_excluded_or_refused():
    """AirtableTableConfig has no field allowlist, so exclusion has to be real."""
    state = airtable_state(sensitive_columns={"Users": ["Secret Token"]})
    try:
        generated = AirtableCodeGenerator(state).generate()
    except GeneratorError as exc:
        assert "Secret Token" in str(exc)
        return

    tables = airtable_base_entries(base_config_module(generated["base_config"]))[0][2]
    allowed = tables["users"].get("fields")
    assert allowed, (
        "no refusal and no fields= allowlist: the loader syncs every field of "
        "every table it is given, so Secret Token lands in the warehouse"
    )
    assert {sanitize_name(f) for f in allowed} == {"email", "hackatime_alias", "created_at"}


def test_airtable_hostile_identifiers(airtable_definitions_source, dlt_assets_source):
    state = airtable_state()
    state["selected_tables"] = ["on", "Weird Table"]
    state["schema"] = [
        {**state["schema"][0], "name": "on"},
        {**state["schema"][1], "name": "Weird Table"},
    ]
    state["dau_config"] = {
        **state["dau_config"],
        "ht_mapping_table": "on",
        "custom_table": "Weird Table",
    }
    generated = AirtableCodeGenerator(state).generate()

    compile(
        patch_airtable_definitions(airtable_definitions_source, generated),
        AIRTABLE_DEFINITIONS_PATH,
        "exec",
    )
    compile(patch_dlt_assets(dlt_assets_source, generated), DLT_ASSETS_PATH, "exec")

    entry = yaml.safe_load("sources:\n" + generated["sources_yml"])["sources"][0]
    assert [t["name"] for t in entry["tables"]] == ["on", "weird_table"]
    assert entry["tables"][0]["meta"]["dagster"]["deps"] == ["test_program_on_warehouse"]

    assert 'a."hours_logged"' in generated["dau_custom_hourly"]
    assert "source('airtable_test_program', 'weird_table')" in generated["dau_custom_hourly"]


def test_postgres_hostile_identifiers(assets_source, sources_source):
    quoted = 'he said "hi"'
    state = {
        "program_name": "hostile",
        "selected_tables": ["on", "logs"],
        "sensitive_columns": {"logs": ['secret"token']},
        "sync_configs": {
            "logs": {"mode": "incremental", "primary_key": ["id"], "update_key": quoted}
        },
        "schema": [
            {
                "name": "on",
                "schema": "public",
                "columns": [
                    {"name": "id", "type": "integer", "udt": "int4"},
                    {"name": "email", "type": "text", "udt": "text"},
                ],
                "primary_key": ["id"],
            },
            {
                "name": "logs",
                "schema": "public",
                "columns": [
                    {"name": "id", "type": "integer", "udt": "int4"},
                    {"name": "user_email", "type": "text", "udt": "text"},
                    {"name": quoted, "type": "timestamp", "udt": "timestamptz"},
                    {"name": 'secret"token', "type": "text", "udt": "text"},
                ],
                "primary_key": ["id"],
            },
        ],
        "dau_config": {
            "has_custom_time": True,
            "custom_table": "logs",
            "timestamp_column": quoted,
            "user_column": "user_email",
            "start_date": "2026-06-01",
        },
    }
    generated = CodeGenerator(state).generate()
    compile(patch_assets_py(assets_source, "hostile", generated), "assets.py", "exec")

    config = assigned_value(generated["replication_config"], "hostile_replication_config")
    streams = ast.literal_eval(dict_entry(config, "streams"))
    assert streams["public.logs"]["update_key"] == quoted
    assert streams["public.logs"]["select"] == ["id", "user_email", quoted]

    entry = yaml.safe_load("sources:\n" + generated["sources_yml"])["sources"][0]
    assert [t["name"] for t in entry["tables"]] == ["on", "logs"]

    patched = patch_sources_yml(sources_source, generated)
    names = [s["name"] for s in yaml.safe_load(patched)["sources"]]
    assert names.count("hostile") == 1

    assert 'a."he said ""hi"""' in generated["dau_custom_hourly"]


def test_target_schema_cannot_inject_a_defaults_key():
    state = {
        "program_name": "hostile",
        "target_schema": 'hostile", "mode": "truncate',
        "selected_tables": ["users"],
        "schema": [
            {
                "name": "users",
                "schema": "public",
                "columns": [{"name": "id", "type": "integer", "udt": "int4"}],
                "primary_key": ["id"],
            }
        ],
    }
    try:
        config = CodeGenerator(state).generate()["replication_config"]
    except GeneratorError:
        return

    defaults = dict_entry(
        assigned_value(config, "hostile_replication_config"), "defaults"
    )
    assert dict_keys(defaults) == ["mode", "object"]
    assert ast.literal_eval(defaults)["mode"] == "full-refresh"


def test_airtable_field_name_with_a_newline_stays_inside_its_comment():
    """Field names are echoed after a `#`; a newline in one would end the comment."""
    state = airtable_state()
    state["schema"][0]["columns"][2] = {
        **state["schema"][0]["columns"][2],
        "name": "Secret\nToken",
    }
    generated = AirtableCodeGenerator(state).generate()

    patched = patch_generated_ids(repo_file(GENERATED_IDS_PATH), generated)
    compile(patched, GENERATED_IDS_PATH, "exec")
    assert '            secret_token = "fldA3"  # Name: Secret Token' in patched
    assert "Token" not in [line.strip() for line in patched.split("\n")]


def test_two_airtable_fields_cannot_share_a_generated_ids_constant():
    """dlt/assets.py renames by constant, so the loser keeps its Airtable name forever."""
    state = airtable_state()
    state["schema"][0]["columns"].append(
        {"name": "created at", "type": "text", "udt": "singleLineText", "field_id": "fldA5"}
    )
    generated = AirtableCodeGenerator(state).generate()

    with pytest.raises(REFUSALS) as exc:
        patch_generated_ids(repo_file(GENERATED_IDS_PATH), generated)
    assert "'Created At'" in str(exc.value)
    assert "'created at'" in str(exc.value)


def test_validate_locally_airtable(airtable_generated):
    results = validate_locally(REPO_ROOT, PROGRAM, airtable_generated)
    assert set(results) >= {"airtable/definitions.py", "dlt/assets.py", "sources.yml"}
    assert all(r["ok"] for r in results.values()), results
