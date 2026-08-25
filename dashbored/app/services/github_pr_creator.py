"""Create a PR to hackclub/data-warehouse with generated warehouse code.

Uses libcst to structurally patch assets.py instead of regex anchors.
Uses PyYAML to patch sources.yml instead of string manipulation.
"""

from __future__ import annotations

import ast
import base64
import keyword
import re
import time
from pathlib import Path

import libcst as cst
import requests
import yaml

from .airtable_code_generator import sanitize_name


UPSTREAM_REPO = "hackclub/data-warehouse"
BASE_BRANCH = "main"
API = "https://api.github.com"
API_TIMEOUT = 10
FORK_READY_TIMEOUT = 10
SLING_ASSETS_PATH = "orpheus_engine/defs/sling/assets.py"
SLING_DEFINITIONS_PATH = "orpheus_engine/defs/sling/definitions.py"
AIRTABLE_DEFINITIONS_PATH = "orpheus_engine/defs/airtable/definitions.py"
GENERATED_IDS_PATH = "orpheus_engine/defs/airtable/generated_ids.py"
DLT_ASSETS_PATH = "orpheus_engine/defs/dlt/assets.py"
SOURCES_YML_PATH = "orpheus_engine_dbt/models/sources.yml"
DAU_MODEL_PATH = "orpheus_engine_dbt/models/summer_2026_analytics/summer_unified_time_log.sql"


class ConflictError(Exception):
    pass


class _GithubPrBase:
    """Shared GitHub API plumbing for fork-based PR creation.

    Forks upstream to the user's account, pushes there, then opens
    a cross-repo PR back to upstream. The contribution shows up as
    authored by the user.
    """

    def __init__(self, token: str, program_name: str, generated: dict, user: dict):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }
        self.program = program_name
        self.generated = generated
        self.user = user
        self._fork_repo = None

    @property
    def fork_repo(self) -> str:
        if not self._fork_repo:
            self._fork_repo = self._ensure_fork()
        return self._fork_repo

    def create(self) -> dict:
        self._check_conflicts()

        branch = f"dashbored/add-{self.program}"
        base_sha = self._get_ref_sha(f"heads/{BASE_BRANCH}")

        if self._ref_exists(f"heads/{branch}", repo=self.fork_repo):
            raise ConflictError(
                f"Branch '{branch}' already exists on your fork. "
                f"A PR for '{self.program}' may already be open."
            )

        tree_entries = self._build_tree()
        if not tree_entries:
            raise ConflictError("No files to commit")

        self._sync_fork()

        base_tree = self._get_commit_tree(base_sha, repo=self.fork_repo)
        new_tree_sha = self._create_tree(tree_entries, base_tree, repo=self.fork_repo)
        commit_sha = self._create_commit(
            self._commit_message(),
            new_tree_sha,
            base_sha,
            repo=self.fork_repo,
        )

        # Everything above is either read-only or produces unreferenced git
        # objects, so a failure leaves nothing behind. The ref is the first
        # thing a retry would trip over, so it goes last and gets rolled back.
        self._create_ref(branch, commit_sha, repo=self.fork_repo)
        try:
            return self._create_pr_request(branch, self.user.get("github_username", ""))
        except Exception:
            self._delete_ref(branch, repo=self.fork_repo)
            raise

    def _check_conflicts(self):
        # Both source types write sources.yml, so this belongs here rather than
        # in either subclass.
        conflict = sources_yml_conflict(self._require_file(SOURCES_YML_PATH), self.generated)
        if conflict:
            raise ConflictError(conflict)

        if not wants_dau(self.generated):
            return
        conflict = dau_model_conflict(self._require_file(DAU_MODEL_PATH), self.program)
        if conflict:
            raise ConflictError(conflict)

    def _build_tree(self) -> list[dict]:
        raise NotImplementedError

    def _commit_message(self) -> str:
        return f"Add {self.program} warehouse mirror via dashbored"

    def _pr_body(self) -> str:
        raise NotImplementedError

    def _create_pr_request(self, branch: str, username: str) -> dict:
        r = self._api(
            "POST", "pulls",
            json={
                "title": self._commit_message(),
                "head": f"{username}:{branch}",
                "base": BASE_BRANCH,
                "body": self._pr_body(),
            },
        )
        return r.json()

    def _blob(self, path: str, content: str) -> dict:
        return {"path": path, "mode": "100644", "type": "blob", "content": content}

    def _dau_tree_entries(self) -> list[dict]:
        if not wants_dau(self.generated):
            return []
        content = self._require_file(DAU_MODEL_PATH)
        patched = patch_dau_sql(content, self.program, self.generated)
        return [self._blob(DAU_MODEL_PATH, patched)]

    def _sources_tree_entry(self) -> list[dict]:
        content = self._require_file(SOURCES_YML_PATH)
        patched = patch_sources_yml(content, self.generated)
        return [self._blob(SOURCES_YML_PATH, patched)]

    def _dau_section(self) -> str:
        custom = custom_dau_sql(self.generated)
        if custom:
            return (
                f"<details>\n<summary>hand-written DAU SQL (from the wizard, needs review)"
                f"</summary>\n\n```sql\n{custom}\n```\n</details>"
            )

        parts = []
        if self.generated.get("dau_ht_claims"):
            parts.append(
                f"<details>\n<summary>hackatime claims CTE (draft, needs review)</summary>\n\n"
                f"```sql\n{self.generated['dau_ht_claims']}\n```\n</details>"
            )
        if self.generated.get("dau_custom_hourly"):
            parts.append(
                f"<details>\n<summary>custom time CTE (draft, needs review)</summary>\n\n"
                f"```sql\n{self.generated['dau_custom_hourly']}\n```\n</details>"
            )
        return "\n\n".join(parts)

    def _api(self, method: str, path: str, repo: str = UPSTREAM_REPO, **kwargs) -> requests.Response:
        url = f"{API}/repos/{repo}/{path}" if not path.startswith("http") else path
        kwargs.setdefault("timeout", API_TIMEOUT)
        r = requests.request(method, url, headers=self.headers, **kwargs)
        r.raise_for_status()
        return r

    def _ensure_fork(self) -> str:
        username = self.user.get("github_username", "")
        repo_name = UPSTREAM_REPO.split("/")[1]
        fork_full = f"{username}/{repo_name}"

        try:
            r = self._api("GET", "", repo=fork_full)
            if r.json().get("fork"):
                return fork_full
        except requests.HTTPError:
            pass

        r = self._api("POST", "forks", json={"default_branch_only": True})
        fork_full = r.json()["full_name"]

        # Forking is asynchronous. Waiting it out would blow the gunicorn
        # request window, so give up quickly and let the user retry against
        # the fork GitHub is already building.
        deadline = time.monotonic() + FORK_READY_TIMEOUT
        while True:
            try:
                self._api("GET", f"git/ref/heads/{BASE_BRANCH}", repo=fork_full)
                return fork_full
            except requests.HTTPError:
                if time.monotonic() >= deadline:
                    raise ConflictError(
                        f"GitHub is still creating your fork of {UPSTREAM_REPO}. "
                        f"Give it a moment and submit again."
                    )
                time.sleep(1)

    def _sync_fork(self):
        """Fast-forward the fork's main to upstream, without clobbering it."""
        try:
            self._api("POST", "merge-upstream", repo=self.fork_repo, json={"branch": BASE_BRANCH})
        except requests.HTTPError:
            pass

    def _fetch_file(self, path: str, repo: str = UPSTREAM_REPO) -> str | None:
        """None means the file is genuinely absent; anything else raises."""
        try:
            r = self._api("GET", f"contents/{path}", repo=repo)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise
        return base64.b64decode(r.json()["content"]).decode()

    def _require_file(self, path: str, repo: str = UPSTREAM_REPO) -> str:
        try:
            content = self._fetch_file(path, repo=repo)
        except requests.RequestException as e:
            raise ConflictError(
                f"GitHub would not serve '{path}' ({e}). Nothing was changed — try again in a minute."
            ) from e
        if content is None:
            raise ConflictError(
                f"'{path}' no longer exists in {repo}. dashbored has to patch it to build this PR."
            )
        return content

    def _get_ref_sha(self, ref: str, repo: str = UPSTREAM_REPO) -> str:
        return self._api("GET", f"git/ref/{ref}", repo=repo).json()["object"]["sha"]

    def _ref_exists(self, ref: str, repo: str = UPSTREAM_REPO) -> bool:
        try:
            self._api("GET", f"git/ref/{ref}", repo=repo)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return False
            raise
        return True

    def _create_ref(self, branch: str, sha: str, repo: str = UPSTREAM_REPO):
        self._api("POST", "git/refs", repo=repo, json={"ref": f"refs/heads/{branch}", "sha": sha})

    def _delete_ref(self, branch: str, repo: str = UPSTREAM_REPO):
        try:
            self._api("DELETE", f"git/refs/heads/{branch}", repo=repo)
        except requests.RequestException:
            pass

    def _get_commit_tree(self, sha: str, repo: str = UPSTREAM_REPO) -> str:
        return self._api("GET", f"git/commits/{sha}", repo=repo).json()["tree"]["sha"]

    def _create_tree(self, entries: list[dict], base_tree: str, repo: str = UPSTREAM_REPO) -> str:
        r = self._api("POST", "git/trees", repo=repo, json={"tree": entries, "base_tree": base_tree})
        return r.json()["sha"]

    def _create_commit(self, message: str, tree_sha: str, parent_sha: str, repo: str = UPSTREAM_REPO) -> str:
        r = self._api(
            "POST", "git/commits",
            repo=repo,
            json={"message": message, "tree": tree_sha, "parents": [parent_sha]},
        )
        return r.json()["sha"]


class GithubPrCreator(_GithubPrBase):
    """Creates PRs for sling/postgres-backed programs."""

    def __init__(
        self,
        token: str,
        program_name: str,
        generated: dict,
        encrypted_url: str,
        user: dict,
    ):
        super().__init__(token, program_name, generated, user)
        self.encrypted_url = encrypted_url

    def _check_conflicts(self):
        super()._check_conflicts()
        conflict = check_conflicts(self._require_file(SLING_ASSETS_PATH), self.program)
        if conflict:
            raise ConflictError(conflict)

    def _build_tree(self) -> list[dict]:
        entries = [
            self._blob(
                SLING_ASSETS_PATH,
                patch_sling_assets(
                    self._require_file(SLING_ASSETS_PATH), self.program, self.generated
                ),
            ),
            self._blob(
                SLING_DEFINITIONS_PATH,
                patch_definitions_py(
                    self._require_file(SLING_DEFINITIONS_PATH), self.program, self.generated
                ),
            ),
        ]
        entries.extend(self._sources_tree_entry())
        entries.extend(self._dau_tree_entries())
        return entries

    def _pr_body(self) -> str:
        tables_list = ", ".join(self.generated.get("table_names") or []) or "see config"

        return f"""add `{self.program}` to the warehouse.

tables: {tables_list}
env var: `{self.generated['env_var_name']}`

<details>
<summary>connection url (age-encrypted)</summary>

```
{self.encrypted_url}
```
set as `{self.generated['env_var_name']}` in deployment env.
</details>

{self._dau_section()}

---
*via [dashbored](https://dashbored.hackclub.com)*"""


class AirtablePrCreator(_GithubPrBase):
    """Creates PRs for airtable-backed programs."""

    def _commit_message(self) -> str:
        return f"Add {self.program} airtable sync via dashbored"

    def _check_conflicts(self):
        super()._check_conflicts()
        for conflict in (
            airtable_base_conflict(
                self._require_file(AIRTABLE_DEFINITIONS_PATH), self.program
            ),
            dlt_assets_conflict(self._require_file(DLT_ASSETS_PATH), self.program),
            generated_ids_conflict(
                self._require_file(GENERATED_IDS_PATH), self.program
            ),
        ):
            if conflict:
                raise ConflictError(conflict)

    def _build_tree(self) -> list[dict]:
        entries = [
            self._blob(
                AIRTABLE_DEFINITIONS_PATH,
                patch_airtable_definitions(
                    self._require_file(AIRTABLE_DEFINITIONS_PATH), self.generated
                ),
            ),
            self._blob(
                GENERATED_IDS_PATH,
                patch_generated_ids(
                    self._require_file(GENERATED_IDS_PATH), self.generated
                ),
            ),
            self._blob(
                DLT_ASSETS_PATH,
                patch_dlt_assets(self._require_file(DLT_ASSETS_PATH), self.generated),
            ),
        ]
        entries.extend(self._sources_tree_entry())
        entries.extend(self._dau_tree_entries())
        return entries

    def _pr_body(self) -> str:
        tables_list = ", ".join(self.generated.get("table_names") or []) or "see config"

        return f"""add `{self.program}` to the warehouse (airtable).

tables: {tables_list}
sync: full-refresh via DLT

field IDs for the new base are included in `generated_ids.py`, read from the base schema at wizard time. sync uses the existing `AIRTABLE_PERSONAL_ACCESS_TOKEN`, which needs access to this base before the first materialization.

{self._dau_section()}

---
*via [dashbored](https://dashbored.hackclub.com)*"""



def check_conflicts(source: str, program: str) -> str | None:
    """Walk the CST to check if a program already exists in assets.py."""
    try:
        tree = cst.parse_module(source)
    except cst.ParserSyntaxError:
        return None

    target_names = {f"{program}_db_connection", f"{program}_replication_config"}

    for stmt in tree.body:
        names = _assignment_names(stmt)
        for name in names:
            if name in target_names:
                return f"Program '{program}' already exists in assets.py (found {name})"
    return None


def patch_sling_assets(source: str, program: str, generated: dict) -> str:
    """Refuse to patch a program that is already in assets.py, then patch."""
    conflict = check_conflicts(source, program)
    if conflict:
        raise ConflictError(conflict)
    return patch_assets_py(source, program, generated)


def patch_assets_py(source: str, program: str, generated: dict) -> str:
    """Structurally patch assets.py using libcst."""
    tree = cst.parse_module(source)

    tree = _visit_required(
        tree, _EnvVarInserter(generated["env_var_name"]), "_SLING_CONNECTION_URL_ENV_VARS"
    )
    tree = _visit_required(
        tree,
        _ConnectionsListInserter(generated["connection_variable_name"]),
        "the connections= list of sling_replication_resource",
    )
    tree = _insert_before_assignment(
        tree, "warehouse_db_connection", generated["connection_resource"]
    )

    suffix = f'\n\n{generated["replication_config"]}\n\n{generated["asset_function"]}\n'
    new_stmts = cst.parse_module(suffix).body
    tree = tree.with_changes(body=(*tree.body, *new_stmts))

    return tree.code


def patch_definitions_py(source: str, program: str, generated: dict) -> str:
    """Add import and asset list entry to definitions.py."""
    name = generated["definitions_import"]

    anchor = "    sling_replication_resource,\n"
    if anchor not in source:
        raise ConflictError(
            f"Could not find the sling_replication_resource import in "
            f"{SLING_DEFINITIONS_PATH}; dashbored's patch needs updating."
        )
    source = source.replace(anchor, f"    {name},\n{anchor}")

    tree = cst.parse_module(source)
    entry_name = generated["definitions_asset_entry"].strip().rstrip(",")
    tree = _visit_required(
        tree, _AssetsListInserter(entry_name), f"the assets= list in {SLING_DEFINITIONS_PATH}"
    )

    return tree.code


def _visit_required(tree: cst.Module, inserter: cst.CSTTransformer, what: str) -> cst.Module:
    """Run a transformer that must find its target, or say so out loud."""
    tree = tree.visit(inserter)
    if not inserter.done:
        raise ConflictError(f"Could not find {what}; dashbored's patch needs updating.")
    return tree


def patch_sources_yml(source: str, generated: dict) -> str:
    new_source_text = generated.get("sources_yml", "")
    if not new_source_text:
        return source
    return source.rstrip() + "\n" + new_source_text + "\n"


def sources_yml_conflict(source: str, generated: dict) -> str | None:
    """A dbt source name may only appear once in the project.

    patch_sources_yml is a blind append, and a repeated `- name:` is valid YAML
    that compiles fine, so neither the patcher nor validate_locally notices. dbt
    refuses the whole project at parse time — every model, not just this one —
    and in a 600-line file the duplicate line looks exactly like a correct
    addition to whoever reviews the PR.
    """
    try:
        after = yaml.safe_load(patch_sources_yml(source, generated)) or {}
    except yaml.YAMLError as e:
        return f"{SOURCES_YML_PATH} would not be valid YAML after patching: {e}"

    names = [s.get("name") for s in (after.get("sources") or []) if isinstance(s, dict)]
    dupes = sorted({n for n in names if n and names.count(n) > 1})
    if not dupes:
        return None
    return (
        f"{SOURCES_YML_PATH} already declares the dbt source "
        f"{', '.join(repr(d) for d in dupes)}. Adding it again is valid YAML but dbt "
        f"refuses the entire project — pick a different program name."
    )


def wants_dau(generated: dict) -> bool:
    return bool((generated.get("dau_sql") or "").strip())


def custom_dau_sql(generated: dict) -> str | None:
    """The hand-written CTEs, when the review tab is showing them instead of ours.

    Both generators fold custom SQL into dau_sql and leave the auto-generated
    pieces in place beside it, so the difference between the two is the only
    signal that the user overrode them.
    """
    shown = (generated.get("dau_sql") or "").strip()
    if not shown:
        return None

    window = (generated.get("dau_program_window") or "").strip()
    body = shown[len(window):].strip() if window and shown.startswith(window) else shown
    ours = "\n\n".join(
        filter(None, [generated.get("dau_ht_claims"), generated.get("dau_custom_hourly")])
    ).strip()
    return body if body and body != ours else None


def patch_dau_sql(source: str, program: str, generated: dict) -> str:
    """Insert DAU CTEs into the unified time log model at the right anchor points."""
    result = source

    if wants_dau(generated):
        conflict = dau_model_conflict(source, program)
        if conflict:
            raise ConflictError(conflict)

    if wants_dau(generated) and not generated.get("dau_program_window"):
        raise ConflictError(
            f"'{program}' has DAU SQL but no program window: every CTE in "
            f"{DAU_MODEL_PATH.rsplit('/', 1)[-1]} is joined to program_windows, so without "
            f"a start date the program reports zero DAU. Go back and set one."
        )

    pw = generated.get("dau_program_window")
    if pw:
        entry = re.sub(r"^-- program_windows entry:\n", "", pw).strip().rstrip(",")
        anchor = _require_anchor(
            re.search(
                r"(\s*\)\s*AS\s+t\(program_name,\s*start_at,\s*end_at_exclusive\))",
                result,
            ),
            "the program_windows VALUES list",
        )
        before = result[:anchor.start()]
        lines = before.split("\n")
        for i in range(len(lines) - 1, -1, -1):
            stripped = lines[i].lstrip()
            if stripped.startswith("--") or not stripped:
                continue
            comment = lines[i].find("--")
            code = lines[i][:comment] if comment >= 0 else lines[i]
            if ")" in code and not code.rstrip().endswith(","):
                idx = code.rfind(")")
                lines[i] = lines[i][:idx + 1] + "," + lines[i][idx + 1:]
            break
        before = "\n".join(lines)
        result = before + f"\n        {entry}\n    " + result[anchor.start():]

    custom = custom_dau_sql(generated)
    if custom:
        return _verify_dau_patch(source, _patch_custom_dau(result, program, custom))

    ht = generated.get("dau_ht_claims")
    if ht:
        claims_cte = re.sub(r"^-- DRAFT:.*\n(-- .*\n)*", "", ht).strip()
        anchor = _require_anchor(
            re.search(r"^all_claims_raw AS \(", result, re.MULTILINE),
            "the all_claims_raw CTE",
        )
        result = result[:anchor.start()] + claims_cte + "\n\n" + result[anchor.start():]
        result = _union_into_claims(result, program)

    ch = generated.get("dau_custom_hourly")
    if ch:
        anchor = _require_anchor(
            re.search(r"^custom_in_window AS \(", result, re.MULTILINE),
            "the custom_in_window CTE",
        )
        result = result[:anchor.start()] + ch.strip() + "\n\n" + result[anchor.start():]
        result = _union_into_custom(result, program)

    return _verify_dau_patch(source, result)


def _union_into_claims(result: str, program: str) -> str:
    anchor = _require_anchor(
        re.search(r"(UNION ALL SELECT \* FROM \S+_ht_claims\s*\n)(\),)", result),
        "the last UNION ALL inside all_claims_raw",
    )
    return _insert_at(
        result, anchor.end(1), f"    UNION ALL SELECT * FROM {program}_ht_claims\n"
    )


def _union_into_custom(result: str, program: str) -> str:
    anchor = _require_anchor(
        re.search(r"(UNION ALL SELECT \* FROM \S+_custom_hourly\s*\n)(\s*\) c\b)", result),
        "the last UNION ALL inside custom_in_window",
    )
    return _insert_at(
        result, anchor.end(1), f"            UNION ALL SELECT * FROM {program}_custom_hourly\n"
    )


def _insert_at(source: str, index: int, text: str) -> str:
    return source[:index] + text + source[index:]


def _patch_custom_dau(result: str, program: str, custom: str) -> str:
    """Ship the CTEs the user wrote, wired into the same UNIONs the generated ones use.

    The wizard asks for CTEs named <program>_ht_claims and <program>_custom_hourly;
    anything else it defines is a helper those two select from. A block that defines
    neither cannot be reached by the model at all, so refuse it instead of committing
    dead SQL under a DAU tab that claimed it was in.
    """
    block = custom.strip().rstrip(";").rstrip()

    # The CTEs are spliced into the model's existing WITH clause, so a WITH the
    # user (or the LLM they pasted the copy-prompt into) wrote would land in the
    # middle of that list, where Postgres rejects it.  Strip leading -- comments
    # first so they can't hide the keyword from re.match.
    bare = re.sub(r"^(\s*--[^\n]*\n)*\s*", "", block)
    lead = re.match(r"WITH\s+(RECURSIVE\s+)?", bare, re.IGNORECASE)
    if lead and lead.group(1):
        raise ConflictError(
            f"The custom DAU SQL opens with WITH RECURSIVE. Its CTEs go into the WITH "
            f"clause {DAU_MODEL_PATH.rsplit('/', 1)[-1]} already has, which is not recursive "
            f"and cannot be made recursive for one program. Rewrite them without recursion."
        )
    if lead:
        block = bare[lead.end():]

    _dau_check(block, "The custom DAU SQL")
    defined = [name for name, _, _ in _top_level_ctes(block)]

    wired = False
    if f"{program}_ht_claims" in defined:
        result = _union_into_claims(result, program)
        wired = True
    if f"{program}_custom_hourly" in defined:
        result = _union_into_custom(result, program)
        wired = True

    if not wired:
        raise ConflictError(
            f"The custom DAU SQL defines {', '.join(defined) or 'no top-level CTE'}, but the "
            f"model only reads '{program}_ht_claims' and '{program}_custom_hourly'. Rename a "
            f"CTE to one of those so the DAU model can select from it."
        )

    anchor = _require_anchor(
        re.search(r"^all_claims_raw AS \(", result, re.MULTILINE),
        "the all_claims_raw CTE",
    )
    return _insert_at(result, anchor.start(), _sql_comma_terminated(block) + "\n\n")


_SQL_NONCODE = re.compile(r"--[^\n]*|'(?:[^']|'')*'|\"[^\"]*\"|\{\{.*?\}\}", re.S)


def _sql_blank(sql: str) -> str:
    """The SQL with comments, literals and jinja blanked out, every offset preserved.

    Block comments are handled with a depth counter so nested /* ... /* ... */ ... */
    is blanked as a unit (the regex fallback matched the inner pair only).
    """
    def _blank(m):
        return re.sub(r"[^\n]", " ", m.group())

    out = []
    i, n = 0, len(sql)
    while i < n:
        m = _SQL_NONCODE.match(sql, i)
        if m:
            out.append(_blank(m))
            i = m.end()
        elif sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth += 1
                    j += 2
                elif sql.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            out.append(re.sub(r"[^\n]", " ", sql[i:j]))
            i = j
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def _top_level_ctes(sql: str) -> list[tuple[str, int, int]]:
    """(name, offset of the name, offset past the closing paren) for each CTE in the WITH clause.

    Paren depth, not indentation: a block the user pasted indented defines its CTEs
    just as much as one flush against column 1, and a `foo AS (` inside a CTE body
    defines nothing.
    """
    blanked = _sql_blank(sql)
    depths, closes = [], {}
    stack: list[int] = []
    depth = 0
    for i, ch in enumerate(blanked):
        depths.append(depth)
        if ch == "(":
            stack.append(i)
            depth += 1
        elif ch == ")":
            depth -= 1
            if stack:
                closes[stack.pop()] = i

    entries = []
    for m in re.finditer(r"\b(\w+)\s+AS\s*\(", blanked, re.IGNORECASE):
        open_at = m.end() - 1
        if depths[m.start()] == 0 and open_at in closes:
            entries.append((m.group(1), m.start(), closes[open_at] + 1))
    return entries


def _sql_comma_terminated(block: str) -> str:
    """Comma after the last actual SQL, not inside whatever comment trails it."""
    end = len(_sql_blank(block).rstrip())
    if end and block[end - 1] == ",":
        return block
    return block[:end] + "," + block[end:]


def _line_of(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def _dau_check(sql: str, label: str):
    """The DAU model is SQL, so compile()/yaml.safe_load() cannot vouch for it.

    Catches the ways a CTE list stays a plausible-looking diff while no longer
    being SQL Postgres will run: unbalanced parens, a repeated query name ("WITH
    query name specified more than once"), and a comma eaten by a -- comment.
    """
    blanked = _sql_blank(sql)
    if blanked.count("(") != blanked.count(")"):
        raise ConflictError(
            f"{label} has unbalanced parentheses "
            f"({blanked.count('(')} open, {blanked.count(')')} close)."
        )

    ctes = _top_level_ctes(sql)
    names = [name for name, _, _ in ctes]
    dupes = sorted({name for name in names if names.count(name) > 1})
    if dupes:
        raise ConflictError(
            f"{label} defines {', '.join(dupes)} more than once. Postgres rejects a WITH "
            f"clause with a repeated query name, so this would break the model for every "
            f"program."
        )

    for (name, _, end), (nxt, nxt_start, _) in zip(ctes, ctes[1:]):
        if "," not in blanked[end:nxt_start]:
            raise ConflictError(
                f"{label} has no comma between CTE '{name}' (line {_line_of(sql, end)}) and "
                f"'{nxt}' — a trailing `--` comment swallowed it. Move the comment above the "
                f"CTE and try again."
            )


def _verify_dau_patch(source: str, patched: str) -> str:
    """Everything the patch added has to be reachable, or the program reports zero DAU."""
    _dau_check(patched, DAU_MODEL_PATH)

    before = {name for name, _, _ in _top_level_ctes(source)}
    blanked = _sql_blank(patched)
    orphans = [
        name
        for name, start, end in _top_level_ctes(patched)
        if name not in before
        and not re.search(rf"\b{re.escape(name)}\b", blanked[:start] + blanked[end:])
    ]
    if orphans:
        raise ConflictError(
            f"{', '.join(orphans)} would be added to {DAU_MODEL_PATH} but nothing in the "
            f"model selects from them, so those hours would never reach DAU. Name the CTEs "
            f"'<program>_ht_claims' and '<program>_custom_hourly' and let helpers feed them."
        )
    return patched


def dau_model_conflict(source: str, program: str) -> str | None:
    """Every program in the model already owns its CTE names and a program_windows row."""
    names = {name for name, _, _ in _top_level_ctes(source)}
    clash = sorted(names & {f"{program}_ht_claims", f"{program}_custom_hourly"})
    if clash:
        return (
            f"{DAU_MODEL_PATH} already defines {', '.join(clash)}. Adding {program} again "
            f"would repeat a WITH query name, which Postgres rejects — pick a different "
            f"program name, or edit the existing CTE by hand."
        )
    if re.search(rf"^\s*\('{re.escape(program)}',", source, re.MULTILINE):
        return (
            f"'{program}' already has a program_windows row in {DAU_MODEL_PATH}. A second "
            f"row would double every hour it logs — pick a different program name, or edit "
            f"the existing window by hand."
        )
    return None


def _require_anchor(match: re.Match | None, what: str) -> re.Match:
    if not match:
        raise ConflictError(
            f"Could not find {what} in {DAU_MODEL_PATH}. The model changed shape — "
            f"dashbored's DAU patch needs updating before this PR can include DAU support."
        )
    return match


def _base_entries(source: str) -> list[tuple[str, str]]:
    """Every (program, base_id) in airtable_config.bases, duplicates included.

    Duplicate dict keys are legal Python, so a second entry for a program does
    not fail to compile — it silently shadows the real one. Reading the pairs
    out of the AST is the only way to see that happen.
    """
    for node in ast.walk(_parse(source, AIRTABLE_DEFINITIONS_PATH)):
        if not (isinstance(node, ast.Call) and _called_name(node.func) == "AirtableServiceConfig"):
            continue
        for kw in node.keywords:
            if kw.arg == "bases" and isinstance(kw.value, ast.Dict):
                return [
                    (key.value, _kwarg_str(value, "base_id") or "")
                    for key, value in zip(kw.value.keys, kw.value.values)
                    if isinstance(key, ast.Constant) and isinstance(key.value, str)
                ]
    raise ConflictError(
        f"Could not read the airtable_config bases dict out of "
        f"{AIRTABLE_DEFINITIONS_PATH}; dashbored's patch needs updating."
    )


def _parse(source: str, path: str) -> ast.Module:
    try:
        return ast.parse(source)
    except SyntaxError as e:
        raise ConflictError(
            f"'{path}' is not valid Python ({e.msg} on line {e.lineno}), so dashbored "
            f"cannot tell whether this program is already in it."
        ) from e


def _called_name(func: ast.expr) -> str:
    if isinstance(func, ast.Name):
        return func.id
    return func.attr if isinstance(func, ast.Attribute) else ""


def _kwarg_str(node: ast.expr, name: str) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    for kw in node.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None


def airtable_base_conflict(source: str, program: str) -> str | None:
    existing = dict(_base_entries(source))
    if program not in existing:
        return None
    return (
        f"Program '{program}' already exists in {AIRTABLE_DEFINITIONS_PATH} "
        f"(base {existing[program] or 'unknown'}). Adding it again would shadow the "
        f"existing config — pick a different program name, or edit that entry by hand."
    )


def dlt_assets_conflict(source: str, program: str) -> str | None:
    name = f"{program}_assets"
    for stmt in _parse(source, DLT_ASSETS_PATH).body:
        if name in _assigned_names(stmt):
            return (
                f"'{name}' is already defined in {DLT_ASSETS_PATH}. Appending another "
                f"assignment would rebind it and drop the existing {program} sync assets."
            )
    return None


def generated_ids_conflict(source: str, program: str) -> str | None:
    if not re.search(rf"^    class {re.escape(program)}:", source, re.MULTILINE):
        return None
    return (
        f"AirtableIDs already carries a '{program}' class in {GENERATED_IDS_PATH}; "
        f"dashbored will not overwrite it."
    )


def _assigned_names(stmt: ast.stmt) -> set[str]:
    if not isinstance(stmt, ast.Assign):
        return set()
    return {t.id for t in stmt.targets if isinstance(t, ast.Name)}


def patch_airtable_definitions(source: str, generated: dict) -> str:
    """Insert a new AirtableBaseConfig entry into the airtable_config dict."""
    base_config = generated.get("base_config", "")
    if not base_config:
        return source

    program, _, _ = parse_base_config(base_config)
    conflict = airtable_base_conflict(source, program)
    if conflict:
        raise ConflictError(conflict)

    # Insert before the closing "    }\n)" of the bases={...} dict, falling back
    # to the last closing paren of airtable_config.
    anchor = re.search(r"\n    \}\n\)", source) or re.search(r"\n\)\s*\n", source)
    if not anchor:
        raise ConflictError(
            f"Could not find the end of the airtable_config bases dict in "
            f"{AIRTABLE_DEFINITIONS_PATH}; dashbored's patch needs updating."
        )

    patched = (
        _comma_terminated(source[:anchor.start()])
        + "\n"
        + _comma_terminated(base_config.rstrip())
        + source[anchor.start():]
    )

    count = sum(1 for key, _ in _base_entries(patched) if key == program)
    if count != 1:
        raise ConflictError(
            f"Patching {AIRTABLE_DEFINITIONS_PATH} left {count} '{program}' entries in "
            f"airtable_config.bases; dashbored's patch needs updating."
        )
    return patched


def _comma_terminated(block: str) -> str:
    """Give the last dict entry in a block a trailing comma if it lacks one."""
    lines = block.split("\n")
    for i in range(len(lines) - 1, -1, -1):
        code = _strip_comment(lines[i]).rstrip()
        if not code:
            continue
        if not code.endswith((",", "{", "(", "[")):
            lines[i] = code + "," + lines[i][len(code):]
        break
    return "\n".join(lines)


def _strip_comment(line: str) -> str:
    """Drop a trailing # comment, ignoring hashes inside string literals."""
    quote = ""
    i = 0
    while i < len(line):
        c = line[i]
        if quote:
            if c == "\\":
                i += 1
            elif c == quote:
                quote = ""
        elif c in "\"'":
            quote = c
        elif c == "#":
            return line[:i]
        i += 1
    return line


def patch_dlt_assets(source: str, generated: dict) -> str:
    """Append a new create_airtable_sync_assets call to dlt/assets.py.

    dlt/definitions.py collects these with load_assets_from_modules, which picks
    up any module-level list of assets, so the assignment is all that is needed.
    """
    sync_call = generated.get("dlt_sync_assets", "")
    if not sync_call:
        return source

    program = _sync_call_program(sync_call)
    conflict = dlt_assets_conflict(source, program)
    if conflict:
        raise ConflictError(conflict)

    patched = _append_sync_call(source, sync_call)

    count = sum(
        1 for stmt in _parse(patched, DLT_ASSETS_PATH).body
        if f"{program}_assets" in _assigned_names(stmt)
    )
    if count != 1:
        raise ConflictError(
            f"Patching {DLT_ASSETS_PATH} left {count} bindings of '{program}_assets'; "
            f"dashbored's patch needs updating."
        )
    return patched


def _sync_call_program(sync_call: str) -> str:
    match = re.search(r"^(\w+)_assets = create_airtable_sync_assets\(", sync_call, re.MULTILINE)
    if not match:
        raise ConflictError(
            "dashbored generated a dlt sync assignment it cannot read back; "
            "the Airtable code generator and the PR patcher disagree."
        )
    return match.group(1)


def _append_sync_call(source: str, sync_call: str) -> str:
    # Insert after the last create_airtable_sync_assets block so the new one
    # keeps company with its peers rather than landing after unrelated assets.
    last_match = None
    for m in re.finditer(r"\n\w+_assets = create_airtable_sync_assets\(", source):
        last_match = m

    if last_match:
        paren_end = source.find("\n)\n", last_match.start())
        if paren_end != -1:
            insert_at = paren_end + 3
            return source[:insert_at] + f"\n{sync_call}\n" + source[insert_at:]

    return source.rstrip() + f"\n\n{sync_call}\n"


def parse_base_config(base_config: str) -> tuple[str, str, list[tuple[str, str]]]:
    """(program, base_id, [(table, table_id)]) read back out of the generated entry."""
    try:
        module = ast.parse("_ = {\n" + base_config.rstrip().rstrip(",") + ",\n}")
        entry = module.body[0].value
        key = entry.keys[0]
        call = entry.values[0]
        program = key.value
        base_id = _kwarg_str(call, "base_id")
        tables = next(kw.value for kw in call.keywords if kw.arg == "tables")
        table_ids = [
            (name.value, _kwarg_str(config, "table_id"))
            for name, config in zip(tables.keys, tables.values)
        ]
    except (SyntaxError, AttributeError, IndexError, StopIteration) as e:
        raise ConflictError(
            f"dashbored generated an Airtable base config it cannot read back ({e}); "
            f"the Airtable code generator and the PR patcher disagree."
        ) from e

    if not isinstance(program, str) or not base_id or any(not t or not i for t, i in table_ids):
        raise ConflictError(
            "The generated Airtable base config is missing a base id or a table id; "
            "re-run the wizard's base introspection."
        )
    return program, base_id, table_ids


def patch_generated_ids(source: str, generated: dict) -> str:
    """Add the new base's field IDs to AirtableIDs.

    dlt/assets.py resolves getattr(AirtableIDs, base_name).<table>.<field> on every
    materialization to rename Airtable's field names into warehouse columns, so a
    PR that adds a base without adding it here merges an AttributeError. Regenerating
    the file is not an option for the reviewer either: the script authenticates with
    the warehouse's own token, which has no grant on a base the wizard user picked.
    """
    base_config = generated.get("base_config", "")
    if not base_config:
        return source

    program, base_id, table_ids = parse_base_config(base_config)
    conflict = generated_ids_conflict(source, program)
    if conflict:
        raise ConflictError(conflict)

    schema = generated.get("airtable_schema") or generated.get("schema") or []
    if not schema:
        raise ConflictError(
            f"The generated code does not carry the Airtable base schema, so "
            f"AirtableIDs.{program} would have no field IDs and the first sync would fail. "
            f"dashbored needs generated['airtable_schema'] to build this PR."
        )

    block = build_generated_ids_block(program, base_id, table_ids, schema)

    marker = "\n# fmt: on"
    at = source.rfind(marker)
    if at == -1:
        if "class AirtableIDs:" not in source:
            raise ConflictError(
                f"{GENERATED_IDS_PATH} does not look like the generated AirtableIDs file; "
                f"dashbored's patch needs updating."
            )
        patched = source.rstrip("\n") + "\n\n" + block
    else:
        patched = source[:at + 1] + block + source[at + 1:]

    try:
        compile(patched, GENERATED_IDS_PATH, "exec")
    except SyntaxError as e:
        raise ConflictError(
            f"The patched {GENERATED_IDS_PATH} does not compile ({e.msg} on line {e.lineno}); "
            f"dashbored's patch needs updating."
        ) from e
    return patched


def build_generated_ids_block(
    program: str, base_id: str, table_ids: list[tuple[str, str]], schema: list[dict],
) -> str:
    """One AirtableIDs.<base> class, spelled the way generate_airtable_ids.py spells it."""
    by_table_id = {t.get("table_id"): t for t in schema if t.get("table_id")}
    _check_airtable_id(base_id, "base")
    _check_class_name(program, "The program name", "pick a different program name")

    lines = [
        f"    class {program}:",
        f"        \"\"\"IDs for Base '{program}' (ID: {base_id})\"\"\"",
        f'        BASE_ID = "{base_id}"',
        "",
    ]
    for table, table_id in table_ids:
        info = by_table_id.get(table_id)
        if info is None:
            raise ConflictError(
                f"dashbored has no field IDs for table '{table}' ({table_id}), so the "
                f"AirtableIDs entry the dlt sync reads would be empty. Re-run the wizard "
                f"so it re-reads the base schema."
            )
        _check_airtable_id(table_id, f"table '{table}'")
        _check_class_name(
            table,
            f"The Airtable table '{info.get('name') or table}'",
            "rename the table in Airtable",
        )
        lines += [
            f"        class {table}:",
            f"            \"\"\"IDs for Table '{table}' (ID: {table_id})\"\"\"",
            f'            TABLE_ID = "{table_id}"',
            "",
        ]
        seen: dict[str, str] = {}
        lines += [_field_constant(table, column, seen) for column in info.get("columns", [])]
        lines.append("")

    # Two trailing blank lines: what the generator script leaves between bases,
    # so re-running it against the merged config produces no diff.
    lines += ["", ""]
    return "\n".join(lines)


def _field_constant(table: str, column: dict, seen: dict[str, str]) -> str:
    name = column.get("name", "")
    field_id = column.get("field_id")
    const = sanitize_name(name)
    if not field_id:
        raise ConflictError(
            f"Field '{name}' on table '{table}' has no Airtable field ID, so the dlt sync "
            f"could not rename it. Re-run the wizard so it re-reads the base schema."
        )
    if not const.isidentifier() or keyword.iskeyword(const):
        raise ConflictError(
            f"Field '{name}' on table '{table}' becomes '{const}', which cannot be a "
            f"generated_ids constant. Rename it in Airtable, then re-run the wizard."
        )
    # Two Airtable field names can sanitize to one Python name ('id' and 'ID'), and
    # the later assignment simply wins — the loser's column keeps its Airtable name
    # forever, because dlt/assets.py renames by looking the constant up here.
    if const in seen and seen[const] != name:
        raise ConflictError(
            f"Fields '{seen[const]}' and '{name}' on table '{table}' both become the "
            f"generated_ids constant '{const}', so only one of them would ever be renamed "
            f"in the warehouse. Rename one in Airtable, then re-run the wizard."
        )
    seen[const] = name
    _check_airtable_id(field_id, f"field '{const}' on table '{table}'")
    return f'            {const} = "{field_id}"  # Name: {_one_line(name)}'


def _check_class_name(name: str, subject: str, fix: str) -> None:
    """generated_ids.py spells bases and tables as nested classes.

    A name that sanitizes to `class` or `None` is a SyntaxError there, and the
    only thing downstream is the compile check, whose message blames dashbored
    for something only the user can fix.
    """
    if not (name or "").isidentifier() or keyword.iskeyword(name):
        raise ConflictError(
            f"{subject} cannot be a class name in {GENERATED_IDS_PATH} "
            f"('{name}' is not a usable Python name) — {fix}, then re-run the wizard."
        )


def _check_airtable_id(value: str, what: str) -> None:
    """Airtable IDs land in a string literal and a docstring, unquoted.

    They come off the meta API for a base the wizard user chose, so anything that
    is not the documented app/tbl/fld + alphanumerics shape is either Airtable
    changing its format or someone trying to write Python into generated_ids.py.
    """
    if not re.fullmatch(r"[A-Za-z0-9]+", value or ""):
        raise ConflictError(
            f"Airtable returned {what} ID {value!r}, which is not an Airtable ID. dashbored "
            f"will not write it into {GENERATED_IDS_PATH}."
        )


def _one_line(name: str) -> str:
    """A field name is echoed after a `#`; a newline in it would end the comment."""
    return re.sub(r"\s+", " ", name).strip()


class _EnvVarInserter(cst.CSTTransformer):
    def __init__(self, env_var: str):
        self.env_var = env_var
        self._in_target = False
        self.done = False

    def visit_Assign(self, node: cst.Assign) -> bool:
        for target in node.targets:
            if isinstance(target.target, cst.Name):
                if target.target.value == "_SLING_CONNECTION_URL_ENV_VARS":
                    self._in_target = True
        return True

    def leave_Assign(self, original: cst.Assign, updated: cst.Assign) -> cst.Assign:
        self._in_target = False
        return updated

    def leave_List(self, original: cst.List, updated: cst.List) -> cst.List:
        if not self._in_target:
            return updated

        elements = list(updated.elements)
        if not elements:
            elements.append(cst.Element(
                value=cst.SimpleString(f'"{self.env_var}"'),
            ))
            self._in_target = False
            self.done = True
            return updated.with_changes(elements=elements)

        ref_comma = elements[0].comma
        new_el = cst.Element(
            value=cst.SimpleString(f'"{self.env_var}"'),
            comma=ref_comma,
        )
        elements.insert(-1, new_el)

        self._in_target = False
        self.done = True
        return updated.with_changes(elements=elements)


class _ConnectionsListInserter(cst.CSTTransformer):
    def __init__(self, var_name: str):
        self.var_name = var_name
        self._in_target = False
        self.done = False

    def visit_Assign(self, node: cst.Assign) -> bool:
        for target in node.targets:
            if isinstance(target.target, cst.Name):
                if target.target.value == "sling_replication_resource":
                    self._in_target = True
        return True

    def leave_Assign(self, original: cst.Assign, updated: cst.Assign) -> cst.Assign:
        self._in_target = False
        return updated

    def leave_List(self, original: cst.List, updated: cst.List) -> cst.List:
        if not self._in_target:
            return updated

        elements = list(updated.elements)
        if not elements:
            elements.append(cst.Element(value=cst.Name(self.var_name)))
            self._in_target = False
            self.done = True
            return updated.with_changes(elements=elements)

        ref_comma = elements[0].comma
        new_el = cst.Element(
            value=cst.Name(self.var_name),
            comma=ref_comma,
        )

        insert_idx = len(elements) - 1
        for i, el in enumerate(elements):
            if isinstance(el.value, cst.Name) and el.value.value == "warehouse_db_connection":
                insert_idx = i
                break
        elements.insert(insert_idx, new_el)

        self._in_target = False
        self.done = True
        return updated.with_changes(elements=elements)



class _AssetsListInserter(cst.CSTTransformer):
    def __init__(self, entry: str):
        self.entry = entry
        self._in_assets_kwarg = False
        self.done = False

    def visit_Arg(self, node: cst.Arg) -> bool:
        if isinstance(node.keyword, cst.Name) and node.keyword.value == "assets":
            self._in_assets_kwarg = True
        return True

    def leave_Arg(self, original: cst.Arg, updated: cst.Arg) -> cst.Arg:
        self._in_assets_kwarg = False
        return updated

    def leave_List(self, original: cst.List, updated: cst.List) -> cst.List:
        if not self._in_assets_kwarg or self.done:
            return updated

        elements = list(updated.elements)
        if not elements:
            elements.append(cst.Element(value=cst.Name(self.entry)))
            self.done = True
            return updated.with_changes(elements=elements)

        ref_comma = elements[0].comma
        new_el = cst.Element(
            value=cst.Name(self.entry),
            comma=ref_comma,
        )
        elements.insert(-1, new_el)
        self.done = True
        return updated.with_changes(elements=elements)


def _insert_before_assignment(
    tree: cst.Module, target_name: str, code: str,
) -> cst.Module:
    body = list(tree.body)
    new_stmts = cst.parse_module(code + "\n\n").body

    for i, stmt in enumerate(body):
        names = _assignment_names(stmt)
        if target_name in names:
            for j, new_stmt in enumerate(new_stmts):
                body.insert(i + j, new_stmt)
            return tree.with_changes(body=body)

    for new_stmt in new_stmts:
        body.insert(-1, new_stmt)
    return tree.with_changes(body=body)


def _assignment_names(stmt: cst.BaseStatement) -> set[str]:
    names: set[str] = set()
    if isinstance(stmt, cst.SimpleStatementLine):
        for item in stmt.body:
            if isinstance(item, cst.Assign):
                for target in item.targets:
                    if isinstance(target.target, cst.Name):
                        names.add(target.target.value)
    return names


def _compile_check(patched: str, label: str):
    compile(patched, label, "exec")


def _yaml_check(patched: str, label: str):
    yaml.safe_load(patched)


def validate_locally(repo_root: Path | None, program: str, generated: dict) -> dict:
    if repo_root is None:
        return {}

    if generated.get("source_type") == "airtable":
        checks = [
            ("airtable/definitions.py", AIRTABLE_DEFINITIONS_PATH,
             lambda s: patch_airtable_definitions(s, generated), _compile_check),
            ("airtable/generated_ids.py", GENERATED_IDS_PATH,
             lambda s: patch_generated_ids(s, generated), _compile_check),
            ("dlt/assets.py", DLT_ASSETS_PATH,
             lambda s: patch_dlt_assets(s, generated), _compile_check),
        ]
    else:
        checks = [
            ("assets.py", SLING_ASSETS_PATH,
             lambda s: patch_sling_assets(s, program, generated), _compile_check),
            ("definitions.py", SLING_DEFINITIONS_PATH,
             lambda s: patch_definitions_py(s, program, generated), _compile_check),
        ]

    checks.append(
        ("sources.yml", SOURCES_YML_PATH, lambda s: patch_sources_yml(s, generated), _yaml_check)
    )
    if wants_dau(generated):
        checks.append(
            (DAU_MODEL_PATH.rsplit("/", 1)[-1], DAU_MODEL_PATH,
             lambda s: patch_dau_sql(s, program, generated), _dau_check)
        )

    results = {}
    for label, relpath, patch, check in checks:
        path = repo_root / relpath
        if not path.exists():
            continue
        try:
            patched = patch(path.read_text())
            if check:
                check(patched, label)
            results[label] = {"ok": True, "lines": patched.count("\n")}
        except Exception as e:
            results[label] = {"ok": False, "error": str(e)}

    return results
