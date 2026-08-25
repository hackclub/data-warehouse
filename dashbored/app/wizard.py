import json
import keyword
import re
import uuid
from urllib.parse import urlsplit

import requests
from flask import Blueprint, render_template, redirect, url_for, session, flash, request, g, current_app
from .api import checked_connection_url, CONNECT_FAILED
from .auth import _authorized, refresh_airtable_token
from .services.db_introspector import DbIntrospector, ConnectionError as DbConnectionError
from .services.airtable_introspector import AirtableIntrospector, AirtableError
from .services.code_generator import CodeGenerator
from .services.airtable_code_generator import AirtableCodeGenerator
from .services.github_pr_creator import GithubPrCreator, AirtablePrCreator, ConflictError
from .services.dau_sql import GeneratorError
from .services.sops_encryptor import EncryptionError, encrypt as sops_encrypt

bp = Blueprint("wizard", __name__)

_NAME_STRIP = re.compile(r"[^a-z0-9_-]")
_NAME_RUNS = re.compile(r"_+")
# script/add_env_var.py refuses an env var over ENV_VAR_MAX_LEN, and the name it
# reads is f"{program_name.upper()}_DATABASE_URL".
MAX_PROGRAM_NAME = 64 - len("_DATABASE_URL")
MAX_WIZARDS = 5
SENSITIVE_PREFIX = "sensitive_columns["


@bp.before_request
def require_auth():
    if not session.get("user"):
        flash("Please sign in to continue.", "alert")
        return redirect(url_for("auth.login"))
    if not session.get("github_token"):
        flash("GitHub connection required.", "alert")
        return redirect(url_for("auth.login"))
    if not _authorized():
        session.clear()
        flash("Your access has been revoked. Sign in again if that's a mistake.", "alert")
        return redirect(url_for("auth.login"))


def _wid():
    if hasattr(g, "wizard_wid"):
        return g.wizard_wid
    return request.args.get("wid") or request.form.get("wid") or ""


def _url(endpoint, wid=None, **kwargs):
    wid = wid or _wid()
    if wid:
        kwargs["wid"] = wid
    return url_for(endpoint, **kwargs)


def wizard_state(create=False):
    """The state for this request's wid, or an empty dict a read can't persist to.

    Each entry holds a whole introspected schema and the session is re-read and
    re-written per request, so only a write mints one: otherwise every bare
    /wizard/ visit and every expired-link retry left another schema behind
    forever. Least-recently-written entries are evicted past MAX_WIZARDS.
    """
    wizards = session.get("wizards") or {}
    wid = _wid()
    if wid not in wizards:
        if not create:
            g.wizard_wid = wid
            return {}
        wid = wid or str(uuid.uuid4())[:8]
    g.wizard_wid = wid
    if create:
        wizards[wid] = wizards.pop(wid, {})
        for stale in list(wizards)[:-MAX_WIZARDS]:
            del wizards[stale]
        session["wizards"] = wizards
        session.modified = True
    return wizards[wid]


def save_wizard(**updates):
    ws = wizard_state(create=True)
    ws.update(updates)
    session.modified = True


def _is_airtable():
    return wizard_state().get("source_type") == "airtable"


def _generator(ws):
    if ws.get("source_type") == "airtable":
        return AirtableCodeGenerator(ws)
    return CodeGenerator(ws)


@bp.route("/reset", methods=["POST"])
def reset():
    wid = _wid()
    wizards = session.get("wizards", {})
    wizards.pop(wid, None)
    session.modified = True
    return redirect(url_for("wizard.setup"))


@bp.route("/")
def setup():
    return render_template("wizard/setup.html", ws=wizard_state(), wid=_wid())


def _normalize_program_name(raw):
    """The program name is interpolated straight into generated Python
    identifiers, so it has to be a legal one before it goes anywhere near a fork.

    Lowercase, drop anything outside [a-z0-9_-], hyphens become underscores,
    then collapse runs of underscores and trim the ends so that
    f"{name.upper()}_DATABASE_URL" also satisfies script/add_env_var.py's
    ENV_VAR_RE. A name it rejects means a merged PR whose secret can never be
    installed, which costs the user the whole wizard.

    The setup form's live preview does not do the collapse/trim half, so an
    accepted name can differ from what it showed; the value stored here is the
    one that counts.
    """
    name = _NAME_STRIP.sub("", (raw or "").lower()).replace("-", "_")
    name = _NAME_RUNS.sub("_", name).strip("_")
    if not name or len(name) > MAX_PROGRAM_NAME:
        return None
    if not name.isidentifier() or keyword.iskeyword(name) or keyword.issoftkeyword(name):
        return None
    return name


@bp.route("/introspect", methods=["POST"])
def introspect():
    program_name = _normalize_program_name(request.form.get("program_name"))
    source_type = request.form.get("source_type", "postgres")

    if not program_name:
        flash(
            "Program name must contain letters, digits, underscores or hyphens, "
            "must not start with a digit, can't be a Python keyword, and can't be "
            f"longer than {MAX_PROGRAM_NAME} characters.",
            "alert",
        )
        return redirect(_url("wizard.setup"))

    save_wizard(program_name=program_name, source_type=source_type)

    if source_type == "airtable":
        if not session.get("airtable_token"):
            session["wizard_wid"] = _wid()
            return redirect(url_for("auth.airtable_auth"))
        return redirect(_url("wizard.airtable_bases"))

    return _introspect_postgres(program_name)


def _introspect_postgres(program_name):
    connection_url = (request.form.get("connection_url") or "").strip()
    target_schema = (request.form.get("target_schema") or "").strip()

    dial_url, invalid = checked_connection_url(connection_url)
    if invalid:
        save_wizard(target_schema=target_schema, step="setup")
        flash(invalid, "alert")
        return redirect(_url("wizard.setup"))

    save_wizard(connection_url=connection_url, target_schema=target_schema)

    try:
        schema = DbIntrospector(dial_url).introspect()
        save_wizard(schema=schema, step="tables")
        return redirect(_url("wizard.tables"))
    except DbConnectionError:
        current_app.logger.exception("introspection failed")
        save_wizard(step="setup")
        flash(CONNECT_FAILED, "alert")
        return redirect(_url("wizard.setup"))


@bp.route("/airtable-bases")
def airtable_bases():
    token = session.get("airtable_token")
    if not token:
        flash("Connect to Airtable first.", "alert")
        return redirect(_url("wizard.setup"))

    try:
        bases = AirtableIntrospector(token).list_bases()
    except AirtableError:
        refreshed = refresh_airtable_token()
        if refreshed:
            try:
                bases = AirtableIntrospector(refreshed).list_bases()
            except AirtableError as e:
                session.pop("airtable_token", None)
                flash(str(e), "alert")
                return redirect(_url("wizard.setup"))
        else:
            session.pop("airtable_token", None)
            flash("Airtable session expired — please reconnect.", "alert")
            return redirect(_url("wizard.setup"))

    return render_template("wizard/airtable_bases.html", bases=bases, wid=_wid())


@bp.route("/airtable-bases", methods=["POST"])
def pick_base():
    base_id = request.form.get("base_id", "").strip()
    base_name = request.form.get("base_name", "").strip()

    if not base_id:
        flash("No base selected.", "alert")
        return redirect(_url("wizard.airtable_bases"))

    save_wizard(airtable_base_id=base_id, airtable_base_name=base_name)

    token = session.get("airtable_token")
    try:
        schema = AirtableIntrospector(token).introspect(base_id)
    except AirtableError:
        refreshed = refresh_airtable_token()
        if refreshed:
            try:
                schema = AirtableIntrospector(refreshed).introspect(base_id)
            except AirtableError as e:
                flash(str(e), "alert")
                return redirect(_url("wizard.airtable_bases"))
        else:
            session.pop("airtable_token", None)
            flash("Airtable session expired — please reconnect.", "alert")
            return redirect(_url("wizard.setup"))
    save_wizard(schema=schema, step="tables")
    return redirect(_url("wizard.tables"))


@bp.route("/tables")
def tables():
    ws = wizard_state()
    if not ws.get("schema"):
        return redirect(_url("wizard.setup"))
    return render_template("wizard/tables.html", ws=ws, wid=_wid())


def _sensitive_table(key):
    """The table `sensitive_columns[schema.table][]` names, or None.

    Splitting on "[" truncates any table whose own name contains a bracket —
    legal quoted in Postgres, ordinary in Airtable — and the truncated key
    matches no table, so the generator would emit a stream with no select list
    and replicate the columns the user just clicked to exclude.
    """
    body = key[len(SENSITIVE_PREFIX):]
    for suffix in ("][]", "]"):
        if body.endswith(suffix):
            return body[: -len(suffix)]
    return None


@bp.route("/tables", methods=["POST"])
def update_tables():
    tables = request.form.getlist("tables[]")
    sensitive = {}
    for key in request.form:
        if key.startswith(SENSITIVE_PREFIX):
            table_name = _sensitive_table(key)
            if table_name is None:
                current_app.logger.error("unparseable sensitive_columns key %r", key)
                flash(
                    "Couldn't read which columns to exclude for one of those tables, "
                    "so nothing was saved. Try again.",
                    "alert",
                )
                return redirect(_url("wizard.tables"))
            sensitive[table_name] = request.form.getlist(key)

    if _is_airtable():
        save_wizard(selected_tables=tables, sensitive_columns=sensitive, step="dau")
        return redirect(_url("wizard.dau"))

    save_wizard(selected_tables=tables, sensitive_columns=sensitive, step="sync_config")
    return redirect(_url("wizard.sync_config"))


@bp.route("/sync")
def sync_config():
    ws = wizard_state()
    if _is_airtable():
        return redirect(_url("wizard.dau"))
    if not ws.get("selected_tables"):
        return redirect(_url("wizard.tables"))
    return render_template("wizard/sync_config.html", ws=ws, wid=_wid())


@bp.route("/sync", methods=["POST"])
def update_sync_config():
    raw = request.form.get("sync_configs", "{}")
    try:
        sync_configs = json.loads(raw)
    except json.JSONDecodeError:
        sync_configs = {}

    target_schema = (request.form.get("target_schema") or "").strip()
    if target_schema:
        save_wizard(sync_configs=sync_configs, target_schema=target_schema, step="dau")
    else:
        save_wizard(sync_configs=sync_configs, step="dau")
    return redirect(_url("wizard.dau"))


@bp.route("/dau")
def dau():
    ws = wizard_state()
    if not _is_airtable():
        if not ws.get("sync_configs") and ws.get("sync_configs") != {}:
            return redirect(_url("wizard.sync_config"))
    elif not ws.get("selected_tables"):
        return redirect(_url("wizard.tables"))
    return render_template("wizard/dau.html", ws=ws, wid=_wid())


@bp.route("/dau", methods=["POST"])
def update_dau():
    raw = request.form.get("dau_config", "{}")
    try:
        dau_config = json.loads(raw)
    except json.JSONDecodeError:
        flash("Invalid DAU configuration — try again.", "alert")
        return redirect(_url("wizard.dau"))

    if not isinstance(dau_config, dict):
        flash("Invalid DAU configuration — try again.", "alert")
        return redirect(_url("wizard.dau"))

    has_method = (
        dau_config.get("uses_hackatime")
        or dau_config.get("custom_table")
        or (dau_config.get("use_custom_sql") and (dau_config.get("custom_sql") or "").strip())
    )
    if not has_method:
        flash("Select at least one time tracking method or write custom SQL.", "alert")
        return redirect(_url("wizard.dau"))

    save_wizard(dau_config=dau_config, step="dau_preview")
    return redirect(_url("wizard.dau_preview"))


@bp.route("/dau-preview")
def dau_preview():
    ws = wizard_state()
    if not ws.get("dau_config"):
        return redirect(_url("wizard.dau"))
    return render_template("wizard/dau_preview.html", ws=ws, wid=_wid())


def _expired():
    flash("That wizard session is gone — start a new one.", "alert")
    return redirect(url_for("wizard.setup"))


@bp.route("/review")
def review():
    ws = wizard_state()
    if not ws.get("program_name"):
        return _expired()
    try:
        generated = _generator(ws).generate()
    except GeneratorError as e:
        flash(str(e), "alert")
        return redirect(_url(e.step))
    return render_template("wizard/review.html", ws=ws, generated=generated, wid=_wid())


def _github_failure(e: requests.RequestException) -> str:
    """Whatever GitHub said, plus which call said it — never a bare 500."""
    resp = e.response
    if resp is None:
        return f"Could not reach GitHub: {e}"

    try:
        body = resp.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    detail = body.get("message") or resp.text[:200].strip() or "no detail given"
    extra = "; ".join(
        x["message"] for x in body.get("errors") or [] if isinstance(x, dict) and x.get("message")
    )
    if extra:
        detail = f"{detail} ({extra})"
    step = f"{resp.request.method} {urlsplit(resp.url).path}"
    return f"GitHub returned {resp.status_code} on {step}: {detail}"


@bp.route("/pr", methods=["POST"])
def create_pr():
    ws = wizard_state()
    if not ws.get("program_name"):
        return _expired()
    try:
        generated = _generator(ws).generate()
    except GeneratorError as e:
        flash(str(e), "alert")
        return redirect(_url(e.step))

    try:
        if _is_airtable():
            pr = AirtablePrCreator(
                token=session["github_token"],
                program_name=ws["program_name"],
                generated=generated,
                user=session["user"],
            ).create()
        else:
            connection_url = ws.get("connection_url", "")
            if not connection_url:
                flash("Connection URL is missing — go back and enter it again.", "alert")
                return redirect(_url("wizard.setup"))
            encrypted_url = sops_encrypt(connection_url)
            pr = GithubPrCreator(
                token=session["github_token"],
                program_name=ws["program_name"],
                generated=generated,
                encrypted_url=encrypted_url,
                user=session["user"],
            ).create()
    except (ConflictError, EncryptionError) as e:
        flash(str(e), "alert")
        return redirect(_url("wizard.review"))
    except requests.RequestException as e:
        current_app.logger.exception("PR creation failed")
        flash(_github_failure(e), "alert")
        return redirect(_url("wizard.review"))

    ws.pop("connection_url", None)
    session.pop("airtable_token", None)
    session.pop("airtable_refresh_token", None)
    save_wizard(pr_url=pr["html_url"], step="done")
    return redirect(_url("wizard.done"))


@bp.route("/done")
def done():
    ws = wizard_state()
    if not ws.get("pr_url"):
        return redirect(_url("wizard.setup"))
    return render_template("wizard/done.html", ws=ws, wid=_wid())
