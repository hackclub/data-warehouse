import datetime
import ipaddress
import re
import socket
import threading
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from flask import Blueprint, jsonify, request, session, current_app
from .auth import _authorized
from .services.dau_preview import DauPreviewService
from .services.dau_sql import GeneratorError

bp = Blueprint("api", __name__)

CONNECT_FAILED = (
    "Could not connect to that database. Check the host, port, credentials, "
    "and that it accepts connections from the public internet."
)


def _json_body():
    body = request.get_json(silent=True, force=True)
    return body if isinstance(body, dict) else {}


def _wizard_state():
    wid = request.args.get("wid") or request.form.get("wid") or ""
    if not wid:
        wid = _json_body().get("wid") or ""
    wizards = session.get("wizards", {})
    return wizards.get(wid, {})


@bp.before_request
def require_auth():
    if not session.get("user") or not session.get("github_token"):
        return jsonify({"error": "Not authenticated"}), 401
    if not _authorized():
        return jsonify({"error": "Not authorized"}), 403


ALLOWED_DB_SCHEMES = frozenset(["postgres", "postgresql"])
ALLOWED_DB_PARAMS = frozenset(["sslmode", "application_name"])
DNS_TIMEOUT = 3
PRIVATE_HOST_ERROR = (
    "Connection URL must point at a publicly routable host, not an internal "
    "address. On a development machine, set ALLOW_PRIVATE_DB_HOSTS=1 to allow it."
)


def _resolve_host(host, port):
    """Every address for host, [] if it doesn't resolve, None if DNS timed out.

    getaddrinfo takes no timeout of its own, so a host delegated to a
    black-holing nameserver would otherwise pin a worker thread for the whole
    resolver deadline before any connect_timeout could apply.
    """
    answer = {}

    def lookup():
        try:
            answer["addresses"] = [
                ai[4][0] for ai in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
            ]
        except (OSError, UnicodeError):
            answer["addresses"] = []

    thread = threading.Thread(target=lookup, daemon=True)
    thread.start()
    thread.join(DNS_TIMEOUT)
    return answer.get("addresses")


def _is_public(addr):
    try:
        ip = ipaddress.ip_address(addr.split("%")[0])
    except ValueError:
        return False
    return ip.is_global and not ip.is_multicast


def checked_connection_url(url: str) -> tuple[str | None, str | None]:
    """(url to dial, error) for a user-supplied postgres URL.

    This URL is dialled from inside the production network, so without this the
    endpoint is a port scanner for the internal estate. Two things the host in
    the URL does not tell you: libpq also takes host/hostaddr/service out of the
    query string, which urlsplit ignores, so only harmless parameters survive;
    and the name could resolve somewhere else by the time psycopg2 dials it, so
    the address that was actually vetted is pinned with hostaddr.
    """
    parts = urlsplit((url or "").strip())
    if parts.scheme not in ALLOWED_DB_SCHEMES:
        return None, "Connection URL must start with postgres:// or postgresql://"

    try:
        host, port = parts.hostname, parts.port
    except ValueError:
        return None, "Connection URL has an invalid port."
    if not host:
        return None, "Connection URL is missing a host."

    params = parse_qsl(parts.query, keep_blank_values=True)
    rejected = sorted({k for k, _ in params} - ALLOWED_DB_PARAMS)
    if rejected:
        return None, f"Connection URL parameters not allowed: {', '.join(rejected)}"

    if current_app.config.get("ALLOW_PRIVATE_DB_HOSTS"):
        return urlunsplit(parts), None

    addresses = _resolve_host(host, port or 5432)
    if addresses is None:
        return None, f"Timed out resolving host '{host}'."
    if not addresses:
        return None, f"Could not resolve host '{host}'."
    if not all(_is_public(addr) for addr in addresses):
        return None, PRIVATE_HOST_ERROR

    params.append(("hostaddr", addresses[0]))
    return urlunsplit(parts._replace(query=urlencode(params))), None


@bp.route("/preview_dau", methods=["POST"])
def preview_dau():
    from .auth import refresh_airtable_token

    ws = _wizard_state()
    if ws.get("source_type") == "airtable":
        token = session.get("airtable_token")
        preview = DauPreviewService(ws, airtable_token=token).preview()
        if preview.get("error") and "401" in str(preview.get("error", "")):
            refreshed = refresh_airtable_token()
            if refreshed:
                preview = DauPreviewService(ws, airtable_token=refreshed).preview()
        return jsonify(preview)

    dial_url = None
    if ws.get("connection_url"):
        # The session holds the URL as typed; the address vetted at introspection
        # time is not pinned in it, and this endpoint dials from inside the
        # production network. Re-check and hand the service the pinned address.
        dial_url, invalid = checked_connection_url(ws["connection_url"])
        if invalid:
            return jsonify({"error": invalid}), 422

    return jsonify(DauPreviewService(ws, dial_url=dial_url).preview())


@bp.route("/test_sql", methods=["POST"])
def test_sql():
    ws = _wizard_state()
    if not ws.get("connection_url"):
        return jsonify({"error": "No database connection configured"}), 422

    sql = (_json_body().get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "No SQL provided"}), 422

    rejected = _reject_sql(sql)
    if rejected:
        return jsonify({"error": rejected}), 422

    try:
        resolved = _resolve_sources(sql, ws)
    except GeneratorError as e:
        return jsonify({"error": str(e)}), 422

    cte_names = re.findall(r"(\w+)\s+AS\s*\(", resolved, re.IGNORECASE)
    if not cte_names:
        return jsonify({"error": "Could not find any CTE definitions (expected: name AS (...))"}), 422

    trimmed = resolved.rstrip().rstrip(",")
    test_query = f"WITH {trimmed} SELECT * FROM {cte_names[-1]} LIMIT 20"

    dial_url, invalid = checked_connection_url(ws["connection_url"])
    if invalid:
        return jsonify({"error": invalid}), 422

    try:
        conn = _connect(dial_url)
    except Exception:
        current_app.logger.exception("test query connection failed")
        return jsonify({"error": CONNECT_FAILED}), 422

    try:
        columns, rows = _run_readonly(conn, test_query)
        return jsonify({"columns": columns, "rows": rows})
    except Exception as e:
        error_msg = str(e).replace(dial_url, "[redacted]").replace(ws["connection_url"], "[redacted]")
        return jsonify({"error": error_msg}), 422
    finally:
        conn.close()


_SQL_BLOCKLIST = [
    "pg_read_file", "pg_read_binary_file", "pg_ls_dir",
    "pg_shadow", "pg_authid", "pg_user_mappings",
    "lo_import", "lo_export", "lo_get",
    "copy", "commit", "rollback", "begin",
    "set", "set_config", "reset", "drop", "alter", "truncate",
    "insert", "update", "delete", "grant", "revoke",
    "pg_sleep", "pg_terminate_backend", "pg_cancel_backend",
]

_SQL_BLOCKED = re.compile(r"\b(" + "|".join(_SQL_BLOCKLIST) + r")\b", re.IGNORECASE)

_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")

# In an E'...' string a backslash escapes the next character, including a quote;
# in every other string it is an ordinary character.
_ESCAPE_PREFIX = re.compile(r"(?:^|[^A-Za-z0-9_$])[Ee]\Z")


def _blank_sql_literals(sql: str) -> str | None:
    """Replace strings, quoted identifiers, dollar-quoted bodies and comments
    with spaces so what's left is pure syntax. None if anything is unterminated.
    """
    out = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        dollar = _DOLLAR_TAG.match(sql, i)
        if ch in "'\"":
            escapes = ch == "'" and _ESCAPE_PREFIX.search(sql, 0, i) is not None  # E'...'
            j = i + 1
            while j < n:
                if escapes and sql[j] == "\\" and j + 1 < n:
                    j += 2
                elif sql[j] != ch:
                    j += 1
                elif j + 1 < n and sql[j + 1] == ch:
                    j += 2
                else:
                    break
            if j >= n:
                return None
            j += 1
        elif dollar:
            tag = dollar.group(0)
            end = sql.find(tag, i + len(tag))
            if end == -1:
                return None
            j = end + len(tag)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
        elif sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif sql.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            if depth:
                return None
        else:
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            out.append(sql[i:j])
        else:
            out.append(" " * (j - i))
        i = j
    return "".join(out)


def _reject_sql(sql: str) -> str | None:
    """Gate for user SQL spliced into a WITH clause.

    The load-bearing control is that only a single statement survives: psycopg2
    happily runs semicolon-separated statements, and a second statement is not
    covered by the read-only session. The blocklist is defence in depth, applied
    to the literal-stripped text so it can't be dodged by hiding in a comment.
    """
    bare = _blank_sql_literals(sql)
    if bare is None:
        return "SQL has an unterminated string literal or comment"
    if ";" in bare:
        return "SQL must be a single statement — remove the ';'"
    blocked = _SQL_BLOCKED.search(bare)
    if blocked:
        return f"SQL contains blocked keyword: {blocked.group(1)}"
    return None


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _relation(schema, name):
    if schema == "public":
        return _quote_ident(name)
    return f"{_quote_ident(schema)}.{_quote_ident(name)}"


def _resolve_sources(sql, ws):
    """Point every {{ source(...) }} at the relation it reads in the program's DB.

    The generated SQL addresses tables by their warehouse name, and the
    generator renames a table whose bare name collides across schemas to
    schema_table — so a map keyed only by bare name misses exactly the tables
    schema qualification exists for, and correct SQL comes back as a
    relation-does-not-exist. Ask the generator for the names it emits.
    """
    from .wizard import _generator
    gen = _generator(ws)
    table_map = {
        t["name"]: _relation(t.get("schema") or "public", t["name"])
        for t in ws.get("schema") or []
    }
    for key in gen._selected_tables():
        wh_name = gen._table_name(key)
        source_name = gen._ident(key)[1]
        table_map[wh_name] = _relation(*gen._ident(key))
        if wh_name != source_name:
            table_map.pop(source_name, None)

    def replace_source(match):
        table_name = match.group(2)
        return table_map.get(table_name, _quote_ident(table_name))

    pattern = r"\{\{\s*source\(\s*['\"]([^'\"]*?)['\"]\s*,\s*['\"]([^'\"]*?)['\"]\s*\)\s*\}\}"
    return re.sub(pattern, replace_source, sql)


def _connect(connection_url):
    """Kept apart from the query so a connect failure never reaches the user:
    refused-vs-timeout-vs-auth-failed is exactly the oracle an SSRF wants.
    """
    import psycopg2

    conn = psycopg2.connect(connection_url, connect_timeout=5)
    conn.set_session(readonly=True, autocommit=False)
    return conn


def _run_readonly(conn, query):
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '5000'", ())
        cur.execute(query, ())
        columns = [desc[0] for desc in cur.description]
        rows = [[_serialize(v) for v in row] for row in cur.fetchall()]
        return columns, rows


def _serialize(value):
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return str(value)


@bp.route("/validate_code", methods=["POST"])
def validate_code():
    """Dry-run the generated patches against the upstream sources.

    Always answers with exactly one of:
      {"files": {path: {"ok": bool, ...}}}  — non-empty, every file was checked
      {"unavailable": reason}               — nothing could be checked
      {"error": reason}                     — bad request (422)

    A partial result set is never a pass either: validate_locally skips
    upstream files it cannot find, so a half-populated checkout would report
    the one file it did check as green while the compile checks that catch
    broken generated Python were silently never run.
    """
    from .services import github_pr_creator as gpc
    from .wizard import _generator

    ws = _wizard_state()
    if not ws.get("program_name"):
        return jsonify({"error": "No wizard state found"}), 422

    repo_root = current_app.config["REPO_ROOT"]
    if not repo_root:
        return jsonify({"unavailable": "no orpheus-engine checkout on this host to validate against"})

    upstream = [
        gpc.SLING_ASSETS_PATH, gpc.SLING_DEFINITIONS_PATH,
        gpc.AIRTABLE_DEFINITIONS_PATH, gpc.DLT_ASSETS_PATH,
        gpc.GENERATED_IDS_PATH, gpc.SOURCES_YML_PATH, gpc.DAU_MODEL_PATH,
    ]
    missing = [p for p in upstream if not (repo_root / p).exists()]
    if missing:
        return jsonify({"unavailable": f"this checkout is missing {', '.join(missing)}"})

    try:
        generated = _generator(ws).generate()
    except GeneratorError as e:
        return jsonify({"error": str(e)}), 422
    results = gpc.validate_locally(repo_root, ws["program_name"], generated)
    if not results:
        return jsonify({"unavailable": "the upstream sources this patch targets are not present on this host"})
    return jsonify({"files": results})
