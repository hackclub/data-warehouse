"""
PostgreSQL connection handling with read-only enforcement and SQL validation.
"""

import asyncio
import logging
import os
import re
import unicodedata
from typing import List, Dict, Any, Optional
import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

logger = logging.getLogger(__name__)

# Client-side TCP keepalives. The warehouse pool can leave a connection sitting
# idle between MCP requests; the network path to the Coolify-hosted Postgres
# silently severs idle sockets, and the next query then fails with
# "consuming input failed: SSL error: unexpected eof while reading". Keepalives
# keep idle connections warm and let the OS detect a dead peer quickly.
KEEPALIVE_KWARGS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}

# Queries handled by this server are strictly read-only (enforced by
# validate_sql + a read-only connection), so re-running one is always safe and
# idempotent. If a pooled connection turns out to be dead we transparently
# retry on a fresh connection instead of surfacing a confusing SSL/EOF error
# that an agent could misread as "no data".
MAX_QUERY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 0.25

# How long to wait for a free connection from the pool before giving up. Kept
# short so that if the warehouse is ever connection-saturated the MCP fails
# fast with a clear error instead of hanging an interactive request.
POOL_ACQUIRE_TIMEOUT_SECONDS = 10.0

# AdminShutdown, CrashShutdown and CannotConnectNow. A running query dies with these
# when its backend ends, whatever the login's limits are.
CONNECTION_LOST_SQLSTATES = {"57P01", "57P02", "57P03"}
# 08P01 is in class 08 but means the request was malformed, not that the
# connection dropped.
PROTOCOL_VIOLATION_SQLSTATE = "08P01"


# SQL statements that are never allowed, even with read-only connection
DANGEROUS_PATTERNS = [
    r'\bINSERT\b',
    r'\bUPDATE\b',
    r'\bDELETE\b',
    r'\bDROP\b',
    r'\bTRUNCATE\b',
    r'\bALTER\b',
    r'\bCREATE\b',
    r'\bGRANT\b',
    r'\bREVOKE\b',
    r'\bVACUUM\b',
    r'\bREINDEX\b',
    r'\bCLUSTER\b',
    r'\bCOMMENT\b',
    r'\bLOCK\b',
    r'\bUNLOCK\b',
    r'\bSET\b',
    r'\bRESET\b',
    r'\bDISCARD\b',
    r'\bLISTEN\b',
    r'\bNOTIFY\b',
    r'\bLOAD\b',
    r'\bCOPY\b',
    # DO blocks are refused where a statement starts; see validate_sql.
    # Additional dangerous patterns
    r'\bEXECUTE\b',
    r'\bPREPARE\b',
    r'\bCALL\b',
    r'\bIMPORT\b',
    r'\bMERGE\b',
    r'\bREFRESH\b',
    r'\bSECURITY\b',
    r'\bOWNER\b',
    # SELECT INTO creates tables
    r'\bSELECT\b[^;]*\bINTO\b\s+(?!STRICT\b|TEMP\b|TEMPORARY\b)',
    r'\bINTO\s+(?:TEMP|TEMPORARY\s+)?TABLE\b',
    # RAISE for DoS attacks
    r'\bRAISE\b',
    # Functions are checked by name against FORBIDDEN_FUNCTIONS in validate_sql,
    # which also catches quoted and schema-qualified spellings.
]


class SQLValidationError(Exception):
    """Raised when SQL contains forbidden statements."""
    pass


def _normalize_unicode(text: str) -> str:
    """
    Normalize Unicode text to ASCII to prevent homoglyph attacks.

    Converts full-width characters, look-alike Unicode chars, etc. to their
    ASCII equivalents.
    """
    # NFKC normalization converts full-width chars to ASCII equivalents
    # e.g., ＤＲＯＰ -> DROP
    normalized = unicodedata.normalize('NFKC', text)

    # Comprehensive homoglyph mappings for attack prevention
    # Covers Cyrillic, Greek, mathematical symbols, and other lookalikes
    homoglyphs = {
        # Cyrillic uppercase
        'А': 'A', 'В': 'B', 'С': 'C', 'Е': 'E', 'Н': 'H', 'І': 'I',
        'Ј': 'J', 'К': 'K', 'М': 'M', 'О': 'O', 'Р': 'P', 'Ѕ': 'S',
        'Т': 'T', 'Х': 'X', 'У': 'Y', 'Ғ': 'F',
        # Cyrillic lowercase
        'а': 'a', 'с': 'c', 'е': 'e', 'һ': 'h', 'і': 'i', 'ј': 'j',
        'о': 'o', 'р': 'p', 'ѕ': 's', 'х': 'x', 'у': 'y',
        # Greek uppercase
        'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'H', 'Ι': 'I',
        'Κ': 'K', 'Μ': 'M', 'Ν': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T',
        'Υ': 'Y', 'Χ': 'X',
        # Greek lowercase
        'α': 'a', 'β': 'B', 'ε': 'e', 'ι': 'i', 'κ': 'k', 'ν': 'v',
        'ο': 'o', 'ρ': 'p', 'τ': 't', 'υ': 'u', 'χ': 'x',
        # Various i/l/1 lookalikes
        'ı': 'i', 'ɩ': 'i', 'ǀ': 'l', 'ⅰ': 'i', 'ℓ': 'l', 'ⅼ': 'l',
        'Ɩ': 'I', 'Ⅰ': 'I', 'Ι': 'I', '١': '1', '۱': '1', 'ⅠⅠ': 'II',
        # Various o/0 lookalikes
        'ο': 'o', 'о': 'o', ' օ': 'o', '٥': '0', '۰': '0',
        # Mathematical bold/italic
        '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F',
        '𝐆': 'G', '𝐇': 'H', '𝐈': 'I', '𝐉': 'J', '𝐊': 'K', '𝐋': 'L',
        '𝐌': 'M', '𝐍': 'N', '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R',
        '𝐒': 'S', '𝐓': 'T', '𝐔': 'U', '𝐕': 'V', '𝐖': 'W', '𝐗': 'X',
        '𝐘': 'Y', '𝐙': 'Z',
        # Subscript/superscript
        'ᵃ': 'a', 'ᵇ': 'b', 'ᶜ': 'c', 'ᵈ': 'd', 'ᵉ': 'e', 'ᶠ': 'f',
        'ᵍ': 'g', 'ʰ': 'h', 'ⁱ': 'i', 'ʲ': 'j', 'ᵏ': 'k', 'ˡ': 'l',
        'ᵐ': 'm', 'ⁿ': 'n', 'ᵒ': 'o', 'ᵖ': 'p', 'ʳ': 'r', 'ˢ': 's',
        'ᵗ': 't', 'ᵘ': 'u', 'ᵛ': 'v', 'ʷ': 'w', 'ˣ': 'x', 'ʸ': 'y',
        'ᶻ': 'z',
        # Other common lookalikes
        'ƒ': 'f', 'ɡ': 'g', 'ɦ': 'h', 'ɱ': 'm', 'ɳ': 'n', 'ɾ': 'r',
        'ʋ': 'v', 'ʏ': 'Y', 'ʐ': 'z', 'ꜱ': 's', 'ꜰ': 'F',
        # Armenian
        'Տ': 'S', 'Ո': 'U', 'Ρ': 'P',
    }

    for homoglyph, ascii_char in homoglyphs.items():
        normalized = normalized.replace(homoglyph, ascii_char)

    # As a final safeguard, strip any remaining non-ASCII characters
    # that could be homoglyphs we missed, keeping only safe chars
    # But we keep common punctuation and operators needed for SQL
    safe_result = []
    for char in normalized:
        if ord(char) < 128 or char in '()[]{}.,;:!?@#$%^&*+-=<>/\\|`~"\'':
            safe_result.append(char)
        else:
            # Replace unknown non-ASCII with space to break up potential attacks
            safe_result.append(' ')

    return ''.join(safe_result)


def _is_ident_start(ch: str) -> bool:
    # PostgreSQL scan.l: ident_start [A-Za-z\200-\377_]. Any non-ASCII code
    # point is made of bytes >= 0x80 in UTF-8, so it counts as a letter.
    return ('A' <= ch <= 'Z') or ('a' <= ch <= 'z') or ch == '_' or ord(ch) >= 0x80


def _is_digit(ch: str) -> bool:
    return '0' <= ch <= '9'


def _is_ident_cont(ch: str) -> bool:
    # ident_cont [A-Za-z\200-\377_0-9\$]: "$" continues an identifier.
    return _is_ident_start(ch) or _is_digit(ch) or ch == '$'


def _is_dolq_cont(ch: str) -> bool:
    # dolq_cont [A-Za-z\200-\377_0-9]: no "$" inside a dollar-quote tag.
    return _is_ident_start(ch) or _is_digit(ch)


def _skip_quoted(sql: str, i: int, quote: str, backslash_escapes: bool) -> int:
    """Return the index just past the quoted token that starts at sql[i] == quote."""
    n = len(sql)
    j = i + 1
    while j < n:
        c = sql[j]
        if backslash_escapes and c == '\\':
            j += 2
            continue
        if c == quote:
            if j + 1 < n and sql[j + 1] == quote:
                j += 2
                continue
            return j + 1
        j += 1
    return n  # unterminated: PostgreSQL rejects the whole query


class _LexResult:
    """What PostgreSQL's lexer would see in a query."""

    def __init__(self):
        self.code = []          # SQL with comments, strings and quoted identifiers blanked
        self.names = []         # every identifier, resolved the way PostgreSQL resolves it
        self.unicode_escape = False  # U&'...' or U&"..." (escapes can spell any name)
        self.parameter = False  # $1, $2, ...


def _downcase_identifier(word: str) -> str:
    # PostgreSQL folds unquoted identifiers with downcase_identifier(): ASCII
    # A-Z only in a UTF-8 database.
    return ''.join(chr(ord(c) + 32) if 'A' <= c <= 'Z' else c for c in word)


def _lex(sql: str) -> _LexResult:
    """
    Tokenize SQL the way PostgreSQL's lexer (src/backend/parser/scan.l) does.

    The checks in validate_sql must see exactly the code PostgreSQL will run,
    so every rule below follows scan.l:

    - Identifiers ({ident_start}{ident_cont}*) and numbers are consumed whole.
      "$" continues an identifier, so in ``x$a$`` there is no dollar quote.
    - A dollar quote ``$tag$`` starts only where a new token starts. The tag is
      ``{dolq_start}{dolq_cont}*`` and never contains "$". ``$1`` is a parameter.
    - ``--`` comments end at "\\n" or "\\r"; ``/* */`` comments nest.
    - ``'...'`` uses '' only (standard_conforming_strings is on); ``E'...'``
      also honours backslash escapes, but only when E starts a token.
    - ``"..."`` is a quoted identifier with "" escapes. Its exact content is
      the name (case-sensitive), so ``"pg_sleep"`` names pg_sleep.
    - ``U&"..."`` and ``U&'...'`` can spell any name or text with escapes
      (``\\0070``, or another escape character via UESCAPE); they are flagged.

    This runs on the original text. Unicode normalization happens afterwards,
    on the output, because normalizing first changes where PostgreSQL's tokens
    begin and end (for example a non-ASCII letter before "$a$").
    """
    lex = _LexResult()
    result = lex.code
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ''

        # Line comment: -- up to the end of the line (\n or \r).
        if ch == '-' and nxt == '-':
            j = i + 2
            while j < n and sql[j] not in '\n\r':
                j += 1
            result.append(' ')
            i = j
            continue

        # Block comment: /* ... */, nested.
        if ch == '/' and nxt == '*':
            depth = 1
            j = i + 2
            while j < n and depth > 0:
                if sql[j] == '/' and j + 1 < n and sql[j + 1] == '*':
                    depth += 1
                    j += 2
                elif sql[j] == '*' and j + 1 < n and sql[j + 1] == '/':
                    depth -= 1
                    j += 2
                else:
                    j += 1
            result.append(' ')
            i = j
            continue

        # Standard string literal: '...'
        if ch == "'":
            i = _skip_quoted(sql, i, "'", backslash_escapes=False)
            result.append(" '' ")
            continue

        # Quoted identifier: "..." (the name is the exact content, "" -> ")
        if ch == '"':
            j = _skip_quoted(sql, i, '"', backslash_escapes=False)
            lex.names.append(sql[i + 1:j - 1].replace('""', '"'))
            result.append(' _ident_ ')
            i = j
            continue

        # A "$" here starts a new token: a dollar quote, a parameter, or junk.
        if ch == '$':
            j = i + 1
            if j < n and _is_ident_start(sql[j]):
                j += 1
                while j < n and _is_dolq_cont(sql[j]):
                    j += 1
            if j < n and sql[j] == '$':
                tag = sql[i:j + 1]
                end_pos = sql.find(tag, j + 1)
                result.append(" '' ")
                # Unterminated: PostgreSQL rejects the whole query.
                i = n if end_pos == -1 else end_pos + len(tag)
                continue
            if j == i + 1 and j < n and _is_digit(sql[j]):
                # Positional parameter $1, $2, ...
                while j < n and _is_digit(sql[j]):
                    j += 1
                lex.parameter = True
                result.append(sql[i:j])
                i = j
                continue
            result.append(ch)
            i += 1
            continue

        if _is_ident_start(ch):
            # Escape string E'...': a prefix only at the start of a token.
            if ch in 'Ee' and nxt == "'":
                i = _skip_quoted(sql, i + 1, "'", backslash_escapes=True)
                result.append(" '' ")
                continue
            # Unicode escapes U&'...' / U&"...": a prefix only at token start.
            if ch in 'Uu' and nxt == '&' and i + 2 < n and sql[i + 2] in '\'"':
                lex.unicode_escape = True
            # Identifier or keyword, consumed whole (B'..', X'..', N'..' and
            # U&'..' then lex as a standard string, as in PostgreSQL).
            j = i + 1
            while j < n and _is_ident_cont(sql[j]):
                j += 1
            word = sql[i:j]
            lex.names.append(_downcase_identifier(word))
            result.append(word)
            i = j
            continue

        if _is_digit(ch):
            # Numeric literal. Valid numbers use only ASCII letters, digits,
            # "_" and "."; anything that runs into an identifier is "trailing
            # junk", which PostgreSQL rejects for the whole query.
            j = i + 1
            while j < n and (
                _is_digit(sql[j]) or sql[j] in '_.'
                or ('A' <= sql[j] <= 'Z') or ('a' <= sql[j] <= 'z')
            ):
                j += 1
            result.append(sql[i:j])
            i = j
            continue

        result.append(ch)
        i += 1

    return lex


# Transaction control would end the read-only, always-rolled-back transaction
# each query runs in. These words are blocked where a statement starts ("END"
# is fine inside CASE ... END).
TRANSACTION_CONTROL_STATEMENTS = {
    'ABORT', 'BEGIN', 'COMMIT', 'END', 'PREPARE', 'RELEASE', 'ROLLBACK',
    'SAVEPOINT', 'START',
}

# DO runs a procedural block. Its body is a string, which the lexer blanks, so
# no check here can see what it calls (pg_sleep, pg_terminate_backend, EXECUTE
# of a built-up string, ...). Refused where a statement starts.
PROCEDURAL_BLOCK_STATEMENTS = {'DO'}

# Functions no query may name, however the name is written: unquoted in any
# case, quoted ("pg_sleep"), or schema-qualified (pg_catalog."pg_sleep").
# Matched against each identifier as PostgreSQL resolves it, so this holds for
# calls in SELECT lists, FROM (SELECT * FROM pg_sleep(1)) and anywhere else.
# PostgreSQL itself still enforces privileges; this list stops the calls the
# query login is allowed to make but the tool should not.
FORBIDDEN_FUNCTIONS = frozenset({
    # Sleeping holds a pooled connection for up to statement_timeout.
    'pg_sleep', 'pg_sleep_for', 'pg_sleep_until',
    # Signalling: every MCP user shares one login, and a login may signal its
    # own backends, so these would cancel or end other users' queries.
    'pg_cancel_backend', 'pg_terminate_backend', 'pg_log_backend_memory_contexts',
    # Functions that run SQL given as text, which none of these checks can see.
    # Found by scanning PostgreSQL 18's source for every SPI query call
    # (SPI_execute*, SPI_prepare*, SPI_cursor_open_with_args/parse_open) and
    # confirmed by calling every pg_catalog function that takes text with a
    # marker query: ts_rewrite(tsquery, text), ts_stat and query_to_xml* run
    # the text; cursor_to_xml* run a named cursor's query.
    'query_to_xml', 'query_to_xmlschema', 'query_to_xml_and_xmlschema',
    'cursor_to_xml', 'cursor_to_xmlschema', 'ts_stat', 'ts_rewrite',
    # These run a fixed SELECT * over whole tables, schemas or the database.
    'table_to_xml', 'table_to_xmlschema', 'table_to_xml_and_xmlschema',
    'schema_to_xml', 'schema_to_xmlschema', 'schema_to_xml_and_xmlschema',
    'database_to_xml', 'database_to_xmlschema', 'database_to_xml_and_xmlschema',
    # Session state and locks that outlive the query.
    'set_config', 'pg_advisory_lock', 'pg_advisory_lock_shared',
    'pg_advisory_xact_lock', 'pg_advisory_xact_lock_shared',
    'pg_try_advisory_lock', 'pg_try_advisory_lock_shared',
    'pg_try_advisory_xact_lock', 'pg_try_advisory_xact_lock_shared',
    'pg_notify',
    # Large objects and files.
    'lo_import', 'lo_export', 'lo_from_bytea', 'lo_put', 'lo_create', 'lo_creat',
    'lo_unlink', 'lo_truncate', 'lo_truncate64', 'lowrite', 'lo_open',
    'pg_read_file', 'pg_read_binary_file', 'pg_write_file', 'pg_ls_dir',
    'pg_stat_file', 'pg_ls_logdir', 'pg_ls_waldir', 'pg_ls_tmpdir',
    'pg_ls_archive_statusdir', 'pg_ls_logicalsnapdir', 'pg_ls_logicalmapdir',
    'pg_ls_replslotdir',
    # Server administration.
    'pg_reload_conf', 'pg_rotate_logfile', 'pg_switch_wal',
    'pg_create_restore_point', 'pg_promote',
    # Remote databases.
    'dblink', 'dblink_exec', 'dblink_connect', 'dblink_connect_u',
    'dblink_send_query',
})


_RUNS_A_STRING = (
    " (it runs a query given as text, which this tool cannot check). For row counts"
    " use pg_stat_user_tables.n_live_tup or pg_class.reltuples, or count(*) per table"
)
FORBIDDEN_FUNCTION_HINTS = {
    name: _RUNS_A_STRING
    for name in ('query_to_xml', 'query_to_xmlschema', 'query_to_xml_and_xmlschema',
                 'cursor_to_xml', 'cursor_to_xmlschema', 'ts_stat', 'ts_rewrite')
}
FORBIDDEN_FUNCTION_HINTS.update({
    name: " (it reads whole tables at once); query the tables directly with a LIMIT instead"
    for name in ('table_to_xml', 'table_to_xmlschema', 'table_to_xml_and_xmlschema',
                 'schema_to_xml', 'schema_to_xmlschema', 'schema_to_xml_and_xmlschema',
                 'database_to_xml', 'database_to_xmlschema', 'database_to_xml_and_xmlschema')
})


def validate_sql(sql: str) -> None:
    """
    Validate that SQL doesn't contain dangerous statements.
    Raises SQLValidationError if forbidden patterns are found.

    This is one of three layers. The others are server side and do not depend on
    this lexer: Database._run sends each query with the extended query protocol
    (PostgreSQL itself refuses more than one statement) inside a READ ONLY
    transaction that is always rolled back, and the pool resets each connection
    before reuse. Function calls are NOT stopped server side. The function list
    here is a defence-in-depth deny list matched against every identifier,
    quoted or not; it cannot see SQL hidden inside a string, which is why the
    functions that execute text are on it. The real control for signalling other
    sessions is revoking EXECUTE on those functions in the warehouse database.
    """
    # Tokenize first, on the text PostgreSQL will see.
    lex = _lex(sql)

    if lex.unicode_escape:
        raise SQLValidationError(
            "Unicode escape strings and identifiers (U&'...', U&\"...\") are not allowed"
        )
    if lex.parameter:
        raise SQLValidationError(
            "Positional parameters ($1) are not supported; put the values in the query"
        )
    for name in lex.names:
        if name in FORBIDDEN_FUNCTIONS:
            raise SQLValidationError(
                f"SQL uses a forbidden function: {name}{FORBIDDEN_FUNCTION_HINTS.get(name, '')}"
            )

    # Normalize Unicode to prevent homoglyph attacks (e.g., ＤＲＯＰ -> DROP)
    cleaned = _normalize_unicode(''.join(lex.code))

    # Convert to uppercase for case-insensitive matching
    cleaned = cleaned.upper()

    statements = [part for part in cleaned.split(';') if part.strip()]
    if len(statements) > 1:
        raise SQLValidationError("Only one SQL statement is allowed per query")
    for statement in statements:
        first = re.match(r'\s*\(*\s*([A-Z_]+)', statement)
        if first and first.group(1) in PROCEDURAL_BLOCK_STATEMENTS:
            raise SQLValidationError(
                "DO blocks are not allowed (they run procedural code this tool cannot"
                " check); write the query as a single SELECT"
            )
        if first and first.group(1) in TRANSACTION_CONTROL_STATEMENTS:
            raise SQLValidationError(
                f"SQL contains forbidden statement pattern: {first.group(1)}"
            )

    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            raise SQLValidationError(
                f"SQL contains forbidden statement pattern: {pattern.replace(chr(92), '')}"
            )


def is_connection_lost(error: psycopg.OperationalError) -> bool:
    """
    True if the connection is gone, so the same query can succeed on a new one.

    Other OperationalErrors are the server refusing this query: a statement or lock
    timeout, temp_file_limit, a full disk. Running it again repeats the same damage,
    such as queuing behind a TRUNCATE or spilling to disk again.
    """
    # A socket cut without a word from the server has no SQLSTATE.
    state = error.sqlstate
    if state == PROTOCOL_VIOLATION_SQLSTATE:
        # The server answered: this query broke the protocol (for example a $1
        # with no value). The connection is fine and a retry fails the same way.
        return False
    return state is None or state.startswith("08") or state in CONNECTION_LOST_SQLSTATES


def get_connection_url() -> str:
    """Get the warehouse connection URL from environment."""
    url = os.environ.get('WAREHOUSE_COOLIFY_URL')
    if not url:
        raise RuntimeError("WAREHOUSE_COOLIFY_URL environment variable is not set")
    return url


def make_readonly_url(url: str) -> str:
    """Enable read-only by default while preserving existing connection settings."""
    settings = conninfo_to_dict(url)
    options = settings.get("options", "")
    settings["options"] = f"{options} -c default_transaction_read_only=on".lstrip()
    return make_conninfo(**settings)


MAX_ATTRIBUTION_NAME_LENGTH = 100
_SAFE_ATTRIBUTION_EXTRA = frozenset(" .'-")


def _format_attribution_comment(firstname: str, lastname: str) -> str:
    """Build a single-line SQL block comment for caller attribution.

    Uses ``/* ... */`` without a newline so PostgreSQL logs the caller comment
    and the SQL statement on the same prefixed log line instead of splitting
    the statement onto an unprefixed ``\\t`` continuation line.

    Applies a strict character allow-list (Unicode letters, ASCII digits,
    space, ``.``, ``'``, ``-``) and caps length so no caller name can contain
    ``/*``, ``*/``, newlines, NUL bytes, or break out of the block comment.
    """
    raw = f"{firstname} {lastname}"
    allowed = "".join(
        ch
        for ch in raw
        if (
            unicodedata.category(ch).startswith("L")
            or ("0" <= ch <= "9")
            or ch in _SAFE_ATTRIBUTION_EXTRA
        )
    )
    cleaned = " ".join(allowed.split())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    cleaned = cleaned[:MAX_ATTRIBUTION_NAME_LENGTH].strip()
    # Defense-in-depth: guarantee the comment body can never open/close a block comment
    # or start a SQL line comment.
    assert "/*" not in cleaned and "*/" not in cleaned and "--" not in cleaned
    assert "\n" not in cleaned and "\r" not in cleaned and "\x00" not in cleaned
    return f"/* warehouse-mcp {cleaned} */" if cleaned else "/* warehouse-mcp */"


async def reset_session(conn: psycopg.AsyncConnection) -> None:
    """Pool reset: drop session settings and session advisory locks.

    RESET ALL returns every setting to its connection-start value, which keeps
    default_transaction_read_only=on from the connection options and role.
    """
    await conn.execute("RESET ALL")
    await conn.execute("SELECT pg_catalog.pg_advisory_unlock_all()")


class Database:
    """PostgreSQL database connection pool with read-only enforcement."""

    def __init__(self):
        self._pool: Optional[AsyncConnectionPool] = None

    async def open(self) -> None:
        """Open the connection pool."""
        url = get_connection_url()
        readonly_url = make_readonly_url(url)
        self._pool = AsyncConnectionPool(
            conninfo=readonly_url,
            min_size=1,
            max_size=5,
            open=False,
            # Probe every connection before handing it to a caller. This
            # transparently discards and replaces connections that were
            # silently dropped while idle in the pool -- the root cause of the
            # intermittent "SSL error: unexpected eof while reading" failures.
            check=AsyncConnectionPool.check_connection,
            # Proactively recycle connections so we never lean on a long-lived
            # socket that the network path may sever out from under us.
            max_idle=300.0,
            max_lifetime=1800.0,
            # Undo any session state before the connection serves another
            # caller, whatever the query did.
            reset=reset_session,
            kwargs={
                "autocommit": True,
                "row_factory": dict_row,
                "connect_timeout": 10,
                **KEEPALIVE_KWARGS,
            },
        )
        await self._pool.open()

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def check_auth_database_isolation(self) -> None:
        """Require the warehouse connection to use a different database."""
        if self._pool is None:
            raise RuntimeError("Warehouse connection pool is unavailable")
        async with self._pool.connection(timeout=10) as conn:
            row = await (await conn.execute("""
                SELECT pg_catalog.to_regnamespace('mcp_auth') IS NOT NULL AS has_auth_schema
            """)).fetchone()
        if row["has_auth_schema"]:
            raise RuntimeError("Warehouse queries and MCP auth require separate Postgres databases")

    async def execute_query(
        self,
        sql: str,
        max_rows: int = 10000,
        user_info: tuple[str, str] | None = None
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a read-only SQL query and return results.

        Args:
            sql: The SQL query to execute
            max_rows: Maximum number of rows to return (default 10000)
            user_info: Optional (firstname, lastname) tuple for SQL attribution

        Returns:
            Tuple of (rows as list of dicts, column names)

        Raises:
            SQLValidationError: If SQL contains forbidden statements
        """
        # Validate user SQL before execution
        validate_sql(sql)

        # Prefix SQL with single-line user attribution comment so Postgres
        # logs user=, client=, the caller comment, and the SQL on one line.
        if user_info:
            firstname, lastname = user_info
            sql = f"{_format_attribution_comment(firstname, lastname)} {sql}"
            # Re-validate the final SQL text sent to PostgreSQL (defense in depth).
            validate_sql(sql)

        return await self._run(sql, None, max_rows)

    async def execute_query_with_params(
        self,
        sql: str,
        params: tuple,
        max_rows: int = 10000
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a parameterized read-only SQL query and return results.

        Args:
            sql: The SQL query with %s placeholders
            params: Tuple of parameters to substitute
            max_rows: Maximum number of rows to return (default 10000)

        Returns:
            Tuple of (rows as list of dicts, column names)
        """
        return await self._run(sql, params, max_rows)

    async def _run(
        self,
        sql: str,
        params: Optional[tuple],
        max_rows: int,
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Acquire a pooled connection and run a read-only query, retrying on a
        fresh connection if the one we got turns out to be dead.

        The pool's ``check`` callback already replaces connections that were
        dropped while idle, but a connection can also be severed *mid-query*
        (typically on heavier queries). Since every query here is read-only and
        idempotent, retrying on a new connection is safe and keeps a dropped
        connection from surfacing as a confusing error -- or, worse, being
        misread by an agent as an empty result.
        """
        for attempt in range(1, MAX_QUERY_ATTEMPTS + 1):
            try:
                async with self._pool.connection(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as conn:
                    # READ ONLY, and always rolled back: anything the query
                    # changes in the session (SET, set_config) is undone.
                    async with conn.transaction(force_rollback=True):
                        await conn.execute("SET TRANSACTION READ ONLY")
                        async with conn.cursor() as cur:
                            # Pipeline mode sends the query with the extended
                            # protocol, where PostgreSQL itself refuses more
                            # than one statement, so nothing can follow a
                            # COMMIT that slips past validate_sql.
                            async with conn.pipeline() as pipeline:
                                await cur.execute(sql, params)
                                await pipeline.sync()

                            if cur.description is None:
                                return [], []

                            columns = [desc[0] for desc in cur.description]
                            rows = await cur.fetchmany(max_rows)
                            return [dict(row) for row in rows], columns
            except PoolTimeout:
                # The pool itself couldn't hand us a connection (warehouse is
                # connection-saturated). Retrying just stacks more timeouts, so
                # surface it immediately rather than hanging the request.
                raise
            except psycopg.OperationalError as e:
                if not is_connection_lost(e) or attempt >= MAX_QUERY_ATTEMPTS:
                    raise
                logger.warning(
                    "Connection error on query attempt %d/%d, retrying: %s",
                    attempt, MAX_QUERY_ATTEMPTS, e,
                )
                await asyncio.sleep(RETRY_BACKOFF_SECONDS)

        # Unreachable: the loop either returns or raises.
        raise RuntimeError("query retry loop exited without a result")

    async def list_schemas(self) -> List[str]:
        """List all non-system schemas in the database."""
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name;
        """
        rows, _ = await self.execute_query(sql)
        return [row['schema_name'] for row in rows]

    async def describe_schema(
        self,
        schema_name: str,
        max_columns: int = 1000,
        max_value_length: int = 80,
        max_output_bytes: int = 50000,
        sample_rows: int = 3
    ) -> str:
        """
        Get schema description with tables, columns, and sample data.

        Generates output directly in Python with truncation at SQL level
        to avoid transferring large amounts of data from the database.

        Args:
            schema_name: Name of the schema to describe
            max_columns: Maximum columns to show per table (default 30)
            max_value_length: Maximum length for sample values (default 80)
            max_output_bytes: Stop adding tables when output exceeds this (default 50KB)
            sample_rows: Number of sample rows per table (default 3)

        Returns:
            Markdown description of the schema with tables, columns, and sample data
        """
        # Validate schema name format
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema_name):
            raise ValueError(f"Invalid schema name: {schema_name}")

        # Get all tables and views in schema, with their comments. Views are
        # listed too: some (warehouse_metadata.mirror_freshness) are the main
        # way to read a schema, and their comments say how to read them.
        tables_sql = """
            SELECT t.table_name,
                   t.table_type,
                   c.oid AS table_oid,
                   pg_catalog.obj_description(c.oid, 'pg_class') AS table_comment
            FROM information_schema.tables t
            JOIN pg_catalog.pg_namespace n ON n.nspname = t.table_schema
            JOIN pg_catalog.pg_class c
              ON c.relnamespace = n.oid AND c.relname = t.table_name
            WHERE t.table_schema = %s AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name
        """
        tables, _ = await self.execute_query_with_params(tables_sql, (schema_name,))

        if not tables:
            return f"Schema '{schema_name}' not found or has no tables."

        output_parts = [f"# Schema: {schema_name}\n"]
        current_size = len(output_parts[0])
        budget_exceeded = False

        for table_row in tables:
            table_name = table_row['table_name']
            is_internal = table_name.startswith('_dlt_')

            # Get columns for this table
            # ordinal_position is the column's attnum, so it finds its comment.
            columns_sql = """
                SELECT column_name,
                       data_type,
                       pg_catalog.col_description(%s::oid, ordinal_position::int) AS column_comment
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            all_columns, _ = await self.execute_query_with_params(
                columns_sql, (table_row['table_oid'], schema_name, table_name)
            )

            total_columns = len(all_columns)
            columns = all_columns[:max_columns]

            # Build table header
            table_block = f"\n## {schema_name}.{table_name}"
            if table_row['table_type'] == 'VIEW':
                table_block += " (view)"
            table_block += "\n"
            if table_row['table_comment']:
                table_block += f"{table_row['table_comment']}\n"
            table_block += f"Columns ({total_columns} total)"
            if total_columns > max_columns:
                table_block += f" - showing first {max_columns}"
            table_block += ":\n"

            # Column list: comma-separated, or one per line with its comment
            # when any column has one.
            if any(c['column_comment'] for c in columns):
                table_block += "".join(
                    f"- {c['column_name']}: {c['column_comment']}\n"
                    if c['column_comment'] else f"- {c['column_name']}\n"
                    for c in columns
                )
                if total_columns > max_columns:
                    table_block += f"... (+{total_columns - max_columns} more)\n"
            else:
                col_names = [c['column_name'] for c in columns]
                table_block += ", ".join(col_names)
                if total_columns > max_columns:
                    table_block += f", ... (+{total_columns - max_columns} more)"
                table_block += "\n"

            # Sample data (skip for internal _dlt_* tables or if budget exceeded)
            if budget_exceeded:
                table_block += "(output budget exceeded, samples omitted)\n"
            elif is_internal:
                table_block += "(internal table, samples omitted)\n"
            elif columns:
                # Build SELECT with LEFT() truncation for each column
                select_parts = []
                for col in columns:
                    col_name = col['column_name']
                    # Use format() for the identifier, parameterized queries don't work for identifiers
                    select_parts.append(
                        f"LEFT({self._quote_ident(col_name)}::text, {max_value_length}) AS {self._quote_ident(col_name)}"
                    )

                select_clause = ", ".join(select_parts)
                # Table name needs quoting too
                sample_sql = f"SELECT {select_clause} FROM {self._quote_ident(schema_name)}.{self._quote_ident(table_name)} LIMIT {sample_rows}"

                try:
                    sample_rows_data, _ = await self.execute_query(sample_sql)
                    if sample_rows_data:
                        table_block += f"\nSample data ({len(sample_rows_data)} rows):\n"
                        for row in sample_rows_data:
                            # Format as comma-separated values (empty string for NULL)
                            values = []
                            for c in columns:
                                val = row.get(c['column_name'])
                                values.append('' if val is None else str(val))
                            table_block += ", ".join(values) + "\n"
                    else:
                        table_block += "(no rows)\n"
                except Exception as e:
                    table_block += f"(error reading samples: {str(e)[:50]})\n"

            # Check if adding this table would exceed budget
            block_size = len(table_block.encode('utf-8'))
            if current_size + block_size > max_output_bytes:
                remaining_tables = len(tables) - len(output_parts) + 1
                output_parts.append(f"\n... and {remaining_tables} more tables/views (output limit reached)\n")
                budget_exceeded = True
                break

            output_parts.append(table_block)
            current_size += block_size

        return "".join(output_parts)

    def _quote_ident(self, identifier: str) -> str:
        """Quote a PostgreSQL identifier (table/column name)."""
        # Double any existing double quotes and wrap in double quotes
        return '"' + identifier.replace('"', '""') + '"'

    async def list_columns(
        self,
        schema_name: str,
        table_name: str,
        offset: int = 0,
        limit: int = 100
    ) -> tuple[list[dict], int]:
        """
        List all columns for a specific table with pagination.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            offset: Starting column index
            limit: Maximum columns to return

        Returns:
            Tuple of (columns list, total column count)
        """
        # Validate names to prevent SQL injection
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema_name):
            raise ValueError(f"Invalid schema name: {schema_name}")
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        # Get total count
        count_sql = """
            SELECT COUNT(*) as total
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        count_rows, _ = await self.execute_query_with_params(count_sql, (schema_name, table_name))
        total = count_rows[0]['total'] if count_rows else 0

        # Get columns with pagination
        columns_sql = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            LIMIT %s OFFSET %s
        """
        columns, _ = await self.execute_query_with_params(
            columns_sql,
            (schema_name, table_name, limit, offset)
        )

        return columns, total


# Global database instance
_db: Optional[Database] = None


async def init_database() -> Database:
    """Initialize the global database instance and open the pool."""
    global _db
    _db = Database()
    await _db.open()
    return _db


async def close_database() -> None:
    """Close the global database instance."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None


def get_database() -> Database:
    """Get the global database instance. Must call init_database() first."""
    if _db is None:
        raise RuntimeError("Database not initialized - call init_database() first")
    return _db
