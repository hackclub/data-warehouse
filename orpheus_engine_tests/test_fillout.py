"""Unit tests for the Fillout sync pure helpers (no network / no DB)."""

import json
import time

from orpheus_engine.defs.fillout.definitions import (
    RateLimiter,
    _can_delete_stale_rows,
    _dedupe_by_pk,
    _form_row,
    _j,
    _submission_row,
    get_base_url,
)


def test_j_strips_nul_bytes():
    # Postgres rejects NUL bytes in JSONB; _j must drop the escape sequence.
    out = _j({"answer": "bad" + chr(0) + "value"})
    assert "\\u0000" not in out
    assert json.loads(out) == {"answer": "badvalue"}


def test_j_roundtrips_nested_structures():
    value = {"questions": [{"id": "q1", "value": ["a", "b"]}], "n": 3}
    assert json.loads(_j(value)) == value


def test_submission_row_shape_and_promoted_columns():
    sub = {
        "submissionId": "abc-123",
        "submissionTime": "2026-06-22T21:43:13.188Z",
        "lastUpdatedAt": "2026-06-22T22:00:00.000Z",
        "startedAt": "2026-06-22T21:40:00.000Z",
        "questions": [{"id": "h2ws", "name": "User", "type": "ShortAnswer", "value": "example-user"}],
    }
    row = _submission_row("formX", sub)

    # (form_id, submission_id, submission_time, last_updated_at, started_at, response_json)
    assert len(row) == 6
    assert row[0] == "formX"
    assert row[1] == "abc-123"
    assert row[2] == "2026-06-22T21:43:13.188Z"
    assert row[3] == "2026-06-22T22:00:00.000Z"
    assert row[4] == "2026-06-22T21:40:00.000Z"
    # Full object preserved as JSON in the response column.
    assert json.loads(row[5])["questions"][0]["value"] == "example-user"


def test_submission_row_tolerates_missing_optional_fields():
    row = _submission_row("formY", {"submissionId": "only-id"})
    assert row[1] == "only-id"
    assert row[2] is None and row[3] is None and row[4] is None
    assert json.loads(row[5]) == {"submissionId": "only-id"}


def test_form_row_uses_null_metadata_when_metadata_fetch_failed():
    row = _form_row({"formId": "formZ", "name": "Example form"}, meta=None)
    assert row[:5] == ("formZ", None, "Example form", None, "[]")
    assert row[5:] == (None, None, None, None, None, None)


def test_form_row_json_encodes_metadata_fields():
    row = _form_row(
        {"formId": "formZ", "name": "Example form", "tags": ["event"]},
        meta={"questions": [{"id": "q1"}], "payments": [{"id": "pay1"}]},
    )
    assert json.loads(row[4]) == ["event"]
    assert json.loads(row[5]) == [{"id": "q1"}]
    assert json.loads(row[9]) == [{"id": "pay1"}]


def test_dedupe_by_pk_submissions_keeps_last():
    # Fillout can return the same submission id twice; a batch upsert must not
    # touch the same (form_id, submission_id) twice.
    rows = [
        ("f1", "s1", "t1", None, None, "{}"),
        ("f1", "s2", "t2", None, None, "{}"),
        ("f1", "s1", "t1-newer", None, None, "{}"),  # dup PK, newer
    ]
    out = _dedupe_by_pk(rows, key_len=2)
    assert len(out) == 2
    by_id = {r[1]: r for r in out}
    assert by_id["s1"][2] == "t1-newer"  # last occurrence wins


def test_dedupe_by_pk_forms_uses_single_key():
    rows = [("formA", 1), ("formB", 2), ("formA", 3)]
    out = _dedupe_by_pk(rows, key_len=1)
    assert len(out) == 2
    assert dict(out)["formA"] == 3


def test_rate_limiter_enforces_minimum_spacing():
    # Four acquires at 50ms spacing should take at least 3 intervals (~150ms).
    limiter = RateLimiter(0.05)
    start = time.monotonic()
    for _ in range(4):
        limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.15 - 0.02  # small tolerance for timer granularity


def test_can_delete_stale_rows_only_after_complete_form_pass():
    assert _can_delete_stale_rows(form_errors=0, skipped_forms=0)
    assert not _can_delete_stale_rows(form_errors=1, skipped_forms=0)
    assert not _can_delete_stale_rows(form_errors=0, skipped_forms=1)


def test_get_base_url_supports_host_or_full_api_path(monkeypatch):
    monkeypatch.delenv("FILLOUT_API_BASE_URL", raising=False)
    assert get_base_url() == "https://api.fillout.com/v1/api"

    monkeypatch.setenv("FILLOUT_API_BASE_URL", "https://eu-api.fillout.com")
    assert get_base_url() == "https://eu-api.fillout.com/v1/api"

    monkeypatch.setenv("FILLOUT_API_BASE_URL", "https://api.fillout.com/v1/api")
    assert get_base_url() == "https://api.fillout.com/v1/api"
