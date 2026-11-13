from urllib.parse import unquote, urlsplit

import pytest

from orpheus_engine.defs.sling.reader_grant_drift import _discover_reader_programs
from orpheus_engine.defs.sling.reader_grants import READER_ROLE
from scripts import provision_orchard_reader as por

OWNER_URL = (
    "postgresql://u_owner:ownerpw@ysws-demo-pg-demo.a.db.example.com:31000/ysws_demo"
    "?sslmode=verify-full&sslrootcert=system"
)


def test_reader_url_keeps_public_endpoint_and_quotes_password():
    url = por.reader_url(OWNER_URL, "p@ss/w:rd?#")
    parts = urlsplit(url)
    assert parts.username == por.READER
    assert unquote(parts.password) == "p@ss/w:rd?#"
    assert (parts.hostname, parts.port, parts.path) == ("ysws-demo-pg-demo.a.db.example.com", 31000, "/ysws_demo")
    assert parts.query == "sslmode=verify-full&sslrootcert=system"
    assert "ownerpw" not in url and "u_owner" not in url


def test_reader_is_not_the_column_grant_reader():
    # The §9 drift check expects column grants, which this login does not use.
    assert por.READER != READER_ROLE


def test_drift_check_skips_orchard_reader(monkeypatch):
    monkeypatch.setenv("SAVE_POINT_K8S_URL", por.reader_url(OWNER_URL, "x"))
    import os

    _, skipped = _discover_reader_programs(os.environ)
    assert skipped["save_point"] == f"does not log in as {READER_ROLE}"


def test_set_env_line_replaces_existing_value():
    lines = ["A=1", "B=2", "C=3"]
    assert por.set_env_line(lines, "B", "9") == ["A=1", "C=3", "B=9"]
    assert por.set_env_line(lines, "D", "4") == ["A=1", "B=2", "C=3", "D=4"]


def test_main_rejects_malformed_target(tmp_path):
    with pytest.raises(SystemExit):
        por.main(["--env-file", str(tmp_path / "staged.env"), "NO_DATABASE_ID"])
