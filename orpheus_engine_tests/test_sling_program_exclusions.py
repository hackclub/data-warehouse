import importlib
import re

import pytest

PROGRAMS = ["haven", "haven_booking", "atlantis", "pixl", "cubed", "mipmap"]

# Tables that hold login state, bearer secrets, caches or one-off backups.
UNMIRRORED_TABLE = re.compile(
    r"(^|_)(sessions?|auth_tokens?|rate_limits?|cache|migrations)$"
    r"|_bak_|backup|verifications?$|snapshot_state$"
)

# Secret or contact columns that must stay excluded wherever their table is
# mirrored, checked against each program's live schema when it was added.
EXCLUDED_COLUMNS = {
    "haven": {
        "public.users": ["token", "hcaToken", "phoneNumber", "address"],
        "public.events": ["venueAddress", "latitude", "longitude"],
    },
    "haven_booking": {
        "public.hosts": ["refresh_token_enc"],
        "public.bookings": ["meet_url"],
    },
    "atlantis": {
        "public.auth_user": ["password"],
        "public.atlantis_site_lapseaccount": ["encrypted_token"],
        "public.atlantis_site_profile": ["encrypted_hca_token"],
        "public.atlantis_site_timelapse": ["token", "playback_url"],
        "public.atlantis_site_auditlog": ["ip_address"],
    },
    "pixl": {
        "public.users": [
            "hackatime_token", "address_line1", "address_line2",
            "address_postal", "phone",
        ],
        "public.lobbies": ["password"],
        "public.projects": ["join_code", "journal_share_token"],
        "public.audit_log": ["ip"],
        "public.form_submissions": ["ip_hash"],
        "public.shop_orders": ["tracking"],
    },
    "cubed": {},
    "mipmap": {
        "public.user": [
            "phone_number", "address_line_1", "address_line_2",
            "address_postal_code", "address_phone_number",
        ],
        "public.order": [
            "tracking_number", "address_line_1", "address_line_2",
            "address_postal_code", "address_phone_number",
        ],
        "public.snapshot": ["held_image"],
    },
}


def _config(program):
    module = importlib.import_module(f"orpheus_engine.defs.sling.programs.{program}")
    return getattr(module, f"{program}_replication_config")


@pytest.mark.parametrize("program", PROGRAMS)
def test_streams_are_listed_explicitly(program):
    config = _config(program)
    assert config["defaults"]["object"] == f"{program}.{{stream_table}}"
    for stream, options in config["streams"].items():
        assert "*" not in stream
        table = stream.split(".", 1)[1]
        assert not UNMIRRORED_TABLE.search(table), stream
        # Exclusion lists only: a new column is reviewed by adding it here.
        for column in (options or {}).get("select", []):
            assert column.startswith("-"), (stream, column)


@pytest.mark.parametrize("program", PROGRAMS)
def test_secret_columns_are_excluded(program):
    streams = _config(program)["streams"]
    for stream, columns in EXCLUDED_COLUMNS[program].items():
        assert stream in streams, stream
        select = (streams[stream] or {}).get("select", [])
        for column in columns:
            assert f"-{column}" in select, (stream, column)
