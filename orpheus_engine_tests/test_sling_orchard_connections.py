import importlib

import pytest
from dagster_sling import SlingResource

from orpheus_engine.defs.sling.assets import _isolate_missing_program_connection


@pytest.mark.parametrize("program", ["macondo", "theseus"])
@pytest.mark.parametrize("configured", [False, True])
def test_orchard_connection(monkeypatch, program, configured):
    env = f"{program.upper()}_K8S_URL"
    monkeypatch.delenv(env, raising=False)
    monkeypatch.delenv(f"{env}_SLING", raising=False)
    # An invalid Auth key must have no effect on either direct connection.
    monkeypatch.setenv("AUTH_SSH_PRIVATE_KEY_B64", "invalid-base64")
    if configured:
        monkeypatch.setenv(
            env,
            "postgresql://reader:fake@db.example.com:5432/test"
            "?sslmode=verify-full&sslrootcert=system&application_name=test",
        )
    module = importlib.import_module(f"orpheus_engine.defs.sling.programs.{program}")
    try:
        importlib.reload(module)
        conn = _isolate_missing_program_connection(getattr(module, f"{program}_db_connection"))
        initialized = SlingResource(connections=[conn]).process_config_and_initialize()
        resolved = initialized.connections[0]
        if configured:
            assert resolved.connection_string == (
                "postgresql://reader:fake@db.example.com:5432/test"
                "?sslmode=verify-full&application_name=test"
            )
        else:
            assert resolved.connection_string == (
                f"postgres://missing-env-{program}-k8s-url.invalid:5432/unset"
            )
        assert not getattr(resolved, "ssh_tunnel", None)
        assert not getattr(resolved, "ssh_private_key", None)
    finally:
        # Do not leave a derived test env reference in the imported module.
        monkeypatch.delenv(env, raising=False)
        importlib.reload(module)
