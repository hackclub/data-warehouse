from orpheus_engine.defs.sling._helpers import _replication_index_specs
from orpheus_engine.defs.sling.programs.thirdspace import thirdspace_replication_config


def test_thirdspace_uses_full_refresh_to_propagate_deletes():
    assert thirdspace_replication_config["defaults"]["mode"] == "full-refresh"
    for stream_name, cfg in thirdspace_replication_config["streams"].items():
        if isinstance(cfg, dict):
            assert cfg.get("mode", "full-refresh") == "full-refresh", f"{stream_name} must use full-refresh"
            assert "update_key" not in cfg, f"{stream_name} has a leftover incremental cursor"


def test_thirdspace_has_no_incremental_index_specs():
    # Full-refresh streams need no cursor/merge indexes, so the asset does not
    # call _ensure_incremental_target_indexes.
    assert _replication_index_specs(thirdspace_replication_config) == []
