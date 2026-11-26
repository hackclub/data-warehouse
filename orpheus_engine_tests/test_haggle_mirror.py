import dagster as dg

from orpheus_engine.defs.sling.programs import haggle as haggle_mod


class _RecordingSling(dg.ConfigurableResource):
    def replicate(self, context, replication_config):
        raise AssertionError("sling.replicate must not be called when HAGGLE_K8S_URL is unset")


def test_haggle_mirror_skips_cleanly_when_env_unset(monkeypatch):
    monkeypatch.delenv("HAGGLE_K8S_URL", raising=False)
    monkeypatch.delenv("HAGGLE_K8S_URL_SLING", raising=False)

    with dg.instance_for_test() as inst:
        res = dg.materialize(
            [haggle_mod.haggle_warehouse_mirror],
            instance=inst,
            resources={"sling": _RecordingSling()},
        )
        assert res.success
        ev = inst.get_latest_materialization_event(dg.AssetKey("haggle_warehouse_mirror"))
        md = ev.asset_materialization.metadata
        assert md["replicated"].value is False
        assert md["skipped_reason"].value == "HAGGLE_K8S_URL_unset"


def test_haggle_mirror_replicates_when_env_set(monkeypatch):
    monkeypatch.setenv(
        "HAGGLE_K8S_URL",
        "postgresql://hc_warehouse_reader:pw@example.invalid:5432/ysws_haggle_db?sslmode=verify-full",
    )
    seen_configs: list[dict] = []

    class _FakeSling(dg.ConfigurableResource):
        def replicate(self, context, replication_config):
            seen_configs.append(replication_config)
            return iter([])

    with dg.instance_for_test() as inst:
        res = dg.materialize(
            [haggle_mod.haggle_warehouse_mirror],
            instance=inst,
            resources={"sling": _FakeSling()},
        )
        assert res.success
        ev = inst.get_latest_materialization_event(dg.AssetKey("haggle_warehouse_mirror"))
        assert ev.asset_materialization.metadata["replicated"].value is True

    assert seen_configs == [haggle_mod.haggle_replication_config]
