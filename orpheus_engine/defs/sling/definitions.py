from dagster import Definitions
from .assets import (
    hackatime_warehouse_mirror,
    hackatime_warehouse_app_indexes,
    hcer_public_github_data_warehouse_mirror,
    journey_warehouse_mirror,
    shipwrecked_the_bay_warehouse_mirror,
    summer_of_making_2025_warehouse_mirror,
    hackatime_legacy_warehouse_mirror,
    flavortown_warehouse_mirror,
    hack_club_the_game_warehouse_mirror,
    blueprint_warehouse_mirror,
    stasis_warehouse_mirror,
    fallout_warehouse_mirror,
    horizons_warehouse_mirror,
    midnight_warehouse_mirror,
    stack_warehouse_mirror,
    offtrack_warehouse_mirror,
    macondo_warehouse_mirror,
    beest_warehouse_mirror,
    siege_warehouse_mirror,
    construct_warehouse_mirror,
    carnival_warehouse_mirror,
    attend_warehouse_mirror,
    flavortown_ahoy_warehouse_mirror,
    stardance_ahoy_warehouse_mirror,
    stardance_warehouse_mirror,
    hcb_warehouse_mirror,
    # review_warehouse_mirror deliberately not imported: its source Postgres no
    # longer exists (Shipwrights moved to MySQL on a third-party host, May
    # 2026), so the asset failed every all-assets run for 2 months -- it was a
    # big part of why materialize_all_assets_job had ZERO successful runs in
    # the 14 days to 2026-07-31. Re-register it only once REVIEW_COOLIFY_URL
    # points somewhere real; see the config comment in assets.py.
    joe_warehouse_mirror,
    auth_warehouse_mirror,  # absolute minimum permissions for monthly active stats
    sling_replication_resource,
)

defs = Definitions(
    assets=[
        hackatime_warehouse_mirror,
        hackatime_warehouse_app_indexes,
        hcer_public_github_data_warehouse_mirror,
        journey_warehouse_mirror,
        shipwrecked_the_bay_warehouse_mirror,
        summer_of_making_2025_warehouse_mirror,
        hackatime_legacy_warehouse_mirror,
        flavortown_warehouse_mirror,
        hack_club_the_game_warehouse_mirror,
        blueprint_warehouse_mirror,
        stasis_warehouse_mirror,
        fallout_warehouse_mirror,
        horizons_warehouse_mirror,
        midnight_warehouse_mirror,
        stack_warehouse_mirror,
        offtrack_warehouse_mirror,
        macondo_warehouse_mirror,
        beest_warehouse_mirror,
        siege_warehouse_mirror,
        construct_warehouse_mirror,
        carnival_warehouse_mirror,
        attend_warehouse_mirror,
        flavortown_ahoy_warehouse_mirror,
        stardance_ahoy_warehouse_mirror,
        stardance_warehouse_mirror,
        hcb_warehouse_mirror,
        # review_warehouse_mirror: source DB gone, see import comment above.
        joe_warehouse_mirror,
        auth_warehouse_mirror,
    ],
    resources={
        "sling": sling_replication_resource,
    },
)
