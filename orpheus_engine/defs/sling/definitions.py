from dagster import Definitions
from .assets import (
    hackatime_warehouse_mirror,
    hackatime_warehouse_app_indexes,
    hcer_public_github_data_warehouse_mirror,
    hcb_warehouse_mirror,
    auth_warehouse_mirror,
    sling_replication_resource,
    _program_assets,
)
from .reader_grant_drift import (
    warehouse_reader_grant_drift_job,
    warehouse_reader_grant_drift_schedule,
)

defs = Definitions(
    assets=[
        hackatime_warehouse_mirror,
        hackatime_warehouse_app_indexes,
        hcer_public_github_data_warehouse_mirror,
        hcb_warehouse_mirror,
        auth_warehouse_mirror,
        *_program_assets,
    ],
    jobs=[warehouse_reader_grant_drift_job],
    schedules=[warehouse_reader_grant_drift_schedule],
    resources={
        "sling": sling_replication_resource,
    },
)
