import dagster as dg

from .inventory import DROPPED_COLUMNS, HIDDEN_TABLES
from .purge import connect_warehouse, purge


@dg.op(name="purge_warehouse_secret_columns")
def purge_warehouse_secret_columns(context: dg.OpExecutionContext) -> None:
    """Drop inventory credential columns from the warehouse and hide the rest.

    See orpheus_engine/defs/secret_columns/inventory.py and runbooks/secret-columns.md.
    """
    conn = connect_warehouse()
    try:
        report = purge(conn, log=context.log.info)
    finally:
        conn.close()
    context.log.info(
        f"Checked {len(DROPPED_COLUMNS)} inventory columns and {len(HIDDEN_TABLES)} hidden "
        f"table(s): dropped {len(report.dropped)}, enabled row security on "
        f"{len(report.row_security_enabled)}, {len(report.failures)} problem(s)."
    )
    if report.failures:
        summary = "\n".join(report.failures)
        context.log.error(summary)
        raise dg.Failure(
            description=(
                f"{len(report.failures)} secret column(s) still readable. "
                f"See runbooks/secret-columns.md.\n{summary}"
            )
        )


@dg.job(
    name="warehouse_secret_column_purge_job",
    tags={"dagster/max_runtime": "1800"},  # 30 min; every statement has a timeout
)
def warehouse_secret_column_purge_job():
    purge_warehouse_secret_columns()


# Hourly, so a copy that comes back (a config regression, a re-enabled connector,
# a recreated QuickBooks token table) is gone within the hour. After the first
# clean run each tick is a few catalog reads. :40 is offset from the :30
# all-assets, :10 DAU and :50 project-search ticks.
warehouse_secret_column_purge_schedule = dg.ScheduleDefinition(
    name="warehouse_secret_column_purge_schedule",
    job=warehouse_secret_column_purge_job,
    cron_schedule="40 * * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    jobs=[warehouse_secret_column_purge_job],
    schedules=[warehouse_secret_column_purge_schedule],
)
