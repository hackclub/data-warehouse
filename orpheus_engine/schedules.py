import dagster as dg

# Assets excluded from the main materialize_all_assets_job
# These are either run manually or have special scheduling
EXCLUDED_FROM_MAIN_JOB: set[dg.AssetKey] = {
    dg.AssetKey("slack_member_analytics"),  # Run manually
    dg.AssetKey("slack_member_metadata_enrichment"),  # Takes too long
    dg.AssetKey("highway_github_commits"),  # One-time backfill; Highway ended Oct 2025
    # DuckLake assets run on their own schedule (materialize_ducklake_job) so the
    # lakehouse sync isn't gated on every other asset succeeding. warehouse_row_hashes
    # depends on ALL assets, so a single failure here would otherwise skip it every run
    # (it last ran 2026-01-29 for exactly this reason). See definitions.py.
    dg.AssetKey("warehouse_row_hashes"),
    dg.AssetKey("ducklake_sync"),
}

# Run statuses that count as "still in flight" for the skip-if-running schedules.
IN_FLIGHT_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]


def _make_skip_if_running_schedule(
    name: str,
    job: dg.JobDefinition,
    cron_schedule: str,
) -> dg.ScheduleDefinition:
    """
    Build a schedule that skips its tick while a previous run of the same job is
    still queued or executing.

    Why: these jobs fire on fast cadences (15 min / hourly). When the warehouse
    is slow or wedged (e.g. a long-held Postgres lock), runs used to pile up:
    overnight 2026-07-30→31 a single blocking pg_dump left 6 concurrent runs of
    the same jobs stuck plus 18 more queued. Overlapping runs of the SAME assets
    also corrupt shared local state:
      - dlt pipelines raced on .dlt_pipelines/<name> (FileNotFoundError storms,
        ~800 step failures 2026-07-17..30)
      - Sling mirrors raced on the same _tmp tables ("duplicate key value
        violates unique constraint pg_type_typname_nsp_index", and a temp table
        with exactly 2x the stream row count from two concurrent inserts).
    Skipping a tick is always safe here: every one of these jobs is a
    full-refresh sync, so the next tick does the same work.
    """

    def _evaluation_fn(context: dg.ScheduleEvaluationContext):
        in_flight = context.instance.get_runs(
            filters=dg.RunsFilter(job_name=job.name, statuses=IN_FLIGHT_STATUSES),
            limit=1,
        )
        if in_flight:
            run = in_flight[0]
            return dg.SkipReason(
                f"Skipping {job.name} tick: run {run.run_id} is still "
                f"{run.status.value}. Overlapping runs of the same sync assets "
                "corrupt shared dlt/sling state; the next tick will pick up."
            )
        return dg.RunRequest()

    return dg.ScheduleDefinition(
        name=name,
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone="America/New_York",  # keeps logs in local time
        execution_fn=_evaluation_fn,
    )


# ---------------------------------------------------------------------------
# Asset selections for the fast-cadence jobs.
#
# IMPORTANT: these selections are SUBTRACTED from materialize_all_assets_job.
# Before 2026-07-31 the same assets were materialized both every 15 minutes
# (these jobs) and inside the 6-hourly all-assets job. Whenever the two
# overlapped (the all-assets run takes hours), the same asset ran twice
# concurrently and corrupted its own shared state -- see
# _make_skip_if_running_schedule's docstring for the failure signatures.
# The 15-minute cadence strictly dominates the 6-hour one, so the all-assets
# job loses nothing by not re-syncing these.
# ---------------------------------------------------------------------------

# Unified YSWS refresh, daily Parquet backup, and warehouse assets
UNIFIED_YSWS_SELECTION = (
    dg.AssetSelection.groups("ysws_true_spend_sync") |
    dg.AssetSelection.groups("airtable_unified_ysws_projects_db_refresh") |
    dg.AssetSelection.groups("dlt_airtable_unified_ysws") |
    dg.AssetSelection.assets("unified_ysws_ysws_programs_weighted_referral_count")
)

# Frequent sync selection (every 15 minutes)
# Includes: Slack NPS, Campfire, Campfire Flagship (sources + warehouse mirrors), Zenventory
FREQUENT_SELECTION = (
    # Slack NPS
    dg.AssetSelection.assets("airtable/slack_nps/nps") |
    dg.AssetSelection.assets("slack_nps_nps_warehouse") |
    # Campfire Flagship tables
    dg.AssetSelection.assets("airtable/campfire_flagship/rsvps") |
    dg.AssetSelection.assets("airtable/campfire_flagship/hour_estimation") |
    dg.AssetSelection.assets("airtable/campfire_flagship/cool_game_hour_reduction") |
    # Campfire Flagship warehouse mirrors
    dg.AssetSelection.assets("campfire_flagship_rsvps_warehouse") |
    dg.AssetSelection.assets("campfire_flagship_hour_estimation_warehouse") |
    dg.AssetSelection.assets("campfire_flagship_cool_game_hour_reduction_warehouse") |
    # Campfire tables
    dg.AssetSelection.assets("airtable/campfire/event") |
    dg.AssetSelection.assets("airtable/campfire/organizer") |
    dg.AssetSelection.assets("airtable/campfire/hcb") |
    dg.AssetSelection.assets("airtable/campfire/regions") |
    dg.AssetSelection.assets("airtable/campfire/rsvp") |
    dg.AssetSelection.assets("airtable/campfire/regional_managers") |
    dg.AssetSelection.assets("airtable/campfire/organizer_interest") |
    dg.AssetSelection.assets("airtable/campfire/daydream_events") |
    # Campfire warehouse mirrors
    dg.AssetSelection.assets("campfire_event_warehouse") |
    dg.AssetSelection.assets("campfire_organizer_warehouse") |
    dg.AssetSelection.assets("campfire_hcb_warehouse") |
    dg.AssetSelection.assets("campfire_regions_warehouse") |
    dg.AssetSelection.assets("campfire_rsvp_warehouse") |
    dg.AssetSelection.assets("campfire_regional_managers_warehouse") |
    dg.AssetSelection.assets("campfire_organizer_interest_warehouse") |
    dg.AssetSelection.assets("campfire_daydream_events_warehouse") |
    # Zenventory (AGH Fulfillment)
    dg.AssetSelection.assets("agh_fulfillment_zenventory_items") |
    dg.AssetSelection.assets("agh_fulfillment_zenventory_inventory") |
    dg.AssetSelection.assets("agh_fulfillment_zenventory_customer_orders") |
    dg.AssetSelection.assets("agh_fulfillment_zenventory_purchase_orders") |
    dg.AssetSelection.assets("agh_fulfillment_zenventory_shipments") |
    dg.AssetSelection.assets("agh_fulfillment_labor_costs") |
    # Zenventory -> Airtable inventory sync
    dg.AssetSelection.assets("zenventory_inventory_airtable_sync") |
    # Finance 2026 analytics (dbt models)
    dg.AssetSelection.assets(dg.AssetKey(["finance_2026_analytics", "monthly_summary"])) |
    dg.AssetSelection.assets(dg.AssetKey(["finance_2026_analytics", "major_gifts"])) |
    dg.AssetSelection.assets(dg.AssetKey(["finance_2026_analytics", "metadata"])) |
    # Sleepover (source + warehouse mirrors)
    dg.AssetSelection.groups("airtable_sleepover") |
    dg.AssetSelection.groups("dlt_airtable_sleepover") |
    # Daydream game-jam series (two bases: submissions/podium + events/attendees)
    dg.AssetSelection.groups("airtable_daydream") |
    dg.AssetSelection.groups("dlt_airtable_daydream") |
    dg.AssetSelection.groups("airtable_daydream_ops") |
    dg.AssetSelection.groups("dlt_airtable_daydream_ops") |
    # Stardance (Sling warehouse mirrors)
    dg.AssetSelection.assets("stardance_warehouse_mirror") |
    dg.AssetSelection.assets("stardance_ahoy_warehouse_mirror") |
    # Stardance analytics (dbt model: per-user journey stages for Metabase)
    dg.AssetSelection.assets(dg.AssetKey(["stardance_analytics", "journey_user_stages"])) |
    # Hack Club Videos DB (source + warehouse mirrors)
    dg.AssetSelection.groups("airtable_hack_club_videos_db") |
    dg.AssetSelection.groups("dlt_airtable_hack_club_videos_db")
)

# Hackatime DAU refresh selection (hourly).
# Keep this separate from the broad all-assets job so Summer 2026 DAU freshness
# does not depend on unrelated mirrors. The hourly Hackatime model scans the
# large heartbeat mirror, so hourly is a safer cadence than the 15-minute job
# unless that dbt model becomes incremental.
HACKATIME_DAU_DBT_ASSETS: tuple[dg.AssetKey, ...] = (
    dg.AssetKey(["hackatime_analytics", "hourly_project_activity"]),
    dg.AssetKey(["summer_2026_analytics", "summer_unified_time_log"]),
    dg.AssetKey(["summer_2026_analytics", "daily_active_users"]),
    dg.AssetKey(["summer_2026_analytics", "daily_active_users_deduped"]),
    dg.AssetKey(["summer_2026_analytics", "daily_hours_logged_by_program"]),
    dg.AssetKey(["summer_2026_analytics", "dashboard_daily_totals"]),
    dg.AssetKey(["summer_2026_analytics", "dashboard_dau_methodology"]),
    dg.AssetKey(["summer_2026_analytics", "summer_2026_metadata"]),
    dg.AssetKey(["summer_2026_analytics", "dashboard_data_health"]),
)

# Auto-include upstream source mirrors (sling + airtable DLT) that the DAU dbt
# models depend on, via deps: metadata in sources.yml. This prevents the "new
# program added but its mirror hasn't run yet" failure that killed DAU for 9
# days when crescent was added. Dashbored wires these deps automatically, so new
# programs are picked up with no manual selection changes.
#
# Subtract FREQUENT_SELECTION to avoid running the same asset in two concurrent
# jobs. Exclude airtable_raw_all_bases (the multi-hour all-bases sync used by
# shiba, a historical program whose data never changes).
_dau_upstream_mirrors = (
    dg.AssetSelection.assets(*HACKATIME_DAU_DBT_ASSETS).upstream()
    - FREQUENT_SELECTION
    - dg.AssetSelection.assets("airtable_raw_all_bases_sync")
)

HACKATIME_DAU_SELECTION = (
    dg.AssetSelection.assets(*HACKATIME_DAU_DBT_ASSETS) |
    _dau_upstream_mirrors
)

# Project search (YSWS mention search + upstream processing deps)
PROJECT_SEARCH_SELECTION = dg.AssetSelection.groups("unified_ysws_db_processing")

# ---------------------------------------------------------------------------
# Jobs
#
# Every job carries a dagster/max_runtime tag so that a hung run (stuck on a
# Postgres lock, a dead source DB, an API that never returns) is terminated by
# run monitoring instead of blocking its skip-if-running schedule forever.
# Caps are ~10x the observed p50 duration (14-day window, successful runs):
#   frequent:      p50 ~3.5 min, p90 ~5 min   -> 60 min cap
#   unified_ysws:  p50 ~4 min,   p90 ~6 min   -> 60 min cap
#   hackatime_dau: p50 ~12 min,  p90 ~2.2 h   -> 4 h cap
#   project_search: new job, no p50 data yet   -> 2 h cap
#   all_assets:    takes hours                -> 6 h cap (its own cadence)
# ---------------------------------------------------------------------------

# 1. "All assets" job (excluding assets in EXCLUDED_FROM_MAIN_JOB and everything
# already synced on a faster cadence by the jobs below).
# warehouse_row_hashes is included - it dynamically depends on ALL other assets,
# so Dagster will automatically run it last after all dependencies complete
MAIN_JOB_SELECTION = (
    dg.AssetSelection.all()
    - dg.AssetSelection.assets(*EXCLUDED_FROM_MAIN_JOB)
    - UNIFIED_YSWS_SELECTION
    - FREQUENT_SELECTION
    - HACKATIME_DAU_SELECTION
    - PROJECT_SEARCH_SELECTION
)

materialize_all_assets_job = dg.define_asset_job(
    name="materialize_all_assets_job",
    selection=MAIN_JOB_SELECTION,
    tags={
        "dagster/max_runtime": "21600",  # 6 h
        "orpheus/serial-job": "materialize_all_assets_job",
    },
)

# 2. Every 6 hours schedule (30 minutes past every 6 hours, NY time)
materialize_all_assets_schedule = _make_skip_if_running_schedule(
    name="materialize_all_assets",
    job=materialize_all_assets_job,
    cron_schedule="30 */6 * * *",                           # minute hour day month weekday
)

# 3. Unified YSWS refresh, daily Parquet backup, and warehouse assets job
materialize_unified_ysws_job = dg.define_asset_job(
    name="materialize_unified_ysws_job",
    selection=UNIFIED_YSWS_SELECTION,
    tags={
        "dagster/max_runtime": "3600",  # 60 min
        "orpheus/serial-job": "materialize_unified_ysws_job",
    },
)

# 4. Every 15 minutes schedule for unified YSWS assets
unified_ysws_15min_schedule = _make_skip_if_running_schedule(
    name="unified_ysws_15min_schedule",
    job=materialize_unified_ysws_job,
    cron_schedule="*/15 * * * *",                          # every 15 minutes
)

# 5. Hackatime DAU refresh job and schedule.
materialize_hackatime_dau_job = dg.define_asset_job(
    name="materialize_hackatime_dau_job",
    selection=HACKATIME_DAU_SELECTION,
    tags={
        "dagster/max_runtime": "14400",  # 4 h
        "orpheus/serial-job": "materialize_hackatime_dau_job",
    },
)

hackatime_dau_hourly_schedule = _make_skip_if_running_schedule(
    name="hackatime_dau_hourly_schedule",
    job=materialize_hackatime_dau_job,
    # Every 2 hours: the run's p90 is ~2.2h, so hourly ticks mostly hit the
    # skip-if-running guard anyway, and each extra run is another chance to
    # deadlock against the 6-hourly all-assets job's warehouse-wide scans.
    # (Schedule name stays "hourly" -- renaming would disable it on deploy,
    # see test_schedule_names_unchanged.)
    cron_schedule="10 */2 * * *",                          # offset from all-assets :30 runs
)

# 6. Frequent sync job and schedule (every 15 minutes)
materialize_frequent_job = dg.define_asset_job(
    name="materialize_frequent_job",
    selection=FREQUENT_SELECTION,
    tags={
        "dagster/max_runtime": "3600",  # 60 min
        "orpheus/serial-job": "materialize_frequent_job",
    },
)

frequent_15min_schedule = _make_skip_if_running_schedule(
    name="frequent_15min_schedule",
    job=materialize_frequent_job,
    cron_schedule="*/15 * * * *",                          # every 15 minutes
)

# 7. Project search job and schedule (every 2 hours)
materialize_project_search_job = dg.define_asset_job(
    name="materialize_project_search_job",
    selection=PROJECT_SEARCH_SELECTION,
    tags={
        "dagster/max_runtime": "7200",  # 2 h
        "orpheus/serial-job": "materialize_project_search_job",
    },
)

project_search_2h_schedule = _make_skip_if_running_schedule(
    name="project_search_2h_schedule",
    job=materialize_project_search_job,
    cron_schedule="50 */2 * * *",                          # offset from hackatime :10 and all-assets :30
)

# 8. dbt marts catch-up.
#
# All dbt models run as ONE step (my_dbt_assets) inside materialize_all_assets_job,
# and that step waits on every upstream sync it reads from. When any one of those
# syncs fails, the step never runs and EVERY mart in the job goes stale, not just
# the ones that read the broken source. From 2026-10-02 to 2026-10-13 an expired
# LOOPS_SESSION_TOKEN (401 on loops_raw_audience) froze 32 marts this way,
# including public_unified_analytics and the HCB YSWS true-spend tables.
#
# This sensor watches finished all-assets runs. When a scheduled run ends in
# FAILURE or CANCELED and its dbt step never succeeded, it launches a dbt-only
# run of the same marts. Upstream assets are not in that run, so it builds from
# whatever the healthy syncs already committed (a broken source stays at its last
# good load). It starts only after the all-assets run has finished, so it does
# not overlap that run's syncs. On a normal run (dbt step succeeded) it does
# nothing.
DBT_MARTS_CATCHUP_SELECTION = MAIN_JOB_SELECTION & dg.AssetSelection.kind("dbt")
DBT_STEP_KEY = "my_dbt_assets"
DBT_CATCHUP_TRIGGER_STATUSES = [
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
]

materialize_dbt_marts_catchup_job = dg.define_asset_job(
    name="materialize_dbt_marts_catchup_job",
    selection=DBT_MARTS_CATCHUP_SELECTION,
    tags={
        "dagster/max_runtime": "7200",  # 2 h; the dbt step p50 is ~11 min
        "orpheus/serial-job": "materialize_dbt_marts_catchup_job",
    },
)


def _dbt_step_succeeded(instance: dg.DagsterInstance, run_id: str) -> bool:
    records = instance.get_records_for_run(
        run_id=run_id,
        of_type=dg.DagsterEventType.STEP_SUCCESS,
        limit=1000,
    ).records
    return any(
        r.event_log_entry.step_key == DBT_STEP_KEY for r in records
    )


@dg.sensor(
    name="dbt_marts_catchup_sensor",
    job=materialize_dbt_marts_catchup_job,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def dbt_marts_catchup_sensor(context: dg.SensorEvaluationContext):
    instance = context.instance
    in_flight = instance.get_runs(
        filters=dg.RunsFilter(
            job_name=materialize_dbt_marts_catchup_job.name,
            statuses=IN_FLIGHT_STATUSES,
        ),
        limit=1,
    )
    if in_flight:
        return dg.SkipReason(
            f"dbt marts catch-up run {in_flight[0].run_id} is still in flight."
        )

    finished = instance.get_runs(
        filters=dg.RunsFilter(
            job_name=materialize_all_assets_job.name,
            statuses=DBT_CATCHUP_TRIGGER_STATUSES,
            tags={"dagster/schedule_name": materialize_all_assets_schedule.name},
        ),
        limit=1,
    )
    if not finished:
        return dg.SkipReason("No failed or canceled scheduled all-assets run.")

    run = finished[0]
    # Only the newest finished run matters: an older one is superseded by it.
    if _dbt_step_succeeded(instance, run.run_id):
        return dg.SkipReason(
            f"Run {run.run_id} ended {run.status.value} but its dbt step succeeded."
        )
    return dg.RunRequest(
        # One catch-up per failed run; Dagster drops repeated run_keys.
        run_key=f"dbt-marts-catchup-{run.run_id}",
        tags={"orpheus/dbt-catchup-for-run": run.run_id},
    )


# 9. Wrap in a Definitions so Dagster can find it
defs = dg.Definitions(
    jobs=[
        materialize_all_assets_job,
        materialize_unified_ysws_job,
        materialize_hackatime_dau_job,
        materialize_frequent_job,
        materialize_project_search_job,
        materialize_dbt_marts_catchup_job,
    ],
    sensors=[dbt_marts_catchup_sensor],
    schedules=[
        materialize_all_assets_schedule,
        unified_ysws_15min_schedule,
        hackatime_dau_hourly_schedule,
        frequent_15min_schedule,
        project_search_2h_schedule,
    ],
)
