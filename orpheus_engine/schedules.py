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

HACKATIME_DAU_SELECTION = (
    dg.AssetSelection.assets("hackatime_warehouse_mirror") |
    dg.AssetSelection.assets(*HACKATIME_DAU_DBT_ASSETS)
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
materialize_all_assets_job = dg.define_asset_job(
    name="materialize_all_assets_job",
    selection=(
        dg.AssetSelection.all()
        - dg.AssetSelection.assets(*EXCLUDED_FROM_MAIN_JOB)
        - UNIFIED_YSWS_SELECTION
        - FREQUENT_SELECTION
        - HACKATIME_DAU_SELECTION
        - PROJECT_SEARCH_SELECTION
    ),
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

# 8. Wrap in a Definitions so Dagster can find it
defs = dg.Definitions(
    jobs=[
        materialize_all_assets_job,
        materialize_unified_ysws_job,
        materialize_hackatime_dau_job,
        materialize_frequent_job,
        materialize_project_search_job,
    ],
    schedules=[
        materialize_all_assets_schedule,
        unified_ysws_15min_schedule,
        hackatime_dau_hourly_schedule,
        frequent_15min_schedule,
        project_search_2h_schedule,
    ],
)
