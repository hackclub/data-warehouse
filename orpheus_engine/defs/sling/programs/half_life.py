import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Half Life (Orchard: half-life-db, namespace ysws-half-life). The URL keeps
# sslrootcert=system for psycopg2; _sling_connection_url removes it for Sling,
# which otherwise fails with "open system: no such file or directory".
half_life_db_connection = SlingConnectionResource(
    name="HALF_LIFE_DB",
    type="postgres",
    connection_string=_sling_connection_url("HALF_LIFE_DATABASE_URL"),
)


half_life_replication_config = {
    "source": "HALF_LIFE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "half_life.{stream_table}",
    },

    "streams": {
        "public.hackatime_link": {
            "select": [
                "id", "themeProjectId", "hackatimeProject", "createdAt",
            ],
        },
        "public.post": {
            "select": [
                "-caption", "-objectKey", "-thumbnailKey", "-contentType",
                "-byteSize", "-durationSeconds", "-width", "-height",
                "-viewCount", "-hiddenAt", "-hiddenById", "-hiddenReason",
                "-weekNumber", "-commentCount", "-likeCount", "-pinnedAt",
                "-checkpointKey",
            ],
        },
        "public.program_settings": {
            "select": [
                "-submissionsOpen", "-submissionsCloseAt", "-shopOpen",
                "-shopClosesAt", "-shopGraceDays", "-reviewClaimTtlMinutes",
                "-airtableSyncEnabled", "-updatedById", "-repoSyncEnabled",
            ],
        },
        "public.session_timelapse": {
            "select": [
                "-objectKey", "-playbackUrl", "-thumbnailUrl",
                "-runtimeSeconds", "-speedupFactor", "-externalId",
            ],
        },
        "public.theme_project": {
            "select": [
                "-theme", "-description", "-coverImageKey", "-artifactLinks",
                "-designStatus", "-designReviewComments",
                "-designReviewedAt", "-designReviewedById", "-buildStatus",
                "-buildReviewComments", "-buildReviewedAt",
                "-buildReviewedById", "-tier", "-grantUsd",
                "-grantEmittedAt", "-approvedHours", "-approvedHoursAt",
                "-submissionExtensionUntil", "-deletedById",
                "-bomSavingsUsd", "-buildCoins", "-designApprovedHours",
                "-designApprovedHoursAt", "-designBankedCoins",
                "-designSpendableCoins", "-requestedTier",
                "-starterProjectId", "-bomTaxShippingUsd", "-namedAt",
                "-warmupPhase", "-repoSyncEnabled", "-repoSyncRepo",
                "-repoSyncedAt", "-repoSyncError", "-repoSyncQueuedAt",
            ],
        },
        "public.user": {
            "select": [
                "id", "email", "createdAt", "updatedAt", "slackId",
                "hackatimeUserId", "joinedProgramAt", "fraudFlagged",
            ],
        },
        "public.user_day": None,
        "public.work_session": {
            "select": [
                "-phase", "-title", "-content", "-hoursApproved",
                "-weekNumber", "-reviewComments", "-reviewedAt",
                "-reviewedById",
            ],
        },
    },
}



@dg.asset(
    name="half_life_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def half_life_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Half Life DB → warehouse in a single shot."""
    context.log.info("Starting Half Life → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=half_life_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
