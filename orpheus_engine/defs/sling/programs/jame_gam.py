import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# jame_gam (Orchard source; schema checked 2026-10-31).
jame_gam_db_connection = SlingConnectionResource(
    name="JAME_GAM_DB",
    type="postgres",
    connection_string=_sling_connection_url("JAME_GAM_K8S_URL"),
)


# OAuth accounts, sessions, MCP credentials, webhook secrets and worker queues stay out.
# Arbitrary review/ingest payloads and delivery errors may embed credentials or contact data.
# Program stores encrypted fraud keys and bearer review/webhook tokens; VM URLs carry access.
# Invite acceptance matches the authenticated email; its id is not a bearer link.
# No application key/value settings tables exist (catalog checked 2026-10-31).
jame_gam_replication_config = {
    "source": "JAME_GAM_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "jame_gam.{stream_table}"
    },
    "streams": {
        "ariw.cdnScreenshots": None,
        "ariw.projectMakerEligibility": None,
        "ariw.submissionEvidenceVersion": None,
        "ariw.submissionFile": None,
        "ariw.submissionFileHours": None,
        "ariw.submissionPastProjects": None,
        "public.ActivityEvent": {
            "select": [
                "-meta"
            ]
        },
        "public.ChecklistItem": None,
        "public.Commit": None,
        "public.Devlog": None,
        "public.Draft": {
            "select": [
                "-fieldValues",
                "-checks",
                "-adjustments",
                "-fixChecks",
                "-collaboratorDeflates",
                "-collaboratorNotes"
            ]
        },
        "public.ElapsedClip": None,
        "public.Flag": {
            "select": [
                "-matched"
            ]
        },
        "public.FlagRule": {
            "select": [
                "-config"
            ]
        },
        "public.FraudCheck": None,
        "public.HoursBreakdown": None,
        "public.Invite": None,
        "public.Maker": None,
        "public.Membership": None,
        "public.OutboundDelivery": {
            "select": [
                "-url",
                "-errorDetail",
                "-payload"
            ]
        },
        "public.OutboundEndpoint": {
            "select": [
                "-url",
                "-secretEnc"
            ]
        },
        "public.Program": {
            "select": [
                "-fraudApiKeyEnc",
                "-fraudWebhookToken",
                "-priorityReviewToken"
            ]
        },
        "public.Review": {
            "select": [
                "-fieldValues",
                "-checklist",
                "-adjustments",
                "-fixChecks",
                "-collaboratorDeflates",
                "-collaboratorNotes"
            ]
        },
        "public.ReviewField": None,
        "public.ReviewerNote": None,
        "public.ReviewerVm": {
            "select": [
                "-guacUrl",
                "-rdpUri",
                "-rdpPasswordEnc"
            ]
        },
        "public.Snippet": None,
        "public.Submission": {
            "select": [
                "-programMeta",
                "-aiCheckResult",
                "-preReview"
            ]
        },
        "public.SubmissionCollaborator": None,
        "public.SubmissionOpen": None,
        "public.User": None,
        "public.WebhookDelivery": {
            "select": [
                "-errorDetail"
            ]
        }
    }
}


@dg.asset(
    name="jame_gam_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def jame_gam_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the jame_gam DB → warehouse in a single shot."""
    context.log.info("Starting jame_gam → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=jame_gam_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
