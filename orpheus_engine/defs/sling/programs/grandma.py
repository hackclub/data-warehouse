import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


# Grandma (Orchard: prod, namespace ysws-grandma-ysws).
grandma_db_connection = SlingConnectionResource(
    name="GRANDMA_DB",
    type="postgres",
    connection_string=_sling_connection_url("GRANDMA_K8S_URL"),
)


# Not mirrored: django_session, django_migrations, grandma_ratelimit and the
# four axes_* login-attempt tables (IPs and raw POST data).
# constance_constance holds feature flags, links and Slack channel IDs.
grandma_replication_config = {
    "source": "GRANDMA_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "grandma.{stream_table}",
    },

    "streams": {
        "public.accounts_profile": {
            # identity is Fernet-encrypted identity data
            "select": ["-hackatime_token", "-identity"],
        },
        "public.audit_auditentry": None,
        "public.auth_group": None,
        "public.auth_group_permissions": None,
        "public.auth_permission": None,
        "public.auth_user": {
            "select": ["-password"],
        },
        "public.auth_user_groups": None,
        "public.auth_user_user_permissions": None,
        "public.constance_constance": None,
        "public.django_admin_log": None,
        "public.django_content_type": None,
        "public.ledger_cookieentry": None,
        "public.metrics_activeday": None,
        "public.metrics_dayviews": None,
        "public.orders_order": {
            # 32-hex order nonce of unknown use; no reporting value
            "select": ["-nonce"],
        },
        "public.orders_orderline": None,
        "public.projects_category": None,
        "public.projects_complaint": None,
        "public.projects_demand": None,
        "public.projects_demand_categories": None,
        "public.projects_demand_conflicts": None,
        "public.projects_demand_excluded_subcategories": None,
        "public.projects_project": None,
        "public.projects_projectdemand": None,
        "public.projects_subcategory": None,
        "public.projects_suggestion": None,
        "public.projects_suggestionvote": None,
        "public.shop_rateclass": None,
        "public.shop_shopitem": None,
        "public.submissions_aridelivery": {
            # Raw Ari submission payload
            "select": ["-body"],
        },
        "public.submissions_arievent": None,
        "public.submissions_submission": None,
    },
}


@dg.asset(
    name="grandma_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def grandma_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Grandma DB → warehouse in a single shot."""
    context.log.info("Starting Grandma → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=grandma_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
