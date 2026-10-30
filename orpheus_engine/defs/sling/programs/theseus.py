import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


theseus_db_connection = SlingConnectionResource(
    name="THESEUS_DB",
    type="postgres",
    connection_string=_sling_connection_url("THESEUS_K8S_URL"),
)

theseus_replication_config = {
    "source": "THESEUS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "theseus.{stream_table}",
    },

    "streams": {
        # --- Core mail ---
        "public.letters": {
            "select": [
                "id", "processing_category", "aasm_state", "usps_mailer_id_id",
                "postage", "imb_serial_number", "address_id", "imb_rollover_count",
                "weight", "width", "height", "non_machinable", "created_at",
                "updated_at", "batch_id", "return_address_id", "metadata",
                "postage_type", "mailing_date", "tags", "user_facing_title",
                "printed_at", "mailed_at", "received_at", "user_id",
                "return_address_name", "letter_queue_id", "idempotency_key",
            ],  # Excludes body (letter content), rubber_stamps, recipient_email
        },
        "public.letter_queues": {
            "select": [
                "id", "name", "slug", "user_id", "letter_height", "letter_width",
                "letter_weight", "letter_processing_category", "letter_mailing_date",
                "letter_mailer_id_id", "letter_return_address_id",
                "letter_return_address_name", "user_facing_title", "tags",
                "created_at", "updated_at", "type", "template", "postage_type",
                "usps_payment_account_id", "include_qr_code",
                "hcb_payment_account_id",
            ],
        },

        # --- Addresses ---
        "public.addresses": {
            "select": [
                "id", "city", "state", "postal_code", "country", "created_at",
                "updated_at", "batch_id", "import_token",
            ],  # Excludes first_name, last_name, line_1, line_2, phone_number, email
        },
        "public.return_addresses": {
            "select": [
                "id", "city", "state", "postal_code", "country", "shared",
                "user_id", "created_at", "updated_at",
            ],  # Excludes name, line_1, line_2
        },

        # --- Batches ---
        "public.batches": {
            "select": [
                "id", "user_id", "created_at", "updated_at", "type",
                "warehouse_template_id", "address_count",
                "warehouse_user_facing_title", "aasm_state", "letter_height",
                "letter_width", "letter_weight", "letter_mailer_id_id",
                "letter_return_address_id", "letter_processing_category",
                "letter_mailing_date", "tags", "letter_return_address_name",
                "letter_queue_id", "hcb_payment_account_id", "hcb_transfer_id",
            ],  # Excludes field_mapping (jsonb, may contain PII mapping rules)
        },

        # --- Warehouse / fulfillment ---
        "public.warehouse_orders": {
            "select": [
                "id", "hc_id", "aasm_state", "user_id", "surprise",
                "user_facing_title", "user_facing_description",
                "zenventory_id", "source_tag_id", "created_at", "updated_at",
                "address_id", "dispatched_at", "mailed_at", "canceled_at",
                "carrier", "service", "postage_cost", "weight",
                "idempotency_key", "notify_on_dispatch", "batch_id",
                "template_id", "metadata", "tags", "labor_cost", "contents_cost",
            ],  # Excludes internal_notes, tracking_number, recipient_email
        },
        "public.warehouse_line_items": None,
        "public.warehouse_skus": None,
        "public.warehouse_templates": None,
        "public.warehouse_purchase_orders": {
            "select": [
                "id", "supplier_name", "supplier_id", "order_number", "notes",
                "required_by_date", "status", "zenventory_id", "user_id",
                "created_at", "updated_at",
            ],
        },
        "public.warehouse_purchase_order_line_items": None,
        "public.warehouse_purpose_codes": None,

        # --- Users ---
        "public.users": {
            "select": [
                "id", "slack_id", "email", "is_admin", "created_at", "updated_at",
                "username", "can_warehouse", "can_impersonate_public",
                "home_mid_id", "home_return_address_id", "hca_id",
                "can_use_indicia",
            ],  # Excludes icon_url
        },
        "public.public_users": {
            "select": [
                "id", "email", "created_at", "updated_at", "opted_out_of_map",
                "hca_id",
            ],
        },

        # --- Tags ---
        "public.common_tags": None,
        "public.source_tags": None,

        # --- USPS / postage ---
        # usps_indicia and usps_payment_accounts excluded at maintainer request
        # (contain check images and ACH details)
        "public.usps_mailer_ids": None,
        "public.usps_iv_mtr_events": {
            "select": [
                "id", "happened_at", "letter_id", "batch_id", "opcode",
                "zip_code", "mailer_id_id", "created_at", "updated_at",
            ],  # Excludes payload (raw USPS json)
        },

        # --- HCB integration ---
        "public.hcb_payment_accounts": {
            "select": [
                "id", "user_id", "hcb_oauth_connection_id", "organization_id",
                "organization_name", "created_at", "updated_at",
            ],
        },

        # --- Rails internals (disabled) ---
        "public.schema_migrations": {"disabled": True},
    },
}



@dg.asset(
    name="theseus_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def theseus_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Theseus (mail.hackclub.com) DB tables/columns into the warehouse."""
    context.log.info("Starting Theseus → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=theseus_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
