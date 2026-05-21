import dagster as dg

from .events import airtable_audit_log_events


defs = dg.Definitions(
    assets=[airtable_audit_log_events],
)
