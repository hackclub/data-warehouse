import dagster as dg

from .users import airtable_enterprise_users


defs = dg.Definitions(
    assets=[airtable_enterprise_users],
)
