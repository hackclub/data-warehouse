import requests

from .sensitive import is_sensitive


class AirtableError(Exception):
    pass


AIRTABLE_META_URL = "https://api.airtable.com/v0/meta"

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 10

# What the warehouse actually ends up with. AirtableResource pins
# count/autoNumber/rating to Int64, number/currency/percent/duration to Float64
# and checkbox to Boolean, then stringifies everything else -- dates included,
# they are never parsed. dlt turns those into bigint/double/bool/text. Fields
# holding more than one value become pl.List(pl.Utf8), which dlt normalizes out
# into a child table instead of a column on the parent.
FIELD_TYPE_MAP = {
    "count": "bigint",
    "autoNumber": "bigint",
    "rating": "bigint",
    "number": "double",
    "currency": "double",
    "percent": "double",
    "duration": "double",
    "checkbox": "bool",
    "multipleSelects": "child table",
    "multipleCollaborators": "child table",
    "multipleRecordLinks": "child table",
    "multipleAttachments": "child table",
}

DEFAULT_FIELD_TYPE = "text"

# Field types whose value type is declared in options.result.type
RESULT_WRAPPER_FIELD_TYPES = {"formula", "rollup", "multipleLookupValues"}


class AirtableIntrospector:
    def __init__(self, pat: str):
        self.pat = pat
        self.headers = {
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
        }

    def list_bases(self) -> list[dict]:
        bases = []
        params = {}
        while True:
            r = self._get(f"{AIRTABLE_META_URL}/bases", params)
            bases.extend(
                {"id": b["id"], "name": b.get("name", b["id"])}
                for b in r.get("bases", [])
            )
            offset = r.get("offset")
            if not offset:
                return bases
            params = {"offset": offset}

    def introspect(self, base_id: str) -> list[dict]:
        r = self._get(f"{AIRTABLE_META_URL}/bases/{base_id}/tables")
        tables = []
        for t in r.get("tables", []):
            fields = t.get("fields", [])
            columns = [
                {
                    "name": f["name"],
                    "type": _field_type(f),
                    "udt": f.get("type", "unknown"),
                    "nullable": True,
                    "default": None,
                    "sensitive": is_sensitive(f["name"]),
                    "field_id": f["id"],
                }
                for f in fields
            ]
            tables.append({
                "name": t["name"],
                "table_id": t["id"],
                "schema": "airtable",
                "columns": columns,
                "primary_key": ["id"],
                "row_count": 0,
                "excluded": False,
                "has_updated_at": any(
                    f.get("type") == "lastModifiedTime" for f in fields
                ),
                "has_created_at": any(
                    f.get("type") == "createdTime" for f in fields
                ),
            })
        return tables

    def _get(self, url: str, params: dict | None = None) -> dict:
        try:
            r = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except requests.RequestException as e:
            raise AirtableError(f"Could not connect to Airtable: {e}")
        if r.status_code == 401:
            raise AirtableError("Invalid Airtable Personal Access Token.")
        if r.status_code == 403:
            raise AirtableError(
                "Token lacks required scopes. "
                "Needs: data.records:read, schema.bases:read"
            )
        if r.status_code != 200:
            raise AirtableError(f"Airtable API error ({r.status_code}): {r.text[:200]}")
        return r.json()


def _field_type(field: dict) -> str:
    kind = field.get("type", "")
    if kind in RESULT_WRAPPER_FIELD_TYPES:
        result = (field.get("options") or {}).get("result") or {}
        kind = result.get("type", "")
    return FIELD_TYPE_MAP.get(kind, DEFAULT_FIELD_TYPE)
