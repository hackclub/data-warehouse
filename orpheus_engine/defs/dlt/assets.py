import dlt
from dlt.destinations import postgres # Import the specific destination class

import hashlib
import os
import shutil
import tempfile
import time
from typing import Iterable, Dict, Any, Optional
import orpheus_engine.defs.loops.definitions as loops_defs
import polars as pl

from dagster import AssetExecutionContext, asset, Output, MetadataValue, AssetIn
# Removed DagsterDltResource, dlt_assets imports as they are no longer used

from ..airtable.generated_ids import AirtableIDs

# --- Helper Function for Destination ---
def warehouse_coolify_destination() -> postgres:
    """Creates and returns a configured postgres destination instance for DLT."""
    creds = os.getenv("WAREHOUSE_COOLIFY_URL")
    if not creds:
        raise ValueError("Environment variable WAREHOUSE_COOLIFY_URL is not set or empty.")
    # Initialize the destination with credentials
    # DLT handles the connection details internally
    return dlt.destinations.postgres(credentials=creds)


# --- Run-scoped dlt working directories ---
#
# All of these loads are full-refresh ("replace") syncs, so dlt's local working
# directory carries NO state worth keeping between runs. Sharing one directory
# per pipeline across runs caused two distinct production failure storms
# (2026-07-17..30, ~800 step failures):
#
#   1. A run killed mid-load (redeploy, OOM, max-runtime kill) left a pending
#      load package behind. Every later run tried to resume that package first
#      and died on FileNotFoundError (".../load/normalized/<id>/schema_updates
#      .json"), permanently red until someone happened to redeploy the
#      container (daydream_ysws_config failed this way for 13 days straight).
#   2. Overlapping runs of the same asset (15-min job overlapping itself while
#      the warehouse was slow, or overlapping the 6-hourly all-assets job)
#      raced on the same directory and deleted each other's normalize/load
#      files mid-flight.
#
# Giving every materialization its own throwaway directory removes the shared
# state entirely: nothing to resume, nothing to race on. Pipeline state that
# matters (schema versions) lives in the destination's _dlt tables and is
# restored from there on each run.
DLT_PIPELINES_ROOT = ".dlt_pipelines"


def _fresh_pipelines_dir(pipeline_name: str, run_id: str) -> str:
    """Create a unique, run-scoped dlt pipelines directory."""
    os.makedirs(DLT_PIPELINES_ROOT, exist_ok=True)
    return tempfile.mkdtemp(
        prefix=f"{pipeline_name}_{run_id[:8]}_", dir=DLT_PIPELINES_ROOT
    )


def _sweep_stale_pipeline_dirs(max_age_hours: float = 24.0) -> None:
    """
    Best-effort cleanup of run-scoped dirs leaked by crashed runs.

    Normal runs remove their own directory in a finally block; this only
    catches dirs orphaned by a hard kill. Anything older than max_age_hours is
    guaranteed dead (jobs carry dagster/max_runtime caps well below that).
    """
    try:
        now = time.time()
        for entry in os.scandir(DLT_PIPELINES_ROOT):
            try:
                if entry.is_dir(follow_symlinks=False) and (
                    now - entry.stat().st_mtime > max_age_hours * 3600
                ):
                    shutil.rmtree(entry.path, ignore_errors=True)
            except OSError:
                continue
    except OSError:
        pass


def _exception_chain_text(exc: BaseException) -> str:
    """Flatten an exception and its __cause__/__context__ chain to one string."""
    parts = []
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return "\n".join(parts)


def _dlt_retryable_reason(exc: BaseException) -> Optional[str]:
    """
    Classify a pipeline.run failure as retryable-with-fresh-state, or None.

    These are exactly the failure signatures observed in prod event_logs where
    a retry with a clean working directory succeeds:
      - local load-package corruption (missing files inside the pipelines dir)
      - a normalize job failing on those missing files
      - the destination schema-creation race between parallel assets
    Genuine data/connectivity errors return None and propagate.
    """
    text = _exception_chain_text(exc)
    if "pg_namespace_nspname_index" in text and "already exists" in text:
        return "schema_create_race"
    if "FileNotFoundError" in text or "No such file or directory" in text:
        return "local_state_corruption"
    if (
        "NormalizeJobFailed" in text
        or "unterminated quoted string" in text
        or "LoadClientJobRetry" in text
    ):
        return "corrupted_load_package"
    return None

def sanitize_postgres_column_name(col_name: str) -> str:
    """
    Sanitize column names for PostgreSQL compatibility.
    
    PostgreSQL has specific rules for identifiers:
    - Hyphens are treated as minus operators unless quoted
    - Spaces and special characters need to be handled
    - Reserved words should be avoided
    - Maximum identifier length is 63 characters
    
    This function replaces problematic characters with underscores and handles length limits.
    """
    # Replace hyphens with underscores (most common issue)
    sanitized = col_name.replace('-', '_')
    
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    
    # Replace other problematic characters
    sanitized = sanitized.replace('(', '_').replace(')', '_')
    sanitized = sanitized.replace('[', '_').replace(']', '_')
    sanitized = sanitized.replace('{', '_').replace('}', '_')
    sanitized = sanitized.replace('@', '_at_')
    sanitized = sanitized.replace('#', '_hash_')
    sanitized = sanitized.replace('%', '_percent_')
    sanitized = sanitized.replace('&', '_and_')
    sanitized = sanitized.replace('+', '_plus_')
    sanitized = sanitized.replace('=', '_equals_')
    sanitized = sanitized.replace('!', '_exclamation_')
    sanitized = sanitized.replace('?', '_question_')
    
    # Remove multiple consecutive underscores
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    
    # Ensure the name doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    
    # PostgreSQL has a 63-character limit on identifiers
    # If the name is too long, truncate it and add a hash suffix
    if len(sanitized) > 63:
        # Create a hash of the original name for uniqueness
        name_hash = hashlib.md5(col_name.encode('utf-8')).hexdigest()[:8]

        # For mailing list columns, try to preserve the ID part
        if 'loopsMailingList' in col_name and '-' in col_name:
            # Extract the ID part (between the first and second dash)
            parts = col_name.split('-')
            if len(parts) >= 3:
                mailing_list_id = parts[1]  # The ID part
                # Create a shorter name: loopsMailingList_ID_hash
                sanitized = f"loopsMailingList_{mailing_list_id}_{name_hash}"
            else:
                # Fallback to normal truncation (54 chars + _ + 8 char hash = 63)
                truncated = sanitized[:54]
                truncated = truncated.rstrip('_')
                sanitized = f"{truncated}_{name_hash}"
        else:
            # Normal truncation for non-mailing list columns (54 chars + _ + 8 char hash = 63)
            truncated = sanitized[:54]
            truncated = truncated.rstrip('_')
            sanitized = f"{truncated}_{name_hash}"

    return sanitized


def sanitize_df_columns_for_postgres(df: pl.DataFrame, context: AssetExecutionContext) -> tuple[pl.DataFrame, dict[str, str]]:
    """
    Sanitize all column names in a DataFrame for PostgreSQL compatibility.
    Logs any column name changes and returns the renamed DataFrame along with the mapping of changed columns.

    Returns:
        A tuple of (sanitized_dataframe, changed_columns_dict)
    """
    column_name_mapping = {col: sanitize_postgres_column_name(col) for col in df.columns}
    changed_columns = {k: v for k, v in column_name_mapping.items() if k != v}

    if changed_columns:
        context.log.info(f"Sanitizing {len(changed_columns)} column names for PostgreSQL compatibility:")
        for original, sanitized in changed_columns.items():
            context.log.info(f"  '{original}' -> '{sanitized}'")
        return df.rename(column_name_mapping), changed_columns

    return df, changed_columns


def _dlt_scalar_columns_from_polars(df: pl.DataFrame) -> dict[str, dict[str, Any]]:
    """Build DLT column hints so all-null scalar Airtable fields stay in Postgres."""
    columns: dict[str, dict[str, Any]] = {}

    for column, dtype in zip(df.columns, df.dtypes):
        if dtype.is_nested():
            continue

        if dtype == pl.Boolean:
            data_type = "bool"
        elif dtype.is_integer():
            data_type = "bigint"
        elif dtype.is_float() or dtype.is_decimal():
            data_type = "double"
        elif dtype == pl.Date:
            data_type = "date"
        elif dtype == pl.Datetime:
            data_type = "timestamp"
        elif dtype == pl.Time:
            data_type = "time"
        else:
            data_type = "text"

        columns[column] = {"data_type": data_type, "nullable": True}

    return columns


def create_airtable_sync_assets(
    base_name: str,
    tables: list[str],
    description: str = None,
    source_suffix: str = "",
    warehouse_dataset_name: str = None,
    source_base_name: str = None
):
    """
    Creates DLT assets for syncing multiple Airtable tables to the warehouse.
    
    Args:
        base_name: The name to use for asset naming (e.g., 'unified_ysws')
        tables: List of table names to sync (e.g., ['neighbors', 'projects'])
        description: Optional custom description for the assets
        source_suffix: Optional suffix for source asset names (e.g., '_refresh')
        warehouse_dataset_name: Override for warehouse dataset name (default: f"airtable_{base_name}")
        source_base_name: Override for the source base name in input dependencies (default: base_name)
    """
    assets_list = []  # Renamed from 'assets' to avoid potential module name conflict
    
    # Use source_base_name if provided, otherwise use base_name
    actual_source_base_name = source_base_name if source_base_name else base_name
    
    for table_name in tables:
        # Create a factory function to properly capture the table_name
        def create_sync_asset_for_table(specific_table_name):
            """Factory function that creates an asset function for a specific table"""
            @asset(
                name=f"{base_name}_{specific_table_name}_warehouse",
                compute_kind="dlt",
                group_name=f"dlt_airtable_{base_name}",
                description=description or f"Loads {base_name}.{specific_table_name} data into the warehouse.airtable_{base_name} schema.",
                ins={
                    f"{specific_table_name}{source_suffix}": AssetIn(key_prefix=["airtable", actual_source_base_name])
                }
            )
            def sync_asset(context: AssetExecutionContext, **kwargs):
                # Use specific_table_name that was captured by the factory function closure
                pipeline_name_base = f"{base_name}_to_warehouse" 
                dataset_name = warehouse_dataset_name if warehouse_dataset_name else f"airtable_{base_name}"
                table_name_warehouse = specific_table_name

                # Get the input DataFrame (use the suffixed key)
                input_key = f"{specific_table_name}{source_suffix}"
                df = kwargs[input_key]

                # Get field mappings from generated_ids - use actual_source_base_name for AirtableIDs lookup
                base_class = getattr(AirtableIDs, actual_source_base_name) 
                table_class = getattr(base_class, specific_table_name)
                field_mappings = {
                    getattr(table_class, field_name): field_name
                    for field_name in dir(table_class)
                    if not field_name.startswith('_') and field_name != 'TABLE_ID'
                }

                existing_columns = set(df.columns)

                # Build present mappings only for columns actually in the DataFrame
                present_mappings = {
                    old_name: new_name
                    for old_name, new_name in field_mappings.items()
                    if old_name in existing_columns
                }

                # Columns that will remain unchanged after renaming
                non_renamed_columns = existing_columns - set(present_mappings.keys())

                # 'taken' are names that will exist after rename for sure (cannot collide with these)
                taken = set(non_renamed_columns)

                # Group mappings by their desired target name
                from collections import defaultdict
                groups = defaultdict(list)
                for old_name, new_name in present_mappings.items():
                    groups[new_name].append(old_name)

                rename_map = {}

                def next_unique(base: str) -> str:
                    # Generate a unique name by adding numeric suffix if needed
                    candidate = base
                    i = 2
                    while candidate in taken:
                        candidate = f"{base}_{i}"
                        i += 1
                    taken.add(candidate)
                    return candidate

                # Prefix for resolving collisions, using the table name to keep it readable
                collision_prefix = f"{specific_table_name}_"

                # Build a deterministic, collision-free mapping
                for target_name, old_names in sorted(groups.items(), key=lambda kv: kv[0]):
                    old_names = sorted(old_names)  # deterministic ordering
                    if len(old_names) == 1 and target_name not in taken:
                        # No collision: use the nice name directly
                        rename_map[old_names[0]] = target_name
                        taken.add(target_name)
                    else:
                        # Collision with existing column or multiple IDs mapping to the same target name
                        # Apply prefix, and number if more than one
                        for idx, old in enumerate(old_names, start=1):
                            base = f"{collision_prefix}{target_name}" if target_name in taken or len(old_names) > 1 else target_name
                            # If there are multiple olds for the same target, make them stable with numeric suffix
                            if len(old_names) > 1:
                                base = f"{base}_{idx}"
                            unique = next_unique(base)
                            rename_map[old] = unique

                context.log.info(f"Resolved rename map for '{specific_table_name}' (total {len(rename_map)}): {rename_map}")

                # Sanity check before renaming
                context.log.info(f"Before renaming: {df.columns}")
                renamed_df = df.rename(rename_map) if rename_map else df
                context.log.info(f"After renaming: {renamed_df.columns}")
                
                # Assert no duplicates
                assert len(renamed_df.columns) == len(set(renamed_df.columns)), "Duplicate column names still present!"
                
                # Convert date/datetime strings to proper date or datetime types using Polars' parser
                context.log.info("Attempting to convert string columns to date/datetime types")
                
                # Sample size for type detection (balance between accuracy and performance)
                SAMPLE_SIZE = 100
                
                for col in renamed_df.columns:
                    if renamed_df[col].dtype == pl.Utf8:
                        # Sample first N rows to check if this is a date/datetime column
                        sample = renamed_df[col].head(SAMPLE_SIZE)
                        sample_non_null = sample.drop_nulls()
                        
                        if sample_non_null.len() == 0:
                            # No data to test, skip
                            continue
                        
                        # Try to parse sample as datetime first (on non-null values)
                        try:
                            sample_converted = sample_non_null.str.to_datetime(strict=False)
                            sample_converted_non_null = sample_converted.drop_nulls().len()
                            
                            # If ≥90% of non-null sample values parsed successfully, this is a date/datetime column
                            if sample_converted_non_null >= sample_non_null.len() * 0.9:
                                # Check if this is a date-only field or has meaningful time components
                                # Sample the first 10 non-null values to determine
                                sample_values = sample_non_null.head(10).to_list()
                                has_meaningful_time = False
                                
                                for val in sample_values:
                                    if not val:
                                        continue
                                    
                                    # Check if string contains time indicators
                                    if 'T' in val or ':' in val or (' ' in val and len(val) > 10):
                                        # Has time format, but check if it's always midnight (00:00:00)
                                        # which indicates it's actually a date field from Airtable
                                        if '00:00:00' not in val:
                                            # Has non-zero time component
                                            has_meaningful_time = True
                                            break
                                        # else: it's 00:00:00, so it's likely a date field
                                    # else: no time component at all (just YYYY-MM-DD)
                                
                                if has_meaningful_time:
                                    # Contains non-midnight time component - use datetime (TIMESTAMP in PostgreSQL)
                                    renamed_df = renamed_df.with_columns(
                                        pl.col(col).str.to_datetime(strict=False).alias(col)
                                    )
                                    
                                    # Validate that years are within valid range (1-9999)
                                    # Count invalid dates before nullifying them
                                    invalid_count = renamed_df.filter(
                                        pl.col(col).is_not_null() & 
                                        ((pl.col(col).dt.year() < 1) | (pl.col(col).dt.year() > 9999))
                                    ).height
                                    
                                    if invalid_count > 0:
                                        # Get sample years for logging (avoid materializing invalid dates)
                                        invalid_years = renamed_df.filter(
                                            pl.col(col).is_not_null() & 
                                            ((pl.col(col).dt.year() < 1) | (pl.col(col).dt.year() > 9999))
                                        ).select(pl.col(col).dt.year().alias('year')).head(5)
                                        context.log.warning(
                                            f"Column '{col}': Found {invalid_count} invalid datetime values with out-of-range years. "
                                            f"Sample invalid years: {invalid_years['year'].to_list()}. These will be set to null."
                                        )
                                    
                                    # Replace out-of-range dates with null
                                    renamed_df = renamed_df.with_columns(
                                        pl.when(
                                            pl.col(col).is_null() | 
                                            ((pl.col(col).dt.year() >= 1) & (pl.col(col).dt.year() <= 9999))
                                        )
                                        .then(pl.col(col))
                                        .otherwise(None)
                                        .alias(col)
                                    )
                                    
                                    context.log.info(
                                        f"Converted column '{col}' to DATETIME/TIMESTAMP "
                                        f"(sample: {sample_converted_non_null}/{sample_non_null.len()} values, "
                                        f"total rows: {renamed_df.height}, invalid dates nullified: {invalid_count})"
                                    )
                                else:
                                    # Date only or all times are midnight - use date (DATE in PostgreSQL)
                                    # First parse as datetime, then cast to date to drop time component
                                    renamed_df = renamed_df.with_columns(
                                        pl.col(col).str.to_datetime(strict=False).cast(pl.Date).alias(col)
                                    )
                                    
                                    # Validate that years are within valid range (1-9999)
                                    # Count invalid dates before nullifying them
                                    invalid_count = renamed_df.filter(
                                        pl.col(col).is_not_null() & 
                                        ((pl.col(col).dt.year() < 1) | (pl.col(col).dt.year() > 9999))
                                    ).height
                                    
                                    if invalid_count > 0:
                                        # Get sample years for logging (avoid materializing invalid dates)
                                        invalid_years = renamed_df.filter(
                                            pl.col(col).is_not_null() & 
                                            ((pl.col(col).dt.year() < 1) | (pl.col(col).dt.year() > 9999))
                                        ).select(pl.col(col).dt.year().alias('year')).head(5)
                                        context.log.warning(
                                            f"Column '{col}': Found {invalid_count} invalid date values with out-of-range years. "
                                            f"Sample invalid years: {invalid_years['year'].to_list()}. These will be set to null."
                                        )
                                    
                                    # Replace out-of-range dates with null
                                    renamed_df = renamed_df.with_columns(
                                        pl.when(
                                            pl.col(col).is_null() | 
                                            ((pl.col(col).dt.year() >= 1) & (pl.col(col).dt.year() <= 9999))
                                        )
                                        .then(pl.col(col))
                                        .otherwise(None)
                                        .alias(col)
                                    )
                                    
                                    context.log.info(
                                        f"Converted column '{col}' to DATE "
                                        f"(sample: {sample_converted_non_null}/{sample_non_null.len()} values, "
                                        f"total rows: {renamed_df.height}, invalid dates nullified: {invalid_count})"
                                    )
                            else:
                                context.log.debug(f"Column '{col}' does not appear to be a date/datetime column")
                        except Exception as e:
                            # Polars couldn't parse it as datetime, keep as string
                            context.log.debug(f"Could not convert '{col}' to date/datetime: {e}")
                
                # Sanitize string columns to remove/escape problematic characters
                context.log.info("Sanitizing string columns to remove problematic characters for SQL")
                for col in renamed_df.columns:
                    if renamed_df[col].dtype == pl.Utf8:
                        # Replace control characters with spaces
                        renamed_df = renamed_df.with_columns(
                            pl.col(col)
                            .str.replace_all(r'[\n\r\t\v\f\x00-\x08\x0b-\x1f\x7f]', ' ', literal=False)
                            .alias(col)
                        )
                        # Remove any remaining non-printable or problematic Unicode characters
                        # Keep only standard printable ASCII and common Unicode
                        renamed_df = renamed_df.with_columns(
                            pl.col(col)
                            .str.replace_all(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', literal=False)
                            .alias(col)
                        )
                        # Trim excessive whitespace that may have been created
                        renamed_df = renamed_df.with_columns(
                            pl.col(col)
                            .str.replace_all(r'\s+', ' ', literal=False)
                            .str.strip_chars()
                            .alias(col)
                        )

                # Sanitize column names for PostgreSQL compatibility (63 char limit, special chars)
                renamed_df, _ = sanitize_df_columns_for_postgres(renamed_df, context)
                dlt_columns = _dlt_scalar_columns_from_polars(renamed_df)
                context.log.info(
                    f"Prepared {len(dlt_columns)} scalar DLT column hint(s) for '{specific_table_name}'."
                )

                # Make DLT pipeline name unique per table to avoid state conflicts
                dlt_pipeline_name = f"{pipeline_name_base}_{specific_table_name}"

                context.log.info(f"Starting DLT pipeline '{dlt_pipeline_name}' to load data.")
                context.log.info(f"Destination: Postgres, Dataset (Schema): '{dataset_name}', Table: '{table_name_warehouse}'")
                context.log.info(f"Columns after renaming: {renamed_df.columns}")

                # Each materialization gets its own throwaway working directory
                # (see DLT_PIPELINES_ROOT comment for the two failure storms
                # this prevents), plus one retry with fresh state for the known
                # transient/corruption signatures.
                _sweep_stale_pipeline_dirs()
                pipelines_dir = _fresh_pipelines_dir(dlt_pipeline_name, context.run_id)

                def _run_load(active_dir: str):
                    pipeline = dlt.pipeline(
                        pipeline_name=dlt_pipeline_name,  # Use unique pipeline name
                        destination=warehouse_coolify_destination(),
                        dataset_name=dataset_name,
                        progress="log",
                        pipelines_dir=active_dir,
                    )
                    # Convert to dict iterator - DLT will create child tables for
                    # nested structures. Use insert_values format (PostgreSQL
                    # doesn't support parquet directly).
                    return pipeline.run(
                        data=renamed_df.iter_rows(named=True),
                        table_name=table_name_warehouse,
                        write_disposition="replace",
                        columns=dlt_columns,
                        primary_key="id",
                        loader_file_format="insert_values",
                    )

                recovered_from = None
                try:
                    try:
                        load_info = _run_load(pipelines_dir)
                    except Exception as e:
                        reason = _dlt_retryable_reason(e)
                        if reason is None:
                            context.log.error(f"DLT pipeline '{dlt_pipeline_name}' failed: {e}")
                            raise
                        context.log.warning(
                            f"DLT pipeline '{dlt_pipeline_name}' failed with recoverable "
                            f"condition '{reason}' ({e}). Retrying once with fresh state..."
                        )
                        shutil.rmtree(pipelines_dir, ignore_errors=True)
                        pipelines_dir = _fresh_pipelines_dir(dlt_pipeline_name, context.run_id)
                        load_info = _run_load(pipelines_dir)
                        recovered_from = reason
                        context.log.info(f"DLT pipeline retry succeeded after '{reason}'.")
                finally:
                    shutil.rmtree(pipelines_dir, ignore_errors=True)

                context.log.info(f"DLT pipeline run finished successfully for table '{specific_table_name}'.")
                context.log.info(f"Load Info for '{specific_table_name}': {load_info}")

                metadata = {
                    "dlt_pipeline_name": MetadataValue.text(dlt_pipeline_name),
                    "dlt_dataset_name": MetadataValue.text(dataset_name),
                    "dlt_table_name": MetadataValue.text(table_name_warehouse),
                    "write_disposition": MetadataValue.text("replace"),
                    "first_run": MetadataValue.bool(load_info.first_run),
                    "num_records": MetadataValue.int(renamed_df.height),
                    "columns": MetadataValue.text(", ".join(renamed_df.columns)),
                }
                if recovered_from:
                    metadata["recovered_from"] = MetadataValue.text(recovered_from)

                return Output(value=None, metadata=metadata)
            
            # Set the function name to match the asset name
            sync_asset.__name__ = f"{base_name}_{specific_table_name}_warehouse"
            return sync_asset
        
        # Create the asset function for this specific table and add it to our list
        asset_function = create_sync_asset_for_table(table_name)
        assets_list.append(asset_function)
    
    return assets_list

neighborhood_assets = create_airtable_sync_assets(
    base_name="neighborhood",
    tables=["neighbors", "hackatime_projects"],
    description="Loads neighborhood.neighbors and neighborhood.hackatime_projects data into the warehouse.neighborhood schema."
)

shipwrecked_assets = create_airtable_sync_assets(
    base_name="shipwrecked",
    tables=["rsvps"],
    description="Loads shipwrecked.rsvps data into the warehouse.shipwrecked schema."
)

highway_assets = create_airtable_sync_assets(
    base_name="highway",
    tables=["rsvps"],
    description="Loads highway.rsvps data into the warehouse.highway schema."
)

athena_award_assets = create_airtable_sync_assets(
    base_name="athena_award",
    tables=["registered_users", "email_slack_invites", "free_sticker_form", "projects"],
    description="Loads athena_award.registered_users, athena_award.email_slack_invites, athena_award.free_sticker_form, and athena_award.projects data into the warehouse.athena_award schema."
)

slack_nps_assets = create_airtable_sync_assets(
    base_name="slack_nps",
    tables=["nps"],
    description="Loads slack_nps.nps data into the warehouse.airtable_slack_nps schema."
)

campfire_assets = create_airtable_sync_assets(
    base_name="campfire",
    tables=[
        "event", "organizer", "hcb", "regions", "rsvp", "regional_managers",
        "organizer_interest", "daydream_events", "participants", "venues", "jay",
        "us_based_clubs", "disbursements", "emergency_disbursements", "funding_rates",
        "check_in_3", "check_in_4", "check_in_5", "camie", "captured_emails",
        "nyc_data", "amber_merch_distribution", "training", "campfire_tutorial",
        "us_bagles", "us_west", "hcb_issues", "ysws_project_submission", "ysws_config",
        "theme", "campfire_workshops", "campfire_busan", "cf_busan_scan_in", "nps",
        "t_shirt_shipping", "t_shirts_delayed_packages", "blr_submissions", "blr_voting"
    ],
    description="Loads campfire data into the warehouse.airtable_campfire schema."
)

campfire_flagship_assets = create_airtable_sync_assets(
    base_name="campfire_flagship",
    tables=["rsvps", "hour_estimation", "cool_game_hour_reduction"],
    description="Loads campfire_flagship data into the warehouse.airtable_campfire_flagship schema."
)

flavortown_assets = create_airtable_sync_assets(
    base_name="flavortown",
    tables=["users", "rsvps", "projects", "ship_events", "devlogs"],
    description="Loads Flavortown data into the warehouse.airtable_flavortown schema."
)

milkyway_assets = create_airtable_sync_assets(
    base_name="milkyway",
    tables=["users", "projects", "devlogs", "artlog"],
    description="Loads Milkyway data into the warehouse.airtable_milkyway schema."
)

sleepover_assets = create_airtable_sync_assets(
    base_name="sleepover",
    tables=[
        "registered_users", "shop", "user_shop_info", "ysws_project_submission",
        "projects", "fulfillment", "ysws_config", "pyramid", "hackdash",
        "prize_suggestion", "calendar_events", "show_and_tells",
        "travel_grant_applications"
    ],
    description="Loads sleepover data into the warehouse.airtable_sleepover schema."
)

hack_club_videos_db_assets = create_airtable_sync_assets(
    base_name="hack_club_videos_db",
    tables=["attributed_authors", "channels", "payments", "videos"],
    description="Loads hack_club_videos_db data into the warehouse.airtable_hack_club_videos_db schema."
)

# Daydream game-jam series: two Airtable bases (no app DB). Project hours are
# static per-project numbers (no per-day grain); see airtable/definitions.py.
daydream_assets = create_airtable_sync_assets(
    base_name="daydream",
    tables=[
        "ysws_project_submission",
        "projects",
        "podium_attendees",
        "patch_names",
        "ysws_config",
    ],
    description="Loads daydream data into the warehouse.airtable_daydream schema."
)

daydream_ops_assets = create_airtable_sync_assets(
    base_name="daydream_ops",
    tables=["all_attendees_ever", "projects", "confirmed_events"],
    description="Loads daydream_ops data into the warehouse.airtable_daydream_ops schema."
)

# --- DLT Asset: Loads Data into Warehouse using DLT ---
@asset(
    compute_kind="dlt", # Tagging the compute type for UI clarity
    group_name="dlt"    # Organizing asset in the UI
)
def loops_audience(
    context: AssetExecutionContext,
    loops_processed_audience: pl.DataFrame # <-- Changed dependency
):
    """
    Loads Loops contacts data from the upstream asset 'loops_processed_audience'
    into the data warehouse (Postgres) using dlt.
    """
    pipeline_name = "loops_to_warehouse"
    dataset_name = "loops" # This will typically be the schema name in Postgres
    table_name = "audience" # This will be the table name within the schema

    context.log.info(f"Starting DLT pipeline '{pipeline_name}' to load data.")
    context.log.info(f"Destination: Postgres, Dataset (Schema): '{dataset_name}', Table: '{table_name}'")

    # Store original columns for error handling
    original_columns = loops_processed_audience.columns

    # Apply column name sanitization for PostgreSQL compatibility
    sanitized_df, changed_columns = sanitize_df_columns_for_postgres(loops_processed_audience, context)

    # Check for potential data type issues
    context.log.info(f"DataFrame schema:")
    for col, dtype in sanitized_df.schema.items():
        context.log.info(f"  {col}: {dtype}")
    
    context.log.info(f"Total columns to load: {len(sanitized_df.columns)}")

    # Log a sample of the data being loaded for debugging
    sample_data = sanitized_df.head(1).to_dicts()
    if sample_data:
        context.log.info(f"Sample data row (first record):")
        for key, value in sample_data[0].items():
            if 'loopsMailingList' in key:
                context.log.info(f"  {key}: {value} (type: {type(value).__name__})")

    # Run-scoped working directory + one fresh-state retry, same rationale as
    # create_airtable_sync_assets (see DLT_PIPELINES_ROOT comment above).
    _sweep_stale_pipeline_dirs()
    pipelines_dir = _fresh_pipelines_dir(pipeline_name, context.run_id)

    def _run_load(active_dir: str):
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=warehouse_coolify_destination(),
            dataset_name=dataset_name,
            progress="log",  # Optional: display progress logs
            pipelines_dir=active_dir,
        )
        # Convert Polars DataFrame to dict iterator - DLT will handle nested structures
        return pipeline.run(
            data=sanitized_df.iter_rows(named=True),
            table_name=table_name,
            write_disposition="replace", # Options: "append", "replace", "merge"
            primary_key="email", # Specify primary key for potential merging or indexing
            loader_file_format="insert_values"  # Supports nested structures and datetime
        )

    try:
        try:
            load_info = _run_load(pipelines_dir)
        except Exception as e:
            reason = _dlt_retryable_reason(e)
            if reason is None:
                raise
            context.log.warning(
                f"DLT pipeline '{pipeline_name}' failed with recoverable condition "
                f"'{reason}' ({e}). Retrying once with fresh state..."
            )
            shutil.rmtree(pipelines_dir, ignore_errors=True)
            pipelines_dir = _fresh_pipelines_dir(pipeline_name, context.run_id)
            load_info = _run_load(pipelines_dir)
            context.log.info(f"DLT pipeline retry succeeded after '{reason}'.")

        context.log.info(f"DLT pipeline run finished successfully.")
        # Pretty-print the load info for detailed logs
        context.log.info(f"Load Info: {load_info}") # Use the __str__ representation for concise logging

        # Return Dagster metadata about the DLT load operation
        return Output(
            value=None, # This asset doesn't produce data for other Dagster assets
            metadata={
                "dlt_pipeline_name": MetadataValue.text(pipeline_name),
                "dlt_dataset_name": MetadataValue.text(dataset_name),
                "dlt_table_name": MetadataValue.text(table_name),
                "write_disposition": MetadataValue.text("replace"),
                "first_run": MetadataValue.bool(load_info.first_run),
                "columns_sanitized": MetadataValue.int(len(changed_columns)),
                "total_columns": MetadataValue.int(len(sanitized_df.columns)),
                "sanitized_column_names": MetadataValue.json(list(changed_columns.keys())) if changed_columns else MetadataValue.text("None")
            }
        )

    except Exception as e:
        context.log.error(f"DLT pipeline '{pipeline_name}' failed: {e}")
        
        # Additional debugging for column-related errors
        if "column" in str(e).lower() or "identifier" in str(e).lower():
            context.log.error("This appears to be a column-related error. Checking problematic columns:")
            for col in original_columns:
                sanitized_col = sanitize_postgres_column_name(col)
                if len(sanitized_col) > 60:  # Close to the 63-character limit
                    context.log.error(f"  Long column name: '{col}' -> '{sanitized_col}' ({len(sanitized_col)} chars)")
                if '-' in col or ' ' in col:
                    context.log.error(f"  Special chars in: '{col}' -> '{sanitized_col}'")
        
        # Re-raise the exception to mark the Dagster asset run as failed
        raise
    finally:
        shutil.rmtree(pipelines_dir, ignore_errors=True)

