from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import polars as pl
from dotenv import dotenv_values


BACKUP_ENV_PREFIX = "HC_WAREHOUSE_CSV_BACKUPS_"
LOCAL_ROOT_ENV = f"{BACKUP_ENV_PREFIX}ROOT"
PREFIX_ENV = f"{BACKUP_ENV_PREFIX}PREFIX"
BUCKET_ENV = f"{BACKUP_ENV_PREFIX}BUCKET_NAME"
ACCESS_KEY_ENV = f"{BACKUP_ENV_PREFIX}ACCESS_KEY_ID"
SECRET_KEY_ENV = f"{BACKUP_ENV_PREFIX}SECRET_ACCESS_KEY"
ENDPOINT_ENV = f"{BACKUP_ENV_PREFIX}ENDPOINT"
REGION_ENV = f"{BACKUP_ENV_PREFIX}REGION"
BACKUP_TIMEZONE = ZoneInfo("America/New_York")
PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
PAYLOAD_EXTENSIONS = (".csv", ".parquet")


@dataclass(frozen=True)
class ParquetBackupFile:
    filename: str
    table: str
    content: bytes
    row_count: int
    column_count: int


@dataclass(frozen=True)
class BackupConfig:
    backend: str
    root: str
    bucket_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region_name: str = "auto"
    prefix: str = ""


@dataclass(frozen=True)
class ParquetBackupResult:
    source: str
    snapshot_date: str
    metadata_location: str
    file_locations: tuple[str, ...]
    metadata: dict[str, Any]


def dataframe_to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serialize a Polars DataFrame to zstd-compressed Parquet bytes."""
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    return buffer.getvalue()


def write_daily_parquet_backup(
    *,
    source: str,
    files: Iterable[ParquetBackupFile],
    run_id: Optional[str],
    snapshot_date: Optional[date] = None,
    log: Any = None,
) -> Optional[ParquetBackupResult]:
    """
    Write a date-partitioned daily Parquet snapshot and metadata.json.

    Returns None only when no backup configuration is present. If configuration
    is partially present or a write fails, the caller should fail too.
    """
    config = get_backup_config()
    if config is None:
        _log(
            log,
            "warning",
            f"Parquet backup skipped for {source}: no {BACKUP_ENV_PREFIX} configuration found.",
        )
        return None

    snapshot = snapshot_date or datetime.now(BACKUP_TIMEZONE).date()
    snapshot_date_str = snapshot.isoformat()
    partition_prefix = _join_key_parts(
        source,
        f"{snapshot:%Y}",
        f"{snapshot:%m}",
        f"{snapshot:%d}",
    )

    file_list = list(files)
    if not file_list:
        raise ValueError("Cannot write a Parquet backup with no files.")

    expected_payload_keys = {
        _join_key_parts(config.prefix, partition_prefix, backup_file.filename)
        for backup_file in file_list
    }
    _delete_stale_payload_files(config, partition_prefix, expected_payload_keys)

    file_metadata: list[dict[str, Any]] = []
    file_locations: list[str] = []
    for backup_file in sorted(file_list, key=lambda item: item.filename):
        object_key = _join_key_parts(config.prefix, partition_prefix, backup_file.filename)
        _put_object(config, object_key, backup_file.content, PARQUET_CONTENT_TYPE)
        file_locations.append(_location(config, object_key))
        file_metadata.append(
            {
                "filename": backup_file.filename,
                "table": backup_file.table,
                "row_count": backup_file.row_count,
                "column_count": backup_file.column_count,
                "sha256": hashlib.sha256(backup_file.content).hexdigest(),
                "byte_size": len(backup_file.content),
            }
        )

    metadata = {
        "snapshot_date": snapshot_date_str,
        "generated_at": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "source": source,
        "run_id": run_id,
        "files": file_metadata,
    }
    metadata_bytes = (
        json.dumps(metadata, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    metadata_key = _join_key_parts(config.prefix, partition_prefix, "metadata.json")
    _put_object(config, metadata_key, metadata_bytes, "application/json; charset=utf-8")

    _log(
        log,
        "info",
        f"Wrote {len(file_metadata)} Parquet backup file(s) for {source} to {_location(config, _join_key_parts(config.prefix, partition_prefix))}",
    )
    return ParquetBackupResult(
        source=source,
        snapshot_date=snapshot_date_str,
        metadata_location=_location(config, metadata_key),
        file_locations=tuple(file_locations),
        metadata=metadata,
    )


def get_backup_config() -> Optional[BackupConfig]:
    _load_backup_env_if_needed()

    root = os.environ.get(LOCAL_ROOT_ENV)
    if root:
        parsed = urlparse(root)
        if parsed.scheme == "s3":
            return _s3_config_from_root(root)
        if parsed.scheme == "file":
            root = parsed.path
        return BackupConfig(backend="local", root=root)

    configured_values = {
        key: os.environ.get(key)
        for key in (BUCKET_ENV, ACCESS_KEY_ENV, SECRET_KEY_ENV, ENDPOINT_ENV)
    }
    if not any(configured_values.values()):
        return None

    missing = [key for key, value in configured_values.items() if not value]
    if missing:
        raise ValueError(
            "Incomplete backup configuration. Missing: " + ", ".join(missing)
        )

    return BackupConfig(
        backend="s3",
        root=f"s3://{configured_values[BUCKET_ENV]}",
        bucket_name=configured_values[BUCKET_ENV],
        endpoint_url=configured_values[ENDPOINT_ENV],
        access_key_id=configured_values[ACCESS_KEY_ENV],
        secret_access_key=configured_values[SECRET_KEY_ENV],
        region_name=os.environ.get(REGION_ENV, "auto"),
        prefix=os.environ.get(PREFIX_ENV, ""),
    )


def _s3_config_from_root(root: str) -> BackupConfig:
    parsed = urlparse(root)
    missing = [
        key
        for key in (ACCESS_KEY_ENV, SECRET_KEY_ENV, ENDPOINT_ENV)
        if not os.environ.get(key)
    ]
    if missing:
        raise ValueError(
            "Incomplete backup configuration. Missing: " + ", ".join(missing)
        )

    prefix_parts = [parsed.path.strip("/"), os.environ.get(PREFIX_ENV, "")]
    return BackupConfig(
        backend="s3",
        root=root,
        bucket_name=parsed.netloc,
        endpoint_url=os.environ[ENDPOINT_ENV],
        access_key_id=os.environ[ACCESS_KEY_ENV],
        secret_access_key=os.environ[SECRET_KEY_ENV],
        region_name=os.environ.get(REGION_ENV, "auto"),
        prefix=_join_key_parts(*prefix_parts),
    )


def _load_backup_env_if_needed() -> None:
    if _backup_env_is_sufficient():
        return

    for dotenv_path in _candidate_dotenv_paths():
        if not dotenv_path.exists():
            continue
        values = dotenv_values(dotenv_path)
        found = False
        for key, value in values.items():
            if key and key.startswith(BACKUP_ENV_PREFIX) and value is not None:
                os.environ.setdefault(key, value)
                found = True
        if found:
            if _backup_env_is_sufficient():
                return


def _backup_env_is_sufficient() -> bool:
    if os.environ.get(LOCAL_ROOT_ENV):
        return True
    required_s3_env = (BUCKET_ENV, ACCESS_KEY_ENV, SECRET_KEY_ENV, ENDPOINT_ENV)
    return all(os.environ.get(key) for key in required_s3_env)


def _candidate_dotenv_paths() -> list[Path]:
    candidates: list[Path] = []
    cwd = Path.cwd().resolve()

    for directory in (cwd, *cwd.parents):
        candidates.append(directory / ".env")

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        git_common_dir = result.stdout.strip()
        if result.returncode == 0 and git_common_dir:
            git_common_path = Path(git_common_dir)
            if not git_common_path.is_absolute():
                git_common_path = cwd / git_common_path
            candidates.append(git_common_path.resolve().parent / ".env")
    except (OSError, subprocess.SubprocessError):
        pass

    seen: set[Path] = set()
    unique_candidates: list[Path] = []
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            unique_candidates.append(candidate)
    return unique_candidates


def _delete_stale_payload_files(
    config: BackupConfig,
    partition_prefix: str,
    expected_payload_keys: set[str],
) -> None:
    folder_key = _join_key_parts(config.prefix, partition_prefix)
    if config.backend == "local":
        folder_path = Path(config.root) / folder_key
        if not folder_path.exists():
            return
        for payload_path in folder_path.iterdir():
            if payload_path.suffix not in PAYLOAD_EXTENSIONS:
                continue
            key = _join_key_parts(folder_key, payload_path.name)
            if key not in expected_payload_keys:
                payload_path.unlink()
        return

    client = _s3_client(config)
    prefix = f"{folder_key}/" if folder_key else ""
    continuation_token = None
    while True:
        kwargs = {"Bucket": config.bucket_name, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**kwargs)
        stale_keys = [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(PAYLOAD_EXTENSIONS)
            and item["Key"] not in expected_payload_keys
        ]
        if stale_keys:
            client.delete_objects(
                Bucket=config.bucket_name,
                Delete={"Objects": [{"Key": key} for key in stale_keys]},
            )
        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")


def _put_object(
    config: BackupConfig,
    key: str,
    content: bytes,
    content_type: str,
) -> None:
    if config.backend == "local":
        path = Path(config.root) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return

    _s3_client(config).put_object(
        Bucket=config.bucket_name,
        Key=key,
        Body=content,
        ContentType=content_type,
    )


def _s3_client(config: BackupConfig) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name=config.region_name,
        config=Config(signature_version="s3v4"),
    )


def _location(config: BackupConfig, key: str) -> str:
    if config.backend == "local":
        return str(Path(config.root) / key)
    return f"s3://{config.bucket_name}/{key}"


def _join_key_parts(*parts: Optional[str]) -> str:
    return "/".join(
        part.strip("/")
        for part in parts
        if part is not None and part.strip("/")
    )


def _log(log: Any, level: str, message: str) -> None:
    if log is None:
        return
    log_fn = getattr(log, level, None)
    if log_fn is None:
        return
    log_fn(message)
