import hashlib
import io
import json
from datetime import date

import polars as pl
import pyarrow.parquet as pq

from orpheus_engine.defs.shared.daily_backups import (
    ParquetBackupFile,
    dataframe_to_parquet_bytes,
    typecast_dataframe_for_parquet,
    write_daily_parquet_backup,
)


def test_dataframe_to_parquet_bytes_preserves_nested_fields():
    df = pl.DataFrame(
        {
            "id": ["rec1"],
            "linked_records": [["recA", "recB"]],
            "struct_field": [{"name": "Example"}],
        }
    )

    parquet_bytes = dataframe_to_parquet_bytes(df)
    round_tripped = pl.read_parquet(io.BytesIO(parquet_bytes))
    parquet_file = pq.ParquetFile(io.BytesIO(parquet_bytes))

    assert parquet_bytes[:4] == b"PAR1"
    assert parquet_file.metadata.row_group(0).column(0).compression == "ZSTD"
    assert round_tripped.to_dict(as_series=False) == df.to_dict(as_series=False)


def test_typecast_dataframe_for_parquet_conservatively_casts_scalar_strings():
    df = pl.DataFrame(
        {
            "id": ["001", "002"],
            "record_id": ["2026-06-28", "2026-06-29"],
            "postal_code": ["02139", "94110"],
            "created_at": ["2026-06-28T14:30:00Z", "2026-06-29T15:45:30Z"],
            "birthday": ["2008-04-03", "2009-05-04"],
            "age": ["17", "18"],
            "score": ["1.5", "2"],
            "subscribed": ["true", "false"],
            "is_visible": ["1", "0"],
            "title": ["123 Main", "456 Main"],
        },
        schema={
            "id": pl.String,
            "record_id": pl.String,
            "postal_code": pl.String,
            "created_at": pl.String,
            "birthday": pl.String,
            "age": pl.String,
            "score": pl.String,
            "subscribed": pl.String,
            "is_visible": pl.String,
            "title": pl.String,
        },
    )

    typed_df = typecast_dataframe_for_parquet(df)
    parquet_df = pl.read_parquet(io.BytesIO(dataframe_to_parquet_bytes(df)))

    assert typed_df.schema["id"] == pl.String
    assert typed_df.schema["record_id"] == pl.String
    assert typed_df.schema["postal_code"] == pl.String
    assert typed_df.schema["created_at"] == pl.Datetime("us", "UTC")
    assert typed_df.schema["birthday"] == pl.Date
    assert typed_df.schema["age"] == pl.Int64
    assert typed_df.schema["score"] == pl.Float64
    assert typed_df.schema["subscribed"] == pl.Boolean
    assert typed_df.schema["is_visible"] == pl.Boolean
    assert typed_df.schema["title"] == pl.String
    assert parquet_df.schema == typed_df.schema
    assert typed_df["id"].to_list() == ["001", "002"]
    assert typed_df["postal_code"].to_list() == ["02139", "94110"]
    assert typed_df["age"].to_list() == [17, 18]
    assert typed_df["score"].to_list() == [1.5, 2.0]
    assert typed_df["subscribed"].to_list() == [True, False]
    assert typed_df["is_visible"].to_list() == [True, False]


def test_daily_parquet_backup_local_overwrites_in_place(monkeypatch, tmp_path):
    monkeypatch.setenv("HC_WAREHOUSE_CSV_BACKUPS_ROOT", str(tmp_path))

    first_content = dataframe_to_parquet_bytes(pl.DataFrame({"email": ["first@example.com"]}))
    second_content = dataframe_to_parquet_bytes(
        pl.DataFrame({"email": ["first@example.com", "second@example.com"]})
    )

    first = ParquetBackupFile(
        filename="loops_audience.parquet",
        table="loops_audience",
        content=first_content,
        row_count=1,
        column_count=1,
    )
    second = ParquetBackupFile(
        filename="loops_audience.parquet",
        table="loops_audience",
        content=second_content,
        row_count=2,
        column_count=1,
    )

    write_daily_parquet_backup(
        source="loops_audience",
        files=[first],
        run_id="run-1",
        snapshot_date=date(2026, 6, 28),
    )
    write_daily_parquet_backup(
        source="loops_audience",
        files=[second],
        run_id="run-2",
        snapshot_date=date(2026, 6, 28),
    )

    backup_dir = tmp_path / "loops_audience" / "2026" / "06" / "28"
    parquet_files = list(backup_dir.glob("*.parquet"))
    assert parquet_files == [backup_dir / "loops_audience.parquet"]
    assert (backup_dir / "loops_audience.parquet").read_bytes() == second.content
    assert pl.read_parquet(backup_dir / "loops_audience.parquet").height == 2

    metadata = json.loads((backup_dir / "metadata.json").read_text())
    assert metadata["snapshot_date"] == "2026-06-28"
    assert metadata["source"] == "loops_audience"
    assert metadata["run_id"] == "run-2"
    assert metadata["files"] == [
        {
            "filename": "loops_audience.parquet",
            "table": "loops_audience",
            "row_count": 2,
            "column_count": 1,
            "sha256": hashlib.sha256(second.content).hexdigest(),
            "byte_size": len(second.content),
        }
    ]


def test_daily_parquet_backup_local_removes_stale_payloads(monkeypatch, tmp_path):
    monkeypatch.setenv("HC_WAREHOUSE_CSV_BACKUPS_ROOT", str(tmp_path))
    backup_dir = tmp_path / "airtable_unified_ysws_db" / "2026" / "06" / "28"
    backup_dir.mkdir(parents=True)
    (backup_dir / "old_table.csv").write_text("id\nrec_old\n")
    (backup_dir / "old_table.parquet").write_bytes(
        dataframe_to_parquet_bytes(pl.DataFrame({"id": ["rec_old"]}))
    )

    write_daily_parquet_backup(
        source="airtable_unified_ysws_db",
        files=[
            ParquetBackupFile(
                filename="approved_projects.parquet",
                table="approved_projects",
                content=dataframe_to_parquet_bytes(pl.DataFrame({"id": ["rec1"]})),
                row_count=1,
                column_count=1,
            ),
            ParquetBackupFile(
                filename="ysws_programs.parquet",
                table="ysws_programs",
                content=dataframe_to_parquet_bytes(pl.DataFrame({"id": ["rec2"]})),
                row_count=1,
                column_count=1,
            ),
        ],
        run_id="run-1",
        snapshot_date=date(2026, 6, 28),
    )

    assert not list(backup_dir.glob("*.csv"))
    assert sorted(path.name for path in backup_dir.glob("*.parquet")) == [
        "approved_projects.parquet",
        "ysws_programs.parquet",
    ]
    metadata = json.loads((backup_dir / "metadata.json").read_text())
    assert [item["table"] for item in metadata["files"]] == [
        "approved_projects",
        "ysws_programs",
    ]
