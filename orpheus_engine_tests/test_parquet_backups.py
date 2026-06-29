import hashlib
import io
import json
from datetime import date

import polars as pl
import pyarrow.parquet as pq

from orpheus_engine.defs.shared.daily_backups import (
    ParquetBackupFile,
    dataframe_to_parquet_bytes,
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
