# tests/tick_by_tick/test_partitioner.py

"""
Unit tests for tick_by_tick/_partitioner.py.

Uses extract_to_raw_parquet() to produce real, correctly-shaped input
data from the sample fixtures — keeping the partitioner tests honest
about the actual schema the extractor produces.

S3 upload behaviour is split into its own test class
(``TestPartitionToTicksWithS3Upload``), mirroring the pattern already
used in ``test_downloader.py``. The pure local-partitioning tests pass
``upload_to_s3=False`` explicitly to stay isolated from S3 entirely.
"""

from __future__ import annotations

import io
import json

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from b3_data_collector import __version__
from b3_data_collector.common import StageStatus
from b3_data_collector.config import Settings
from b3_data_collector.tick_by_tick import _extractor, _partitioner
from b3_data_collector.tick_by_tick._contract import (
    METADATA_KEY,
    OUTPUT_SCHEMA,
    SCHEMA_VERSION,
    ContractViolationError,
)
from b3_data_collector.tick_by_tick._feed import FeedType
from tests.conftest import place_sample_zip, tick_line, write_tick_zip


@pytest.fixture
def rv_raw_parquet(patched_paths, sample_zip_trade_date):
    """Real raw Parquet, produced by actually running the extractor."""
    place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)
    _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.RV)
    return patched_paths


class TestBuildS3KeyTicks:
    def test_key_format(self, sample_zip_trade_date):
        cfg = FeedType.RV.config
        key = _partitioner._build_s3_key_ticks(cfg.s3_prefix_ticks, sample_zip_trade_date)
        assert key == (
            f"b3/tick_by_tick/rv/ticks/year={sample_zip_trade_date:%Y}/"
            f"{sample_zip_trade_date}.parquet"
        )


class TestPartitionToTicks:
    """Local-only behaviour — S3 upload disabled to keep these isolated."""

    def test_successful_partition(self, rv_raw_parquet, sample_zip_trade_date):
        status, s3_status, tick_count = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
        )

        assert status is StageStatus.SUCCESS
        assert s3_status is StageStatus.SKIPPED
        assert tick_count == 20

        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        assert output_file.exists()

        df = pd.read_parquet(output_file)
        assert str(df["timestamp"].dt.tz) == "America/Sao_Paulo"
        # No unparseable timestamps expected from real, well-formed data
        assert df["timestamp"].isna().sum() == 0
        assert df["timestamp"].iloc[0] == pd.Timestamp("2026-06-30 13:00:00.006", tz="UTC")

    def test_successful_partition_removes_raw_parquet(
        self, rv_raw_parquet, sample_zip_trade_date
    ):
        raw_file = rv_raw_parquet["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        assert raw_file.exists()

        _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
        )

        assert not raw_file.exists()

    def test_output_columns_match_canonical_schema(
        self, rv_raw_parquet, sample_zip_trade_date
    ):
        _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
        )

        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        # Types and nullability, not just names: the schema is the contract.
        assert pq.read_schema(output_file).remove_metadata().equals(OUTPUT_SCHEMA)

    def test_records_full_provenance(self, rv_raw_parquet, sample_zip_trade_date):
        _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
        )

        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        provenance = json.loads(pq.read_schema(output_file).metadata[METADATA_KEY])
        assert provenance["schema_version"] == SCHEMA_VERSION
        assert provenance["feed"] == "RV"
        assert provenance["trade_date"] == "2026-06-30"
        assert provenance["row_count"] == provenance["source_txt_row_count"] == 20
        assert provenance["timestamp_null_count"] == 0
        assert provenance["timezone"] == "America/Sao_Paulo"
        assert provenance["collector_version"] == __version__
        assert len(provenance["source_zip_sha256"]) == 64
        assert {"collector_commit", "created_at_utc", "source_zip_name", "source_txt_name"} <= (
            provenance.keys()
        )

    def test_rows_keep_txt_order(self, patched_paths, sample_zip_trade_date):
        # Same millisecond, out of trade_id order: the TXT order must win.
        lines = [tick_line(trade_id="30"), tick_line(trade_id="10"), tick_line(trade_id="20")]
        write_tick_zip(patched_paths, FeedType.RV, sample_zip_trade_date, lines)
        _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.RV)

        _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
        )

        df = pd.read_parquet(patched_paths["rv_ticks"] / f"{sample_zip_trade_date}.parquet")
        assert df["trade_id"].tolist() == [30, 10, 20]
        assert df["source_row"].tolist() == [2, 3, 4]

    @pytest.mark.parametrize(
        ("session", "time_raw", "expected"),
        [
            # Traded on Friday afternoon for Monday's session (real B3 case).
            ("2026-07-03", "171113202", "2026-06-30 17:11:13.202"),
            # Batch published at 02:29 for the previous session.
            ("2026-06-29", "022938142", "2026-06-30 02:29:38.142"),
        ],
    )
    def test_timestamp_uses_calendar_date_not_session(
        self, patched_paths, sample_zip_trade_date, session, time_raw, expected
    ):
        lines = [tick_line(), tick_line(trade_date=session, time_raw=time_raw)]
        write_tick_zip(patched_paths, FeedType.DERIV, sample_zip_trade_date, lines)
        _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.DERIV)

        _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.DERIV, upload_to_s3=False,
        )

        df = pd.read_parquet(patched_paths["deriv_ticks"] / f"{sample_zip_trade_date}.parquet")
        assert df["trade_date"].iloc[1] == session
        assert df["timestamp"].iloc[1] == pd.Timestamp(expected, tz="America/Sao_Paulo")

    @pytest.mark.parametrize("bad_time", ["", "25000000", "250000000", "106000000", "1000000O6", "1000000060"])
    def test_unreadable_time_is_kept_and_counted(
        self, patched_paths, sample_zip_trade_date, bad_time
    ):
        lines = [tick_line(), tick_line(time_raw=bad_time)]
        write_tick_zip(patched_paths, FeedType.RV, sample_zip_trade_date, lines)
        _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.RV)

        status, _, tick_count = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
        )

        assert status is StageStatus.SUCCESS
        assert tick_count == 2
        output_file = patched_paths["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        df = pd.read_parquet(output_file)
        assert df["timestamp"].isna().tolist() == [False, True]
        assert df["time_raw"].iloc[1] == bad_time
        provenance = json.loads(pq.read_schema(output_file).metadata[METADATA_KEY])
        assert provenance["timestamp_null_count"] == 1

    def test_raw_without_provenance_fails(self, patched_paths, sample_zip_trade_date):
        # A raw Parquet left behind by a collector older than the contract.
        raw_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        pd.DataFrame({"symbol": ["BAIQ39"]}).to_parquet(raw_file, index=False)

        with pytest.raises(ContractViolationError, match="overwrite=True"):
            _partitioner.partition_to_ticks(
                trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
            )

    def test_row_count_mismatch_fails(self, rv_raw_parquet, sample_zip_trade_date):
        raw_file = rv_raw_parquet["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        table = pq.read_table(raw_file)
        provenance = json.loads(table.schema.metadata[METADATA_KEY])
        provenance["source_txt_row_count"] = 21
        pq.write_table(
            table.replace_schema_metadata(
                {**table.schema.metadata, METADATA_KEY: json.dumps(provenance).encode()}
            ),
            raw_file,
        )

        with pytest.raises(ContractViolationError, match="has 20 rows"):
            _partitioner.partition_to_ticks(
                trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
            )

    def test_skips_when_output_exists_and_overwrite_false(
        self, rv_raw_parquet, sample_zip_trade_date
    ):
        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        output_file.write_bytes(b"fake existing ticks")

        status, s3_status, tick_count = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date,
            feed=FeedType.RV,
            overwrite=False,
            upload_to_s3=False,
        )

        assert status is StageStatus.SKIPPED
        assert s3_status is StageStatus.SKIPPED
        assert tick_count is None

    def test_missing_raw_parquet_raises_file_not_found(
        self, patched_paths, sample_zip_trade_date
    ):
        # patched_paths without running the extractor first — no raw file exists
        with pytest.raises(FileNotFoundError):
            _partitioner.partition_to_ticks(
                trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
            )

    def test_empty_dataframe_raises_value_error(self, patched_paths, sample_zip_trade_date):
        # A TXT with only the header: valid layout, zero trades.
        write_tick_zip(patched_paths, FeedType.RV, sample_zip_trade_date, [])
        _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.RV)

        with pytest.raises(ValueError, match="zero rows"):
            _partitioner.partition_to_ticks(
                trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False,
            )


class TestPartitionToTicksWithS3Upload:
    """S3 upload behaviour — network fully mocked via moto."""

    @pytest.fixture
    def fake_settings(self) -> Settings:
        return Settings(
            AWS_ACCESS_KEY_ID="testing",
            AWS_SECRET_ACCESS_KEY="testing",
            AWS_S3_REGION="us-east-1",
            AWS_S3_BUCKET_B3="test-bucket",
        )

    @pytest.fixture
    def s3_bucket(self, fake_settings, monkeypatch):
        monkeypatch.setattr(_partitioner, "settings", fake_settings)
        with mock_aws():
            client = boto3.client("s3", region_name=fake_settings.AWS_S3_REGION)
            client.create_bucket(Bucket=fake_settings.AWS_S3_BUCKET_B3)
            yield client

    def test_upload_success(self, rv_raw_parquet, s3_bucket, sample_zip_trade_date):
        status, s3_status, tick_count = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=True,
        )

        assert status is StageStatus.SUCCESS
        assert s3_status is StageStatus.SUCCESS
        assert tick_count == 20

        cfg = FeedType.RV.config
        key = _partitioner._build_s3_key_ticks(cfg.s3_prefix_ticks, sample_zip_trade_date)
        obj = s3_bucket.get_object(Bucket="test-bucket", Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        assert len(df) == 20

    def test_skips_upload_when_object_already_exists(
        self, rv_raw_parquet, s3_bucket, sample_zip_trade_date
    ):
        cfg = FeedType.RV.config
        key = _partitioner._build_s3_key_ticks(cfg.s3_prefix_ticks, sample_zip_trade_date)
        s3_bucket.put_object(Bucket="test-bucket", Key=key, Body=b"already uploaded")

        _, s3_status, _ = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=True,
        )

        assert s3_status is StageStatus.SKIPPED