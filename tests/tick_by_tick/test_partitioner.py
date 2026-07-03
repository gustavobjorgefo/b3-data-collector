# tests/tick_by_tick/test_partitioner.py

"""
Unit tests for tick_by_tick/_partitioner.py.

Uses extract_to_raw_parquet() to produce real, correctly-shaped input
data from the sample fixtures — keeping the partitioner tests honest
about the actual schema the extractor produces.
"""

from __future__ import annotations

import pandas as pd
import pytest

from b3_data_collector.tick_by_tick import _extractor, _partitioner
from b3_data_collector.common import StageStatus
from b3_data_collector.tick_by_tick._feed import FeedType
from tests.conftest import place_sample_zip


@pytest.fixture
def rv_raw_parquet(patched_paths, sample_zip_trade_date):
    """Real raw Parquet, produced by actually running the extractor."""
    place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)
    _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.RV)
    return patched_paths


class TestPartitionToTicks:
    def test_successful_partition(self, rv_raw_parquet, sample_zip_trade_date):
        status, tick_count = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV
        )

        assert status is StageStatus.SUCCESS
        assert tick_count == 20

        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        assert output_file.exists()

        df = pd.read_parquet(output_file)
        assert "timestamp" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])
        # No unparseable timestamps expected from real, well-formed data
        assert df["timestamp"].isna().sum() == 0

    def test_output_columns_match_canonical_schema(
        self, rv_raw_parquet, sample_zip_trade_date
    ):
        _partitioner.partition_to_ticks(trade_date=sample_zip_trade_date, feed=FeedType.RV)

        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        df = pd.read_parquet(output_file)

        assert list(df.columns) == list(_partitioner._OUTPUT_COLUMNS)

    def test_skips_when_output_exists_and_overwrite_false(
        self, rv_raw_parquet, sample_zip_trade_date
    ):
        output_file = rv_raw_parquet["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        output_file.write_bytes(b"fake existing ticks")

        status, tick_count = _partitioner.partition_to_ticks(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, overwrite=False
        )

        assert status is StageStatus.SKIPPED
        assert tick_count is None

    def test_missing_raw_parquet_raises_file_not_found(
        self, patched_paths, sample_zip_trade_date
    ):
        # patched_paths without running the extractor first — no raw file exists
        with pytest.raises(FileNotFoundError):
            _partitioner.partition_to_ticks(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )

    def test_empty_dataframe_raises_value_error(
        self, patched_paths, sample_zip_trade_date, monkeypatch
    ):
        # Write a raw parquet with the right columns but zero rows, to
        # trigger _validate_output's empty-dataframe check directly.
        raw_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        empty_df = pd.DataFrame(columns=[
            "trade_date", "symbol", "price", "quantity", "time",
            "buyer_broker", "seller_broker", "session_type",
            "update_action", "trade_id", "reference_date", "channel_type",
        ])
        empty_df.to_parquet(raw_file, index=False)

        with pytest.raises(ValueError):
            _partitioner.partition_to_ticks(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )