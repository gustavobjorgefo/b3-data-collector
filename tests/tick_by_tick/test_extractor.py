# tests/tick_by_tick/test_extractor.py

"""
Unit tests for tick_by_tick/_extractor.py — parses real (trimmed) B3 ZIPs.

Uses tests/fixtures/sample_rv.zip and sample_deriv.zip, built from real
B3 downloads via scripts/build_test_fixture.py, so these tests exercise
the actual on-disk format (separator, encoding, price/time formatting)
rather than a hand-written approximation of it.
"""

from __future__ import annotations

import zipfile

import pandas as pd
import pytest

from b3_data_collector.common import StageStatus
from b3_data_collector.tick_by_tick import _extractor
from b3_data_collector.tick_by_tick._feed import FeedType
from tests.conftest import place_sample_zip


class TestExtractToRawParquetRV:
    def test_successful_extraction(self, patched_paths, sample_zip_trade_date):
        place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.RV
        )

        assert status is StageStatus.SUCCESS

        output_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        assert output_file.exists()

        df = pd.read_parquet(output_file)
        assert len(df) == 20

        # Canonical English column names present after rename
        expected_columns = {
            "trade_date", "symbol", "price", "quantity", "time",
            "buyer_broker", "seller_broker", "session_type",
            "update_action", "trade_id", "reference_date", "channel_type",
        }
        assert expected_columns.issubset(set(df.columns))

        # Type normalisation checks
        assert df["price"].dtype == "float64"
        assert df["quantity"].dtype == "uint32"
        assert df["time"].str.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}$").all()

    def test_skips_when_output_exists_and_overwrite_false(
        self, patched_paths, sample_zip_trade_date
    ):
        place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)
        output_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        output_file.write_bytes(b"fake existing parquet")

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, overwrite=False
        )

        assert status is StageStatus.SKIPPED
        # untouched — still the fake placeholder content
        assert output_file.read_bytes() == b"fake existing parquet"

    def test_missing_zip_raises_file_not_found(self, patched_paths, sample_zip_trade_date):
        with pytest.raises(FileNotFoundError):
            _extractor.extract_to_raw_parquet(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )

    def test_corrupt_zip_raises_value_error(self, patched_paths, sample_zip_trade_date):
        cfg = FeedType.RV.config
        filename = cfg.zip_name_template.format(date=sample_zip_trade_date)
        bad_zip = patched_paths["rv_downloads"] / filename
        bad_zip.write_bytes(b"this is not a real zip file")

        with pytest.raises(ValueError):
            _extractor.extract_to_raw_parquet(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )

    def test_missing_txt_prefix_raises_key_error(self, patched_paths, sample_zip_trade_date):
        cfg = FeedType.RV.config
        filename = cfg.zip_name_template.format(date=sample_zip_trade_date)
        zip_path = patched_paths["rv_downloads"] / filename

        # A well-formed ZIP, but with a TXT name that doesn't match the
        # expected prefix for this date.
        with zipfile.ZipFile(zip_path, "w") as archive:
            archive.writestr("wrong_name.txt", "irrelevant content")

        with pytest.raises(KeyError):
            _extractor.extract_to_raw_parquet(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )


class TestExtractToRawParquetDeriv:
    def test_successful_extraction(self, patched_paths, sample_zip_trade_date):
        place_sample_zip(
            patched_paths, FeedType.DERIV, "sample_deriv.zip", sample_zip_trade_date
        )

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.DERIV
        )

        assert status is StageStatus.SUCCESS

        output_file = patched_paths["deriv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        df = pd.read_parquet(output_file)
        assert len(df) == 20
        assert df["price"].dtype == "float64"