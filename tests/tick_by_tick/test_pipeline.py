# tests/tick_by_tick/test_pipeline.py

"""Unit tests for tick_by_tick/pipeline.py — date resolution logic (no I/O)."""

from __future__ import annotations

from datetime import date

import boto3
import pytest
from moto import mock_aws

from b3_data_collector.common import StageStatus
from b3_data_collector.common import (
    business_days_in_range as _business_days_in_range,
)
from b3_data_collector.common import (
    parse_date as _parse_date,
)
from b3_data_collector.common import (
    resolve_dates as _resolve_dates,
)
from b3_data_collector.config import Settings
from b3_data_collector.tick_by_tick import _downloader, _partitioner
from b3_data_collector.tick_by_tick._feed import FeedType
from b3_data_collector.tick_by_tick.pipeline import run_pipeline


class TestParseDate:
    def test_parses_string(self):
        assert _parse_date("2026-06-26") == date(2026, 6, 26)

    def test_passes_through_date_object(self):
        d = date(2026, 6, 26)
        assert _parse_date(d) is d


class TestBusinessDaysInRange:
    def test_excludes_weekend(self):
        result = _business_days_in_range(date(2026, 6, 26), date(2026, 6, 29))
        assert result == [date(2026, 6, 26), date(2026, 6, 29)]

    def test_single_business_day(self):
        result = _business_days_in_range(date(2026, 6, 26), date(2026, 6, 26))
        assert result == [date(2026, 6, 26)]

    def test_full_business_week(self):
        result = _business_days_in_range(date(2026, 6, 22), date(2026, 6, 26))
        assert len(result) == 5


class TestResolveDates:
    def test_single_string_date(self):
        assert _resolve_dates("2026-06-26") == [date(2026, 6, 26)]

    def test_explicit_list_deduplicates_and_sorts(self):
        result = _resolve_dates(["2026-06-27", "2026-06-26", "2026-06-26"])
        assert result == [date(2026, 6, 26), date(2026, 6, 27)]

    def test_tuple_range_expands_to_business_days(self):
        result = _resolve_dates(("2026-06-26", "2026-06-29"))
        assert result == [date(2026, 6, 26), date(2026, 6, 29)]

    def test_tuple_with_wrong_length_raises_value_error(self):
        with pytest.raises(ValueError):
            _resolve_dates(("2026-06-26", "2026-06-27", "2026-06-28"))

    def test_invalid_type_raises_type_error(self):
        with pytest.raises(TypeError):
            _resolve_dates(12345)


class TestRunPipelineIntegration:
    """
    End-to-end test of run_pipeline: download -> extract -> partition,
    chained for real, with network and S3 mocked and filesystem isolated
    to tmp_path. Uses the real sample ZIP fixture as the download response,
    so the full chain runs on genuine B3-format data.

    ``settings`` is patched on both ``_downloader`` (ZIP archive upload)
    and ``_partitioner`` (processed ticks Parquet upload) — each module
    holds its own module-level reference to the settings object, so both
    must point at the same fake bucket for the full chain to land in the
    same mocked S3.
    """

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
        monkeypatch.setattr(_downloader, "settings", fake_settings)
        monkeypatch.setattr(_partitioner, "settings", fake_settings)
        with mock_aws():
            client = boto3.client("s3", region_name=fake_settings.AWS_S3_REGION)
            client.create_bucket(Bucket=fake_settings.AWS_S3_BUCKET_B3)
            yield client

    def test_full_pipeline_rv_success(
        self, requests_mock, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        # The download stage expects a ZIP response — we feed it the real
        # sample fixture's bytes directly from disk.
        from tests.conftest import FIXTURES_DIR

        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_zip_trade_date:%Y-%m-%d}")
        zip_bytes = (FIXTURES_DIR / "sample_rv.zip").read_bytes()
        requests_mock.get(
            url, content=zip_bytes, headers={"Content-Type": "application/zip"}
        )

        # download_zip() names the local file using zip_name_template, which
        # doesn't match sample_rv.zip's real internal TXT name unless we
        # also make sure the TXT prefix matches — since sample_rv.zip's
        # internal TXT is "30-06-2026_NEGOCIOSAVISTA_RV.txt" and RV's
        # txt_prefix_template resolves to the same string for this date,
        # this works without renaming anything.
        result = run_pipeline(
            dates=sample_zip_trade_date,
            feed=FeedType.RV,
            upload_to_s3=True,
        )

        assert result.failed == 0
        assert result.succeeded == 1

        date_result = result.results[0]
        assert date_result.download is StageStatus.SUCCESS
        assert date_result.extract is StageStatus.SUCCESS
        assert date_result.partition is StageStatus.SUCCESS
        assert date_result.s3_upload is StageStatus.SUCCESS
        assert date_result.ticks_s3_upload is StageStatus.SUCCESS
        assert date_result.tick_count == 20

        # Confirm the final tick-level Parquet actually exists locally...
        import pandas as pd

        ticks_file = patched_paths["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        df = pd.read_parquet(ticks_file)
        assert len(df) == 20
        assert "timestamp" in df.columns

        # ...and landed in S3 under the expected Hive-partitioned key.
        import io

        key = _partitioner._build_s3_key_ticks(cfg.s3_prefix_ticks, sample_zip_trade_date)
        obj = s3_bucket.get_object(Bucket="test-bucket", Key=key)
        df_s3 = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        assert len(df_s3) == 20

    def test_pipeline_stops_gracefully_on_download_404(
        self, requests_mock, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_zip_trade_date:%Y-%m-%d}")
        requests_mock.get(url, status_code=404)

        result = run_pipeline(
            dates=sample_zip_trade_date,
            feed=FeedType.RV,
            upload_to_s3=True,
        )

        assert result.failed == 1
        date_result = result.results[0]
        assert date_result.download is StageStatus.UNAVAILABLE
        # extract/partition never ran — stages default to SKIPPED
        assert date_result.extract is StageStatus.SKIPPED
        assert date_result.partition is StageStatus.SKIPPED
        assert date_result.ticks_s3_upload is StageStatus.SKIPPED