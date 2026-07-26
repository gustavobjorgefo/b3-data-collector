# tests/reader/test_client.py

"""
Unit tests for reader/_client.py — S3 fetch helpers, mocked via moto.

Confirms the reader builds the exact same keys used at upload time by
bdi/_uploader.py and tick_by_tick/_partitioner.py, and surfaces missing
objects as FileNotFoundError rather than a raw botocore exception.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from b3_data_collector.bdi._catalog import CATALOG_BY_NAME
from b3_data_collector.bdi._uploader import _build_s3_key
from b3_data_collector.config import Settings
from b3_data_collector.reader import _client
from b3_data_collector.tick_by_tick._feed import FeedType
from b3_data_collector.tick_by_tick._partitioner import _build_s3_key_ticks


@pytest.fixture
def fake_settings() -> Settings:
    return Settings(
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
        AWS_S3_REGION="us-east-1",
        AWS_S3_BUCKET_B3="test-bucket",
    )


@pytest.fixture
def s3_bucket(fake_settings, monkeypatch):
    monkeypatch.setattr(_client, "settings", fake_settings)
    with mock_aws():
        client = boto3.client("s3", region_name=fake_settings.AWS_S3_REGION)
        client.create_bucket(Bucket=fake_settings.AWS_S3_BUCKET_B3)
        yield client


class TestFetchBdiReportBytes:
    def test_successful_fetch(self, s3_bucket, sample_trade_date, sample_csv_bytes):
        report = CATALOG_BY_NAME["BTBLoanBalance"]
        key = _build_s3_key(report.section, "BTBLoanBalance", sample_trade_date)
        s3_bucket.put_object(Bucket="test-bucket", Key=key, Body=sample_csv_bytes)

        content = _client.fetch_bdi_report_bytes(
            api_name="BTBLoanBalance", trade_date=sample_trade_date
        )

        assert content == sample_csv_bytes

    def test_unknown_report_raises_key_error(self, s3_bucket, sample_trade_date):
        with pytest.raises(KeyError, match="Unknown BDI report"):
            _client.fetch_bdi_report_bytes(
                api_name="NotARealReport", trade_date=sample_trade_date
            )

    def test_missing_object_raises_file_not_found(self, s3_bucket, sample_trade_date):
        with pytest.raises(FileNotFoundError):
            _client.fetch_bdi_report_bytes(
                api_name="BTBLoanBalance", trade_date=sample_trade_date
            )


class TestFetchTickByTickBytes:
    def test_successful_fetch(self, s3_bucket, sample_trade_date):
        cfg = FeedType.RV.config
        key = _build_s3_key_ticks(cfg.s3_prefix_ticks, sample_trade_date)
        s3_bucket.put_object(Bucket="test-bucket", Key=key, Body=b"fake parquet bytes")

        content = _client.fetch_tick_by_tick_bytes(
            feed=FeedType.RV, trade_date=sample_trade_date
        )

        assert content == b"fake parquet bytes"

    def test_missing_object_raises_file_not_found(self, s3_bucket, sample_trade_date):
        with pytest.raises(FileNotFoundError):
            _client.fetch_tick_by_tick_bytes(feed=FeedType.RV, trade_date=sample_trade_date)