# tests/bdi/test_uploader.py

"""Unit tests for bdi/_uploader.py — S3 mocked via moto, no real AWS calls."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from b3_data_collector.bdi import _uploader
from b3_data_collector.common import StageStatus
from b3_data_collector.config import Settings


@pytest.fixture
def fake_settings() -> Settings:
    """
    A Settings instance with explicit test values, bypassing os.getenv
    entirely — the frozen dataclass constructor accepts explicit kwargs
    even though instances can't be mutated after creation.
    """
    return Settings(
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
        AWS_S3_REGION="us-east-1",
        AWS_S3_BUCKET_B3="test-bucket",
    )


@pytest.fixture
def s3_bucket(fake_settings, monkeypatch):
    """
    Point _uploader's `settings` at fake_settings, start moto's S3 mock,
    and pre-create the test bucket. Yields the boto3 client so tests can
    inspect what actually landed in the mock bucket.
    """
    monkeypatch.setattr(_uploader, "settings", fake_settings)
    with mock_aws():
        client = boto3.client("s3", region_name=fake_settings.AWS_S3_REGION)
        client.create_bucket(Bucket=fake_settings.AWS_S3_BUCKET_B3)
        yield client


class TestBuildS3Key:
    def test_key_format(self, sample_trade_date):
        key = _uploader._build_s3_key("renda_variavel", "DailyAverageStocks", sample_trade_date)
        assert key == (
            "b3/bdi/reports/renda_variavel/DailyAverageStocks/"
            f"year={sample_trade_date:%Y}/{sample_trade_date}.csv"
        )


class TestUploadCsv:
    def test_successful_upload(self, s3_bucket, sample_trade_date, sample_csv_bytes):
        status = _uploader.upload_csv(
            content=sample_csv_bytes,
            section="renda_variavel",
            api_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            overwrite=False,
        )

        assert status is StageStatus.SUCCESS

        key = _uploader._build_s3_key("renda_variavel", "DailyAverageStocks", sample_trade_date)
        obj = s3_bucket.get_object(Bucket="test-bucket", Key=key)
        assert obj["Body"].read() == sample_csv_bytes

    def test_skips_when_object_exists_and_overwrite_false(
        self, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        key = _uploader._build_s3_key("renda_variavel", "DailyAverageStocks", sample_trade_date)
        s3_bucket.put_object(Bucket="test-bucket", Key=key, Body=b"already there")

        status = _uploader.upload_csv(
            content=sample_csv_bytes,
            section="renda_variavel",
            api_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            overwrite=False,
        )

        assert status is StageStatus.SKIPPED
        # original content must remain untouched
        obj = s3_bucket.get_object(Bucket="test-bucket", Key=key)
        assert obj["Body"].read() == b"already there"

    def test_overwrites_when_overwrite_true(
        self, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        key = _uploader._build_s3_key("renda_variavel", "DailyAverageStocks", sample_trade_date)
        s3_bucket.put_object(Bucket="test-bucket", Key=key, Body=b"old content")

        status = _uploader.upload_csv(
            content=sample_csv_bytes,
            section="renda_variavel",
            api_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            overwrite=True,
        )

        assert status is StageStatus.SUCCESS
        obj = s3_bucket.get_object(Bucket="test-bucket", Key=key)
        assert obj["Body"].read() == sample_csv_bytes

    def test_upload_failure_returns_failed_status(
        self, fake_settings, monkeypatch, sample_trade_date, sample_csv_bytes
    ):
        """No bucket created — upload should fail gracefully, not raise."""
        monkeypatch.setattr(_uploader, "settings", fake_settings)
        with mock_aws():
            status = _uploader.upload_csv(
                content=sample_csv_bytes,
                section="renda_variavel",
                api_name="DailyAverageStocks",
                trade_date=sample_trade_date,
                overwrite=True,
            )
            assert status is StageStatus.FAILED