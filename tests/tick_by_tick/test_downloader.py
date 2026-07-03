# tests/tick_by_tick/test_downloader.py

"""Unit tests for tick_by_tick/_downloader.py — network mocked, filesystem isolated."""

from __future__ import annotations

import pytest
import requests

import boto3
from moto import mock_aws

from b3_data_collector.common import StageStatus
from b3_data_collector.tick_by_tick import _downloader
from b3_data_collector.tick_by_tick._feed import FeedType
from b3_data_collector.config import Settings


@pytest.fixture
def rv_downloads_dir(tmp_path, monkeypatch):
    """
    Redirect the RV downloads path to an isolated temp directory for the
    duration of one test. monkeypatch.setitem restores the original entry
    automatically afterwards — the real data/ folder is never touched.
    """
    monkeypatch.setitem(_downloader.PATHS_B3, "rv_downloads", tmp_path)
    return tmp_path


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
    monkeypatch.setattr(_downloader, "settings", fake_settings)
    with mock_aws():
        client = boto3.client("s3", region_name=fake_settings.AWS_S3_REGION)
        client.create_bucket(Bucket=fake_settings.AWS_S3_BUCKET_B3)
        yield client


class TestDownloadZip:
    def test_successful_download_without_s3(
        self, requests_mock, rv_downloads_dir, sample_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_trade_date:%Y-%m-%d}")
        requests_mock.get(
            url,
            content=b"fake zip bytes",
            headers={"Content-Type": "application/zip"},
        )

        path, dl_status, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=False,
        )

        assert dl_status is StageStatus.SUCCESS
        assert s3_status is StageStatus.SKIPPED
        assert path is not None and path.exists()
        assert path.read_bytes() == b"fake zip bytes"

    def test_skips_download_when_file_already_exists(
        self, requests_mock, rv_downloads_dir, sample_trade_date
    ):
        cfg = FeedType.RV.config
        filename = cfg.zip_name_template.format(date=sample_trade_date)
        existing_file = rv_downloads_dir / filename
        existing_file.write_bytes(b"already here")

        path, dl_status, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=False,
        )

        assert dl_status is StageStatus.SKIPPED
        assert path == existing_file

    def test_404_returns_unavailable(
        self, requests_mock, rv_downloads_dir, sample_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_trade_date:%Y-%m-%d}")
        requests_mock.get(url, status_code=404)

        path, dl_status, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=False,
        )

        assert dl_status is StageStatus.UNAVAILABLE
        assert path is None

    def test_network_error_returns_failed(
        self, requests_mock, rv_downloads_dir, sample_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_trade_date:%Y-%m-%d}")
        requests_mock.get(url, exc=requests.exceptions.ConnectionError)

        path, dl_status, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=False,
        )

        assert dl_status is StageStatus.FAILED
        assert path is None

    def test_unexpected_content_type_returns_failed(
        self, requests_mock, rv_downloads_dir, sample_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_trade_date:%Y-%m-%d}")
        requests_mock.get(
            url,
            content=b"<html>not a zip</html>",
            headers={"Content-Type": "text/html"},
        )

        path, dl_status, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=False,
        )

        assert dl_status is StageStatus.FAILED
        assert path is None


class TestDownloadZipWithS3Upload:
    def test_download_and_upload_success(
        self, requests_mock, rv_downloads_dir, s3_bucket, sample_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_trade_date:%Y-%m-%d}")
        requests_mock.get(
            url,
            content=b"fake zip bytes",
            headers={"Content-Type": "application/zip"},
        )

        path, dl_status, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=True,
        )

        assert dl_status is StageStatus.SUCCESS
        assert s3_status is StageStatus.SUCCESS

        filename = cfg.zip_name_template.format(date=sample_trade_date)
        s3_key = cfg.s3_prefix + filename
        obj = s3_bucket.get_object(Bucket="test-bucket", Key=s3_key)
        assert obj["Body"].read() == b"fake zip bytes"

    def test_skips_s3_upload_when_object_already_exists(
        self, requests_mock, rv_downloads_dir, s3_bucket, sample_trade_date
    ):
        cfg = FeedType.RV.config
        url = cfg.url_template.format(date=f"{sample_trade_date:%Y-%m-%d}")
        requests_mock.get(
            url,
            content=b"fake zip bytes",
            headers={"Content-Type": "application/zip"},
        )

        filename = cfg.zip_name_template.format(date=sample_trade_date)
        s3_key = cfg.s3_prefix + filename
        s3_bucket.put_object(Bucket="test-bucket", Key=s3_key, Body=b"already uploaded")

        _, _, s3_status = _downloader.download_zip(
            trade_date=sample_trade_date,
            feed=FeedType.RV,
            upload_to_s3=True,
        )

        assert s3_status is StageStatus.SKIPPED