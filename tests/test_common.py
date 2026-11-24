# tests/test_common.py

"""Unit tests for common.py helpers not covered by the subpackage suites."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from b3_data_collector import common
from b3_data_collector.common import require_s3_bucket


class TestRequireS3Bucket:
    def test_returns_configured_bucket(self):
        assert require_s3_bucket("ibexquant-b3-raw") == "ibexquant-b3-raw"

    def test_empty_bucket_raises_value_error(self):
        with pytest.raises(ValueError, match=r"\.env"):
            require_s3_bucket("")


class TestDownloadFileFromS3:
    @pytest.fixture
    def bucket(self):
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    def test_writes_object_to_file(self, bucket, tmp_path):
        bucket.put_object(Bucket="test-bucket", Key="a/b.zip", Body=b"content")
        destination = tmp_path / "nested" / "b.zip"

        common.download_file_from_s3("test-bucket", "a/b.zip", destination)

        assert destination.read_bytes() == b"content"

    def test_missing_key_raises_file_not_found(self, bucket, tmp_path):
        with pytest.raises(FileNotFoundError, match=r"a/missing\.zip"):
            common.download_file_from_s3("test-bucket", "a/missing.zip", tmp_path / "x.zip")
