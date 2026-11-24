# tests/test_common.py

"""Unit tests for common.py helpers not covered by the subpackage suites."""

from __future__ import annotations

import pytest

from b3_data_collector.common import require_s3_bucket


class TestRequireS3Bucket:
    def test_returns_configured_bucket(self):
        assert require_s3_bucket("ibexquant-b3-raw") == "ibexquant-b3-raw"

    def test_empty_bucket_raises_value_error(self):
        with pytest.raises(ValueError, match=r"\.env"):
            require_s3_bucket("")
