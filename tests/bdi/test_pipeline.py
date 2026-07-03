# tests/bdi/test_pipeline.py

"""Unit tests for bdi/pipeline.py — date resolution logic (no I/O)."""

from __future__ import annotations

from datetime import date

import pytest

import boto3
from moto import mock_aws

from b3_data_collector.bdi import _uploader
from b3_data_collector.bdi._models import ReportStatus
from b3_data_collector.bdi.pipeline import run_bdi_pipeline
from b3_data_collector.config import Settings


from b3_data_collector.bdi.pipeline import (
    _business_days_in_range,
    _parse_date,
    _resolve_dates,
)


class TestParseDate:
    def test_parses_string(self):
        assert _parse_date("2026-06-26") == date(2026, 6, 26)

    def test_passes_through_date_object(self):
        d = date(2026, 6, 26)
        assert _parse_date(d) is d


class TestBusinessDaysInRange:
    def test_excludes_weekend(self):
        # Fri 2026-06-26 -> Mon 2026-06-29 (spans Sat/Sun)
        result = _business_days_in_range(date(2026, 6, 26), date(2026, 6, 29))
        assert result == [
            date(2026, 6, 26),  # Fri
            date(2026, 6, 29),  # Mon
        ]

    def test_single_business_day(self):
        result = _business_days_in_range(date(2026, 6, 26), date(2026, 6, 26))
        assert result == [date(2026, 6, 26)]

    def test_full_business_week(self):
        result = _business_days_in_range(date(2026, 6, 22), date(2026, 6, 26))
        assert len(result) == 5


class TestResolveDates:
    def test_single_string_date(self):
        assert _resolve_dates("2026-06-26") == [date(2026, 6, 26)]

    def test_single_date_object(self):
        d = date(2026, 6, 26)
        assert _resolve_dates(d) == [d]

    def test_explicit_list_deduplicates_and_sorts(self):
        result = _resolve_dates(
            ["2026-06-27", "2026-06-26", "2026-06-26"]
        )
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


class TestRunBdiPipelineIntegration:
    """
    End-to-end test of run_bdi_pipeline: real orchestration logic,
    but network and S3 fully mocked. Confirms fetch -> upload wiring
    works correctly across the whole enabled report catalog.
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
        monkeypatch.setattr(_uploader, "settings", fake_settings)
        with mock_aws():
            client = boto3.client("s3", region_name=fake_settings.AWS_S3_REGION)
            client.create_bucket(Bucket=fake_settings.AWS_S3_BUCKET_B3)
            yield client

    def test_full_pipeline_success_for_all_reports(
        self, requests_mock, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        # Every BDI export call, regardless of report name, returns the
        # same sample content — we're testing orchestration, not per-report
        # content correctness (that's covered in test_client.py).
        requests_mock.post(
            "https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR",
            content=sample_csv_bytes,
        )

        result = run_bdi_pipeline(dates=sample_trade_date)

        assert result.failed == 0
        assert result.total > 0
        assert result.succeeded == result.total

        # Spot-check: every result actually succeeded end-to-end
        for report_result in result.results:
            assert report_result.status is ReportStatus.SUCCESS

    def test_pipeline_marks_unavailable_reports_without_failing(
        self, requests_mock, s3_bucket, sample_trade_date
    ):
        requests_mock.post(
            "https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR",
            status_code=404,
        )

        result = run_bdi_pipeline(dates=sample_trade_date)

        assert result.failed == 0
        assert result.unavailable == result.total

    def test_pipeline_continues_after_individual_report_failure(
        self, requests_mock, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        # Every request fails with a 500 — pipeline should record FAILED
        # for each report but still process the entire catalog, not stop
        # at the first failure.
        requests_mock.post(
            "https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR",
            status_code=500,
        )

        result = run_bdi_pipeline(dates=sample_trade_date)

        assert result.failed == result.total
        assert result.succeeded == 0