# tests/bdi/test_pipeline.py

"""Unit tests for bdi/pipeline.py — date resolution logic (no I/O)."""

from __future__ import annotations

from datetime import date

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from b3_data_collector.bdi import _uploader, pipeline
from b3_data_collector.bdi._catalog import ENABLED_REPORTS
from b3_data_collector.bdi._models import ReportStatus
from b3_data_collector.bdi.pipeline import run_bdi_pipeline
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
        monkeypatch.setattr(pipeline, "settings", fake_settings)
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


class TestSkipArchivedReports:
    """
    Step 0 of the BDI pipeline: reports already in S3 are skipped before
    any request to B3, unless ``overwrite=True``.
    """

    _EXPORT_URL = "https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR"

    @pytest.fixture
    def s3_bucket(self, monkeypatch):
        fake_settings = Settings(AWS_S3_REGION="us-east-1", AWS_S3_BUCKET_B3="test-bucket")
        monkeypatch.setattr(_uploader, "settings", fake_settings)
        monkeypatch.setattr(pipeline, "settings", fake_settings)
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    @staticmethod
    def _archive(client, reports, trade_date, content):
        for report in reports:
            key = _uploader._build_s3_key(report.section, report.api_name, trade_date)
            client.put_object(Bucket="test-bucket", Key=key, Body=content)

    def test_archived_date_makes_no_request_to_b3(
        self, requests_mock, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        self._archive(s3_bucket, ENABLED_REPORTS, sample_trade_date, sample_csv_bytes)
        requests_mock.post(self._EXPORT_URL, content=sample_csv_bytes)

        result = run_bdi_pipeline(dates=sample_trade_date)

        assert requests_mock.call_count == 0
        assert result.failed == 0
        assert all(r.status is ReportStatus.SKIPPED for r in result.results)

    def test_fetches_only_missing_reports(
        self, requests_mock, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        missing, *archived = ENABLED_REPORTS
        self._archive(s3_bucket, archived, sample_trade_date, sample_csv_bytes)
        requests_mock.post(self._EXPORT_URL, content=sample_csv_bytes)

        result = run_bdi_pipeline(dates=sample_trade_date)

        assert requests_mock.call_count == 1
        statuses = {r.report_name: r.status for r in result.results}
        assert statuses[missing.api_name] is ReportStatus.SUCCESS

    def test_overwrite_fetches_even_when_archived(
        self, requests_mock, s3_bucket, sample_trade_date, sample_csv_bytes
    ):
        self._archive(s3_bucket, ENABLED_REPORTS, sample_trade_date, sample_csv_bytes)
        requests_mock.post(self._EXPORT_URL, content=sample_csv_bytes)

        run_bdi_pipeline(dates=sample_trade_date, overwrite=True)

        assert requests_mock.call_count == len(ENABLED_REPORTS)

    def test_s3_check_error_falls_back_to_fetch(
        self, requests_mock, s3_bucket, sample_trade_date, sample_csv_bytes, monkeypatch
    ):
        def _raise(**_kwargs):
            raise ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject")

        monkeypatch.setattr(pipeline, "csv_exists_in_s3", _raise)
        requests_mock.post(self._EXPORT_URL, content=sample_csv_bytes)

        result = run_bdi_pipeline(dates=sample_trade_date)

        assert requests_mock.call_count == len(ENABLED_REPORTS)
        assert result.failed == 0

class TestBucketGuard:
    def test_empty_bucket_raises_before_any_request(self, requests_mock, monkeypatch):
        monkeypatch.setattr(pipeline, "settings", Settings(AWS_S3_BUCKET_B3=""))

        with pytest.raises(ValueError, match="AWS_S3_BUCKET_B3"):
            run_bdi_pipeline(dates=date(2026, 11, 17))

        assert requests_mock.call_count == 0
