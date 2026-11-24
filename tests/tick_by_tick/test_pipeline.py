# tests/tick_by_tick/test_pipeline.py

"""Unit tests for tick_by_tick/pipeline.py — date resolution logic (no I/O)."""

from __future__ import annotations

from datetime import date

import boto3
import pytest
from botocore.exceptions import ClientError
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
from b3_data_collector.tick_by_tick import _downloader, _partitioner, pipeline
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

    ``settings`` is patched on ``_downloader`` (ZIP archive upload),
    ``_partitioner`` (processed ticks Parquet upload) and ``pipeline``
    (bucket check at start) — each module holds its own module-level
    reference to the settings object, so all must point at the same fake
    bucket for the full chain to land in the same mocked S3.
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
        monkeypatch.setattr(pipeline, "settings", fake_settings)
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

class TestS3FailureHandling:
    """
    ``run_pipeline`` with every stage stubbed — isolates how S3 upload
    statuses become date-level errors, without network, disk or S3.
    """

    @pytest.fixture
    def stub_stages(self, monkeypatch):
        monkeypatch.setattr(
            pipeline, "settings", Settings(AWS_S3_BUCKET_B3="test-bucket")
        )
        # Stage 0 would otherwise reach the real S3.
        monkeypatch.setattr(pipeline, "_is_archived_in_s3", lambda *_args: False)

        def _stub(
            zip_s3=StageStatus.SUCCESS,
            ticks_s3=StageStatus.SUCCESS,
            extract_error=None,
        ):
            def _extract(**_kwargs):
                if extract_error is not None:
                    raise extract_error
                return StageStatus.SUCCESS

            monkeypatch.setattr(
                pipeline, "download_zip", lambda **_kwargs: (None, StageStatus.SUCCESS, zip_s3)
            )
            monkeypatch.setattr(pipeline, "extract_to_raw_parquet", _extract)
            monkeypatch.setattr(
                pipeline,
                "partition_to_ticks",
                lambda **_kwargs: (StageStatus.SUCCESS, ticks_s3, 20),
            )

        return _stub

    def _run_one(self):
        result = run_pipeline(dates=date(2026, 11, 17), feed=FeedType.DERIV)
        return result, result.results[0]

    def test_all_uploads_succeed(self, stub_stages):
        stub_stages()

        result, date_result = self._run_one()

        assert result.failed == 0
        assert date_result.error is None

    def test_zip_upload_failure_marks_date_failed_but_runs_later_stages(self, stub_stages):
        stub_stages(zip_s3=StageStatus.FAILED)

        result, date_result = self._run_one()

        assert result.failed == 1
        assert date_result.error == "S3 upload failed: ZIP archive"
        assert date_result.extract is StageStatus.SUCCESS
        assert date_result.partition is StageStatus.SUCCESS

    def test_ticks_upload_failure_marks_date_failed(self, stub_stages):
        stub_stages(ticks_s3=StageStatus.FAILED)

        result, date_result = self._run_one()

        assert result.failed == 1
        assert date_result.error == "S3 upload failed: ticks Parquet"

    def test_both_upload_failures_are_reported(self, stub_stages):
        stub_stages(zip_s3=StageStatus.FAILED, ticks_s3=StageStatus.FAILED)

        _, date_result = self._run_one()

        assert date_result.error == (
            "S3 upload failed: ZIP archive; S3 upload failed: ticks Parquet"
        )

    def test_later_stage_error_keeps_earlier_upload_error(self, stub_stages):
        stub_stages(
            zip_s3=StageStatus.FAILED,
            extract_error=FileNotFoundError("ZIP missing"),
        )

        _, date_result = self._run_one()

        assert date_result.error == "S3 upload failed: ZIP archive; ZIP missing"


class TestBucketGuard:
    def test_empty_bucket_raises_before_any_stage(self, monkeypatch):
        monkeypatch.setattr(pipeline, "settings", Settings(AWS_S3_BUCKET_B3=""))

        def _fail_if_called(**_kwargs):
            raise AssertionError("download_zip must not run without a bucket")

        monkeypatch.setattr(pipeline, "download_zip", _fail_if_called)

        with pytest.raises(ValueError, match="AWS_S3_BUCKET_B3"):
            run_pipeline(dates=date(2026, 11, 17), upload_to_s3=True)

    def test_empty_bucket_allowed_when_upload_disabled(self, monkeypatch):
        monkeypatch.setattr(pipeline, "settings", Settings(AWS_S3_BUCKET_B3=""))
        monkeypatch.setattr(
            pipeline,
            "download_zip",
            lambda **_kwargs: (None, StageStatus.UNAVAILABLE, StageStatus.SKIPPED),
        )

        result = run_pipeline(dates=date(2026, 11, 17), upload_to_s3=False)

        assert result.total == 1


class TestSkipArchivedDates:
    """
    Stage 0: a date whose ZIP and ticks are both in S3 is skipped before
    any request to B3 — unless ``overwrite=True`` or ``upload_to_s3=False``.

    B3 answers 404 in every test, so "the stages ran" shows up as exactly
    one download request, without needing a real ZIP.
    """

    @pytest.fixture
    def s3_bucket(self, monkeypatch):
        fake_settings = Settings(AWS_S3_REGION="us-east-1", AWS_S3_BUCKET_B3="test-bucket")
        for module in (_downloader, _partitioner, pipeline):
            monkeypatch.setattr(module, "settings", fake_settings)
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    @pytest.fixture
    def b3_404(self, requests_mock, sample_zip_trade_date):
        for feed in FeedType:
            url = feed.config.url_template.format(date=f"{sample_zip_trade_date:%Y-%m-%d}")
            requests_mock.get(url, status_code=404)
        return requests_mock

    @staticmethod
    def _archive(client, feed, trade_date, *, zip_file=True, ticks=True):
        cfg = feed.config
        if zip_file:
            key = _downloader._build_s3_key_zip(cfg, trade_date)
            client.put_object(Bucket="test-bucket", Key=key, Body=b"zip")
        if ticks:
            key = _partitioner._build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date)
            client.put_object(Bucket="test-bucket", Key=key, Body=b"ticks")

    @pytest.mark.parametrize("feed", list(FeedType))
    def test_archived_date_makes_no_request_to_b3(
        self, feed, b3_404, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        self._archive(s3_bucket, feed, sample_zip_trade_date)

        result = run_pipeline(dates=sample_zip_trade_date, feed=feed)

        assert b3_404.call_count == 0
        assert result.failed == 0
        date_result = result.results[0]
        assert date_result.download is StageStatus.SKIPPED
        assert date_result.partition is StageStatus.SKIPPED
        assert not any(patched_paths[feed.config.paths_key_downloads].iterdir())

    def test_missing_zip_still_runs_the_stages(
        self, b3_404, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        self._archive(s3_bucket, FeedType.RV, sample_zip_trade_date, zip_file=False)

        run_pipeline(dates=sample_zip_trade_date, feed=FeedType.RV)

        assert b3_404.call_count == 1

    def test_missing_ticks_still_runs_the_stages(
        self, b3_404, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        self._archive(s3_bucket, FeedType.RV, sample_zip_trade_date, ticks=False)

        run_pipeline(dates=sample_zip_trade_date, feed=FeedType.RV)

        assert b3_404.call_count == 1

    def test_overwrite_runs_even_when_archived(
        self, b3_404, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        self._archive(s3_bucket, FeedType.RV, sample_zip_trade_date)

        run_pipeline(dates=sample_zip_trade_date, feed=FeedType.RV, overwrite=True)

        assert b3_404.call_count == 1

    def test_local_only_run_ignores_s3(
        self, b3_404, patched_paths, s3_bucket, sample_zip_trade_date
    ):
        self._archive(s3_bucket, FeedType.RV, sample_zip_trade_date)

        run_pipeline(dates=sample_zip_trade_date, feed=FeedType.RV, upload_to_s3=False)

        assert b3_404.call_count == 1

    def test_s3_check_error_runs_the_stages(
        self, b3_404, patched_paths, s3_bucket, sample_zip_trade_date, monkeypatch
    ):
        def _raise(*_args):
            raise ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject")

        monkeypatch.setattr(pipeline, "_is_archived_in_s3", _raise)

        run_pipeline(dates=sample_zip_trade_date, feed=FeedType.RV)

        assert b3_404.call_count == 1
