# tests/bdi/test_models.py

"""Unit tests for bdi/_models.py — pure dataclasses and enum, no I/O."""

from __future__ import annotations

from b3_data_collector.bdi._models import (
    BdiPipelineResult,
    ReportResult,
    ReportStatus,
)


class TestReportResult:
    def test_succeeded_true_for_non_failed_status(self, sample_trade_date):
        result = ReportResult(
            report_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            status=ReportStatus.SUCCESS,
        )
        assert result.succeeded is True

    def test_succeeded_false_for_failed_status(self, sample_trade_date):
        result = ReportResult(
            report_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            status=ReportStatus.FAILED,
            error="boom",
        )
        assert result.succeeded is False

    def test_summary_line_includes_error_when_present(self, sample_trade_date):
        result = ReportResult(
            report_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            status=ReportStatus.FAILED,
            error="network timeout",
        )
        assert "FAILED" in result.summary_line
        assert "network timeout" in result.summary_line

    def test_summary_line_omits_error_when_absent(self, sample_trade_date):
        result = ReportResult(
            report_name="DailyAverageStocks",
            trade_date=sample_trade_date,
            status=ReportStatus.SUCCESS,
        )
        assert "—" not in result.summary_line


class TestBdiPipelineResult:
    def test_counters_aggregate_correctly(self, sample_trade_date):
        results = [
            ReportResult("ReportA", sample_trade_date, ReportStatus.SUCCESS),
            ReportResult("ReportB", sample_trade_date, ReportStatus.SUCCESS),
            ReportResult("ReportC", sample_trade_date, ReportStatus.SKIPPED),
            ReportResult("ReportD", sample_trade_date, ReportStatus.UNAVAILABLE),
            ReportResult("ReportE", sample_trade_date, ReportStatus.FAILED, "err"),
        ]
        pipeline_result = BdiPipelineResult(results=results)

        assert pipeline_result.total == 5
        assert pipeline_result.succeeded == 2
        assert pipeline_result.skipped == 1
        assert pipeline_result.unavailable == 1
        assert pipeline_result.failed == 1

    def test_failed_results_returns_only_failed(self, sample_trade_date):
        results = [
            ReportResult("ReportA", sample_trade_date, ReportStatus.SUCCESS),
            ReportResult("ReportB", sample_trade_date, ReportStatus.FAILED, "err"),
        ]
        pipeline_result = BdiPipelineResult(results=results)

        assert len(pipeline_result.failed_results) == 1
        assert pipeline_result.failed_results[0].report_name == "ReportB"

    def test_summary_contains_failure_section_when_failures_exist(
        self, sample_trade_date
    ):
        results = [ReportResult("ReportA", sample_trade_date, ReportStatus.FAILED, "err")]
        pipeline_result = BdiPipelineResult(results=results)

        assert "FAILURES:" in pipeline_result.summary

    def test_summary_omits_failure_section_when_no_failures(self, sample_trade_date):
        results = [ReportResult("ReportA", sample_trade_date, ReportStatus.SUCCESS)]
        pipeline_result = BdiPipelineResult(results=results)

        assert "FAILURES:" not in pipeline_result.summary