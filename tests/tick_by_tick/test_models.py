# tests/tick_by_tick/test_models.py

"""Unit tests for tick_by_tick/_models.py — pure dataclasses, no I/O."""

from __future__ import annotations

from b3_data_collector.common import StageStatus
from b3_data_collector.tick_by_tick._models import DateResult, PipelineResult


class TestDateResult:
    def test_succeeded_true_when_no_error(self, sample_trade_date):
        result = DateResult(trade_date=sample_trade_date)
        assert result.succeeded is True

    def test_succeeded_false_when_error_set(self, sample_trade_date):
        result = DateResult(trade_date=sample_trade_date, error="download failed")
        assert result.succeeded is False

    def test_summary_line_shows_tick_count_when_present(self, sample_trade_date):
        result = DateResult(trade_date=sample_trade_date, tick_count=12345)
        assert "12,345" in result.summary_line

    def test_summary_line_shows_dash_when_tick_count_missing(self, sample_trade_date):
        result = DateResult(trade_date=sample_trade_date)
        assert "—" in result.summary_line


class TestPipelineResult:
    def test_counters_aggregate_correctly(self, sample_trade_date):
        results = [
            DateResult(trade_date=sample_trade_date),
            DateResult(trade_date=sample_trade_date, error="failed"),
            DateResult(
                trade_date=sample_trade_date,
                download=StageStatus.UNAVAILABLE,
            ),
        ]
        pipeline_result = PipelineResult(results=results)

        assert pipeline_result.total == 3
        assert pipeline_result.succeeded == 2
        assert pipeline_result.failed == 1
        assert pipeline_result.unavailable == 1

    def test_failed_dates_returns_only_failed(self, sample_trade_date):
        results = [
            DateResult(trade_date=sample_trade_date),
            DateResult(trade_date=sample_trade_date, error="failed"),
        ]
        pipeline_result = PipelineResult(results=results)

        assert len(pipeline_result.failed_dates) == 1