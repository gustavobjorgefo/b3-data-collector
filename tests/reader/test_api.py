# tests/reader/test_api.py

"""
Unit tests for reader/api.py — orchestration across dates, mocked via moto.

Confirms multi-date concatenation, per-date graceful skipping when an
object is missing from S3 (logged, not raised), and the empty-result
case when nothing was found at all.
"""

from __future__ import annotations

import io
from datetime import date

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from b3_data_collector.bdi._catalog import CATALOG_BY_NAME
from b3_data_collector.bdi._uploader import _build_s3_key
from b3_data_collector.config import Settings
from b3_data_collector.reader import _client, api
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


# Minimal BTBLoanBalance-shaped CSV, same layout used in tests/bdi/parsers/test_btb_loan_balance.py
_SAMPLE_BTB_CSV = (
    "Descriptive paragraph line, discarded\n"
    "Glossary link, discarded\n"
    "\n"
    "Merged group labels, discarded\n"
    "Real column headers, discarded\n"
    "{date};12345;BRXYZW00001;Example Company SA;BOVESPA;10;1000;50000,00;"
    "1,50;2,00;2,50;2,92;3,10;3,50\n"
)


def _btb_csv_bytes(trade_date: date) -> bytes:
    return _SAMPLE_BTB_CSV.format(date=trade_date.strftime("%d/%m/%Y")).encode("utf-8-sig")


class TestReadBdiReport:
    def test_single_date(self, s3_bucket, sample_trade_date):
        report = CATALOG_BY_NAME["BTBLoanBalance"]
        key = _build_s3_key(report.section, "BTBLoanBalance", sample_trade_date)
        s3_bucket.put_object(
            Bucket="test-bucket", Key=key, Body=_btb_csv_bytes(sample_trade_date)
        )

        df = api.read_bdi_report("BTBLoanBalance", dates=sample_trade_date)

        assert len(df) == 1
        assert df["trade_date"].iloc[0] == pd.Timestamp(sample_trade_date)

    def test_concatenates_multiple_dates(self, s3_bucket):
        report = CATALOG_BY_NAME["BTBLoanBalance"]
        dates = [date(2026, 6, 29), date(2026, 6, 30)]
        for trade_date in dates:
            key = _build_s3_key(report.section, "BTBLoanBalance", trade_date)
            s3_bucket.put_object(
                Bucket="test-bucket", Key=key, Body=_btb_csv_bytes(trade_date)
            )

        df = api.read_bdi_report("BTBLoanBalance", dates=dates)

        assert len(df) == 2
        assert set(df["trade_date"]) == {pd.Timestamp(d) for d in dates}

    def test_missing_date_is_skipped_with_warning(self, s3_bucket, caplog):
        report = CATALOG_BY_NAME["BTBLoanBalance"]
        present_date = date(2026, 6, 30)
        missing_date = date(2026, 6, 29)
        key = _build_s3_key(report.section, "BTBLoanBalance", present_date)
        s3_bucket.put_object(
            Bucket="test-bucket", Key=key, Body=_btb_csv_bytes(present_date)
        )

        df = api.read_bdi_report(
            "BTBLoanBalance", dates=[missing_date, present_date]
        )

        assert len(df) == 1
        assert f"No 'BTBLoanBalance' report in S3 for {missing_date}" in caplog.text

    def test_all_dates_missing_returns_empty_dataframe(self, s3_bucket, sample_trade_date):
        df = api.read_bdi_report("BTBLoanBalance", dates=sample_trade_date)

        assert df.empty

    def test_unknown_report_propagates_key_error(self, s3_bucket, sample_trade_date):
        with pytest.raises(KeyError, match="Unknown BDI report"):
            api.read_bdi_report("NotARealReport", dates=sample_trade_date)


class TestReadTickByTick:
    def _parquet_bytes(self, trade_date: date) -> bytes:
        df = pd.DataFrame({"symbol": ["PETR4"], "trade_date": [trade_date]})
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        return buffer.getvalue()

    def test_single_date(self, s3_bucket, sample_trade_date):
        cfg = FeedType.RV.config
        key = _build_s3_key_ticks(cfg.s3_prefix_ticks, sample_trade_date)
        s3_bucket.put_object(
            Bucket="test-bucket", Key=key, Body=self._parquet_bytes(sample_trade_date)
        )

        df = api.read_tick_by_tick(FeedType.RV, dates=sample_trade_date)

        assert len(df) == 1
        assert df["symbol"].iloc[0] == "PETR4"

    def test_concatenates_multiple_dates(self, s3_bucket):
        cfg = FeedType.RV.config
        dates = [date(2026, 6, 29), date(2026, 6, 30)]
        for trade_date in dates:
            key = _build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date)
            s3_bucket.put_object(
                Bucket="test-bucket", Key=key, Body=self._parquet_bytes(trade_date)
            )

        df = api.read_tick_by_tick(FeedType.RV, dates=dates)

        assert len(df) == 2

    def test_missing_date_is_skipped_with_warning(self, s3_bucket, caplog):
        cfg = FeedType.RV.config
        present_date = date(2026, 6, 30)
        missing_date = date(2026, 6, 29)
        key = _build_s3_key_ticks(cfg.s3_prefix_ticks, present_date)
        s3_bucket.put_object(
            Bucket="test-bucket", Key=key, Body=self._parquet_bytes(present_date)
        )

        df = api.read_tick_by_tick(FeedType.RV, dates=[missing_date, present_date])

        assert len(df) == 1
        assert f"No ticks Parquet in S3 for {missing_date}" in caplog.text

    def test_all_dates_missing_returns_empty_dataframe(self, s3_bucket, sample_trade_date):
        df = api.read_tick_by_tick(FeedType.RV, dates=sample_trade_date)

        assert df.empty