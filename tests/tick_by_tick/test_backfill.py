# tests/tick_by_tick/test_backfill.py

"""
Unit tests for tick_by_tick/backfill.py.

S3 is fully mocked with moto and every local path points to tmp_path, so
the real fixture ZIPs travel the whole way: archived in the fake bucket,
restored, extracted, partitioned and uploaded over the old ticks file.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from b3_data_collector.config import Settings
from b3_data_collector.tick_by_tick import _contract, _partitioner, backfill, pipeline
from b3_data_collector.tick_by_tick._contract import METADATA_KEY
from b3_data_collector.tick_by_tick._downloader import _build_s3_key_zip
from b3_data_collector.tick_by_tick._feed import FeedType
from tests.conftest import FIXTURES_DIR

BUCKET = "test-bucket"
TRADE_DATE = date(2026, 6, 30)
FIXTURE_ZIPS = {FeedType.RV: "sample_rv.zip", FeedType.DERIV: "sample_deriv.zip"}


# --- Fixtures ---

@pytest.fixture
def s3(patched_paths, monkeypatch):
    fake_settings = Settings(
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
        AWS_S3_REGION="us-east-1",
        AWS_S3_BUCKET_B3=BUCKET,
    )
    for module in (backfill, pipeline, _partitioner):
        monkeypatch.setattr(module, "settings", fake_settings)
    # A fixed, clean commit: provenance and the skip rule must agree on it.
    monkeypatch.setattr(_contract, "collector_commit", lambda: "abc123")
    monkeypatch.setattr(backfill, "collector_commit", lambda: "abc123")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def archive_zip(client, feed: FeedType, trade_date: date = TRADE_DATE) -> None:
    client.put_object(
        Bucket=BUCKET,
        Key=_build_s3_key_zip(feed.config, trade_date),
        Body=(FIXTURES_DIR / FIXTURE_ZIPS[feed]).read_bytes(),
    )


def ticks_key(feed: FeedType, trade_date: date = TRADE_DATE) -> str:
    return _partitioner._build_s3_key_ticks(feed.config.s3_prefix_ticks, trade_date)


def put_pre_contract_ticks(client, feed: FeedType, trade_date: date = TRADE_DATE) -> None:
    buffer = io.BytesIO()
    pd.DataFrame({"symbol": ["OLD"]}).to_parquet(buffer, index=False)
    client.put_object(Bucket=BUCKET, Key=ticks_key(feed, trade_date), Body=buffer.getvalue())


def remote_provenance(client, feed: FeedType, trade_date: date = TRADE_DATE) -> dict | None:
    body = client.get_object(Bucket=BUCKET, Key=ticks_key(feed, trade_date))["Body"].read()
    metadata = pq.read_schema(io.BytesIO(body)).metadata or {}
    return json.loads(metadata[METADATA_KEY]) if METADATA_KEY in metadata else None


# --- Listing and remote footers ---

class TestListArchivedDates:
    def test_parses_dates_and_ignores_other_keys(self, s3):
        archive_zip(s3, FeedType.DERIV)
        archive_zip(s3, FeedType.DERIV, date(2026, 7, 1))
        s3.put_object(Bucket=BUCKET, Key="b3/bdi/tick_by_tick_deriv/notes.txt", Body=b"x")

        dates = backfill.list_archived_dates(s3, BUCKET, FeedType.DERIV)

        assert dates == [TRADE_DATE, date(2026, 7, 1)]


class TestReadRemoteSchema:
    def test_reads_footer_metadata(self, s3, tmp_path):
        table = _contract.with_provenance(
            pq.read_table(io.BytesIO(_fixture_parquet_bytes())),
            _contract.SourceProvenance("RV", "2026-06-30", "z", "0" * 64, "t", 1),
        )
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        s3.put_object(Bucket=BUCKET, Key="file.parquet", Body=buffer.getvalue())

        schema = backfill._read_remote_schema(s3, BUCKET, "file.parquet")

        assert json.loads(schema.metadata[METADATA_KEY])["feed"] == "RV"

    def test_missing_object_gives_none(self, s3):
        assert backfill._read_remote_schema(s3, BUCKET, "missing.parquet") is None

    def test_non_parquet_raises(self, s3):
        s3.put_object(Bucket=BUCKET, Key="file.parquet", Body=b"not a parquet file")
        with pytest.raises(ValueError, match="not a Parquet file"):
            backfill._read_remote_schema(s3, BUCKET, "file.parquet")


def _fixture_parquet_bytes() -> bytes:
    buffer = io.BytesIO()
    pd.DataFrame({"column": range(1000)}).to_parquet(buffer, index=False)
    return buffer.getvalue()


# --- Backfill ---

class TestRunBackfill:
    def test_rewrites_old_ticks_for_both_feeds(self, s3, patched_paths, tmp_path):
        for feed in FeedType:
            archive_zip(s3, feed)
            put_pre_contract_ticks(s3, feed)
        report_path = tmp_path / "report.csv"

        rows = backfill.run_backfill(list(FeedType), report_path)

        assert [(row.feed, row.status, row.row_count) for row in rows] == [
            ("RV", "OK", 20), ("DERIV", "OK", 20),
        ]
        for feed in FeedType:
            provenance = remote_provenance(s3, feed)
            assert provenance["schema_version"] == _contract.SCHEMA_VERSION
            assert provenance["collector_commit"] == "abc123"
        with report_path.open(encoding="utf-8") as handle:
            report = list(csv.DictReader(handle))
        assert [line["status"] for line in report] == ["OK", "OK"]
        assert report[0]["timestamp_null_count"] == "0"
        assert report[0]["other_session_row_count"] == "0"

    def test_leaves_no_local_files(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.RV)

        backfill.run_backfill([FeedType.RV], tmp_path / "report.csv")

        assert not [path for directory in patched_paths.values() for path in directory.iterdir()]

    def test_keep_local_keeps_zip_and_ticks(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.RV)

        backfill.run_backfill([FeedType.RV], tmp_path / "report.csv", keep_local=True)

        assert any(patched_paths["rv_downloads"].iterdir())
        assert any(patched_paths["rv_ticks"].iterdir())

    def test_skips_dates_written_by_this_commit(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.RV)
        backfill.run_backfill([FeedType.RV], tmp_path / "first.csv")

        rows = backfill.run_backfill([FeedType.RV], tmp_path / "second.csv")

        assert [row.status for row in rows] == ["SKIPPED"]

    def test_force_rebuilds_dates_written_by_this_commit(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.RV)
        backfill.run_backfill([FeedType.RV], tmp_path / "first.csv")

        rows = backfill.run_backfill([FeedType.RV], tmp_path / "second.csv", force=True)

        assert [row.status for row in rows] == ["OK"]

    def test_dirty_checkout_never_skips(self, s3, patched_paths, tmp_path, monkeypatch):
        monkeypatch.setattr(_contract, "collector_commit", lambda: "abc123+dirty")
        monkeypatch.setattr(backfill, "collector_commit", lambda: "abc123+dirty")
        archive_zip(s3, FeedType.RV)
        backfill.run_backfill([FeedType.RV], tmp_path / "first.csv")

        rows = backfill.run_backfill([FeedType.RV], tmp_path / "second.csv")

        assert [row.status for row in rows] == ["OK"]

    def test_failure_is_reported_and_run_goes_on(self, s3, patched_paths, tmp_path):
        bad_date = date(2026, 6, 29)
        s3.put_object(
            Bucket=BUCKET, Key=_build_s3_key_zip(FeedType.RV.config, bad_date), Body=b"not a zip"
        )
        archive_zip(s3, FeedType.RV)

        rows = backfill.run_backfill([FeedType.RV], tmp_path / "report.csv")

        assert [(row.trade_date, row.status) for row in rows] == [
            ("2026-06-29", "FAILED"), ("2026-06-30", "OK"),
        ]
        assert "Corrupt ZIP" in rows[0].error
        assert not any(patched_paths["rv_downloads"].iterdir())

    def test_requested_date_without_zip_fails(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.RV)

        rows = backfill.run_backfill(
            [FeedType.RV], tmp_path / "report.csv", dates=[date(2026, 7, 1), TRADE_DATE]
        )

        assert [(row.trade_date, row.status) for row in rows] == [
            ("2026-06-30", "OK"), ("2026-07-01", "FAILED"),
        ]
        assert rows[1].error == "no archived ZIP in S3"

    def test_dry_run_writes_nothing_to_s3(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.RV)
        put_pre_contract_ticks(s3, FeedType.RV)

        rows = backfill.run_backfill([FeedType.RV], tmp_path / "report.csv", dry_run=True)

        assert [row.status for row in rows] == ["WOULD_RUN"]
        assert remote_provenance(s3, FeedType.RV) is None


# --- Verification ---

class TestVerify:
    def test_conforming_bucket_has_no_problems(self, s3, patched_paths, tmp_path):
        for feed in FeedType:
            archive_zip(s3, feed)
        backfill.run_backfill(list(FeedType), tmp_path / "report.csv")

        assert backfill.verify(list(FeedType)) == []

    def test_reports_every_kind_of_problem(self, s3, patched_paths):
        archive_zip(s3, FeedType.RV)                        # ZIP, pre-contract ticks
        put_pre_contract_ticks(s3, FeedType.RV)
        archive_zip(s3, FeedType.RV, date(2026, 7, 1))      # ZIP, no ticks
        put_pre_contract_ticks(s3, FeedType.RV, date(2026, 7, 2))  # ticks, no ZIP

        problems = backfill.verify([FeedType.RV])

        assert problems == [
            "RV 2026-07-02: ticks Parquet without archived ZIP",
            "RV 2026-06-30: pre-contract file (no provenance)",
            "RV 2026-07-01: no ticks Parquet",
        ]


# --- Entrypoint ---

class TestMain:
    @pytest.fixture(autouse=True)
    def logs_in_tmp(self, tmp_path, monkeypatch):
        monkeypatch.setattr(backfill, "LOGS_DIR", tmp_path / "logs")
        root_logger = logging.getLogger()
        handlers_before = list(root_logger.handlers)
        level_before = root_logger.level
        yield
        # main() attaches handlers to the root logger; an open log file
        # would also block tmp_path cleanup on Windows.
        for handler in set(root_logger.handlers) - set(handlers_before):
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.setLevel(level_before)

    def test_full_run_succeeds(self, s3, patched_paths, tmp_path):
        archive_zip(s3, FeedType.DERIV)

        exit_code = backfill.main(["--feed", "DERIV"])

        assert exit_code == 0
        assert len(list((tmp_path / "logs").glob("backfill_tick_by_tick_*.csv"))) == 1

    def test_verify_only_fails_on_pre_contract_files(self, s3, patched_paths):
        archive_zip(s3, FeedType.RV)
        put_pre_contract_ticks(s3, FeedType.RV)

        assert backfill.main(["--feed", "RV", "--verify-only"]) == 1

    def test_dry_run_skips_verification(self, s3, patched_paths):
        archive_zip(s3, FeedType.RV)

        assert backfill.main(["--feed", "RV", "--dry-run"]) == 0
