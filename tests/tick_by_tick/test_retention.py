# tests/tick_by_tick/test_retention.py

"""
Unit tests for tick_by_tick/_retention.py.

Files are created empty-but-sized in the patched data directories and S3
is mocked with moto, so no real file or bucket is ever touched. A fixed
``today`` keeps the cutoff deterministic.
"""

from __future__ import annotations

from datetime import date

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from b3_data_collector.config import Settings
from b3_data_collector.tick_by_tick import _retention
from b3_data_collector.tick_by_tick._feed import FeedType
from b3_data_collector.tick_by_tick._partitioner import _build_s3_key_ticks

TODAY          = date(2026, 11, 24)
RETENTION_DAYS = 28                    # cutoff = 2026-10-27
OLD_DATE       = date(2026, 10, 26)    # one day before the cutoff -> eligible
CUTOFF_DATE    = date(2026, 10, 27)    # on the cutoff -> kept
RECENT_DATE    = date(2026, 11, 20)
BUCKET         = "test-bucket"


# --- Fixtures and helpers ---

@pytest.fixture
def s3_bucket(monkeypatch):
    monkeypatch.setattr(_retention, "settings", Settings(AWS_S3_BUCKET_B3=BUCKET))
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _write(directory, name: str, size: int = 1_024):
    path = directory / name
    path.write_bytes(b"x" * size)
    return path


def _zip_name(feed: FeedType, trade_date: date) -> str:
    return feed.config.zip_name_template.format(date=trade_date)


def _put_in_s3(client, feed: FeedType, trade_date: date) -> None:
    cfg = feed.config
    client.put_object(Bucket=BUCKET, Key=cfg.s3_prefix + _zip_name(feed, trade_date), Body=b"")
    client.put_object(
        Bucket=BUCKET, Key=_build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date), Body=b""
    )


def _prune(feed: FeedType = FeedType.RV):
    return _retention.prune_local_files(feed, retention_days=RETENTION_DAYS, today=TODAY)


# --- Name parsing ---

class TestNameParsing:
    @pytest.mark.parametrize("feed", list(FeedType))
    def test_zip_template_round_trips(self, feed):
        name_format = _retention._template_to_strptime(feed.config.zip_name_template)

        parsed = _retention._parse_trade_date(_zip_name(feed, OLD_DATE), name_format)

        assert parsed == OLD_DATE

    def test_name_from_other_feed_does_not_match(self):
        rv_format = _retention._template_to_strptime(FeedType.RV.config.zip_name_template)

        assert _retention._parse_trade_date(_zip_name(FeedType.DERIV, OLD_DATE), rv_format) is None

    def test_parquet_name_round_trips(self):
        parsed = _retention._parse_trade_date(
            f"{OLD_DATE}.parquet", _retention._PARQUET_NAME_FORMAT
        )

        assert parsed == OLD_DATE


# --- Retention rules ---

class TestPruneLocalFiles:
    def test_deletes_old_files_confirmed_in_s3_and_keeps_recent(
        self, patched_paths, s3_bucket
    ):
        old_zip   = _write(patched_paths["rv_downloads"], _zip_name(FeedType.RV, OLD_DATE))
        old_ticks = _write(patched_paths["rv_ticks"], f"{OLD_DATE}.parquet")
        new_zip   = _write(patched_paths["rv_downloads"], _zip_name(FeedType.RV, RECENT_DATE))
        new_ticks = _write(patched_paths["rv_ticks"], f"{RECENT_DATE}.parquet")
        _put_in_s3(s3_bucket, FeedType.RV, OLD_DATE)

        result = _prune()

        assert not old_zip.exists()
        assert not old_ticks.exists()
        assert new_zip.exists()
        assert new_ticks.exists()
        assert result.deleted == 2
        assert result.freed_bytes == 2 * 1_024

    def test_file_on_cutoff_date_is_kept(self, patched_paths, s3_bucket):
        ticks = _write(patched_paths["rv_ticks"], f"{CUTOFF_DATE}.parquet")
        _put_in_s3(s3_bucket, FeedType.RV, CUTOFF_DATE)

        result = _prune()

        assert ticks.exists()
        assert result.deleted == 0

    def test_keeps_old_files_missing_from_s3(self, patched_paths, s3_bucket):
        old_zip   = _write(patched_paths["rv_downloads"], _zip_name(FeedType.RV, OLD_DATE))
        old_ticks = _write(patched_paths["rv_ticks"], f"{OLD_DATE}.parquet")

        result = _prune()

        assert old_zip.exists()
        assert old_ticks.exists()
        assert result.kept_missing_in_s3 == 2
        assert result.deleted == 0

    def test_deletes_old_raw_without_s3_check(self, patched_paths, s3_bucket):
        old_raw = _write(patched_paths["rv_raw_parquet"], f"{OLD_DATE}.parquet")
        new_raw = _write(patched_paths["rv_raw_parquet"], f"{RECENT_DATE}.parquet")

        result = _prune()

        assert not old_raw.exists()
        assert new_raw.exists()
        assert result.deleted == 1

    def test_never_deletes_unrecognised_files(self, patched_paths, s3_bucket):
        # Real case: a manual RV download saved under a non-pipeline name.
        stray = _write(patched_paths["rv_downloads"], "30-06-2026_NEGOCIOSAVISTA_RV.zip")

        result = _prune()

        assert stray.exists()
        assert result.ignored == 1

    def test_only_touches_the_requested_feed(self, patched_paths, s3_bucket):
        deriv_ticks = _write(patched_paths["deriv_ticks"], f"{OLD_DATE}.parquet")
        _put_in_s3(s3_bucket, FeedType.DERIV, OLD_DATE)

        _prune(FeedType.RV)

        assert deriv_ticks.exists()

    def test_s3_error_keeps_file_and_counts_error(
        self, patched_paths, s3_bucket, monkeypatch
    ):
        old_ticks = _write(patched_paths["rv_ticks"], f"{OLD_DATE}.parquet")

        def _raise(*_args, **_kwargs):
            raise ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject")

        monkeypatch.setattr(_retention, "s3_object_exists", _raise)

        result = _prune()

        assert old_ticks.exists()
        assert result.errors == 1

    def test_negative_retention_raises_value_error(self):
        with pytest.raises(ValueError, match="retention_days"):
            _retention.prune_local_files(FeedType.RV, retention_days=-1, today=TODAY)


class TestPruneDirectoryGuard:
    def test_s3_key_without_client_raises(self, tmp_path):
        result = _retention.RetentionResult(label="test", cutoff=TODAY)

        with pytest.raises(ValueError, match="together"):
            _retention._prune_directory(
                directory   = tmp_path,
                name_format = _retention._PARQUET_NAME_FORMAT,
                cutoff      = TODAY,
                result      = result,
                s3_key      = lambda trade_date, _name: f"{trade_date}",
            )
