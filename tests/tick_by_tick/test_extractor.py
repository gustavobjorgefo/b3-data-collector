# tests/tick_by_tick/test_extractor.py

"""
Unit tests for tick_by_tick/_extractor.py — parses real (trimmed) B3 ZIPs.

Uses tests/fixtures/sample_rv.zip and sample_deriv.zip, built from real
B3 downloads via scripts/build_test_fixture.py, so these tests exercise
the actual on-disk format (separator, encoding, price/time formatting)
rather than a hand-written approximation of it.
"""

from __future__ import annotations

import hashlib
import json
import zipfile

import pandas as pd
import pyarrow.parquet as pq
import pytest

from b3_data_collector.common import StageStatus
from b3_data_collector.tick_by_tick import _extractor
from b3_data_collector.tick_by_tick._contract import (
    METADATA_KEY,
    ContractViolationError,
)
from b3_data_collector.tick_by_tick._feed import FeedType
from tests.conftest import place_sample_zip, tick_line, write_tick_zip


class TestExtractToRawParquetRV:
    def test_successful_extraction(self, patched_paths, sample_zip_trade_date):
        place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.RV
        )

        assert status is StageStatus.SUCCESS

        output_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        assert output_file.exists()

        df = pd.read_parquet(output_file)
        assert len(df) == 20

        assert list(df.columns) == [
            "source_row", "symbol", "trade_date", "reference_date", "time_raw",
            "price", "quantity", "update_action", "session_type", "channel_type",
            "trade_id", "buyer_broker", "seller_broker",
        ]
        assert df["price"].dtype == "float64"
        assert df["quantity"].dtype == "uint32"
        assert df["source_row"].dtype == "int32"
        # Header is line 1; rows keep the TXT order.
        assert df["source_row"].tolist() == list(range(2, 22))
        assert df["time_raw"].str.fullmatch(r"\d{9}").all()
        assert (df["reference_date"] == "2026-06-30").all()

    def test_records_source_provenance(self, patched_paths, sample_zip_trade_date):
        zip_path = place_sample_zip(
            patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date
        )

        _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.RV)

        output_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        provenance = json.loads(pq.read_schema(output_file).metadata[METADATA_KEY])
        assert provenance == {
            "feed"                 : "RV",
            "trade_date"           : "2026-06-30",
            "source_zip_name"      : zip_path.name,
            "source_zip_sha256"    : hashlib.sha256(zip_path.read_bytes()).hexdigest(),
            "source_txt_name"      : "30-06-2026_NEGOCIOSAVISTA_RV.txt",
            "source_txt_row_count" : 20,
        }

    def test_skips_when_output_exists_and_overwrite_false(
        self, patched_paths, sample_zip_trade_date
    ):
        place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)
        output_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        output_file.write_bytes(b"fake existing parquet")

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, overwrite=False
        )

        assert status is StageStatus.SKIPPED
        # untouched — still the fake placeholder content
        assert output_file.read_bytes() == b"fake existing parquet"

    def test_skips_when_ticks_exist_and_overwrite_false(
        self, patched_paths, sample_zip_trade_date
    ):
        # The partitioner deletes the raw Parquet after writing ticks, so a
        # re-run must not rebuild it just because it is missing.
        place_sample_zip(patched_paths, FeedType.RV, "sample_rv.zip", sample_zip_trade_date)
        ticks_file = patched_paths["rv_ticks"] / f"{sample_zip_trade_date}.parquet"
        ticks_file.write_bytes(b"fake existing ticks")

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.RV, overwrite=False
        )

        assert status is StageStatus.SKIPPED
        raw_file = patched_paths["rv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        assert not raw_file.exists()

    def test_missing_zip_raises_file_not_found(self, patched_paths, sample_zip_trade_date):
        with pytest.raises(FileNotFoundError):
            _extractor.extract_to_raw_parquet(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )

    def test_corrupt_zip_raises_value_error(self, patched_paths, sample_zip_trade_date):
        cfg = FeedType.RV.config
        filename = cfg.zip_name_template.format(date=sample_zip_trade_date)
        bad_zip = patched_paths["rv_downloads"] / filename
        bad_zip.write_bytes(b"this is not a real zip file")

        with pytest.raises(ValueError):
            _extractor.extract_to_raw_parquet(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )

    def test_missing_txt_prefix_raises_key_error(self, patched_paths, sample_zip_trade_date):
        cfg = FeedType.RV.config
        filename = cfg.zip_name_template.format(date=sample_zip_trade_date)
        zip_path = patched_paths["rv_downloads"] / filename

        # A well-formed ZIP, but with a TXT name that doesn't match the
        # expected prefix for this date.
        with zipfile.ZipFile(zip_path, "w") as archive:
            archive.writestr("wrong_name.txt", "irrelevant content")

        with pytest.raises(KeyError):
            _extractor.extract_to_raw_parquet(
                trade_date=sample_zip_trade_date, feed=FeedType.RV
            )


class TestExtractToRawParquetDeriv:
    def test_successful_extraction(self, patched_paths, sample_zip_trade_date):
        place_sample_zip(
            patched_paths, FeedType.DERIV, "sample_deriv.zip", sample_zip_trade_date
        )

        status = _extractor.extract_to_raw_parquet(
            trade_date=sample_zip_trade_date, feed=FeedType.DERIV
        )

        assert status is StageStatus.SUCCESS

        output_file = patched_paths["deriv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        df = pd.read_parquet(output_file)
        assert len(df) == 20
        assert df["price"].dtype == "float64"
        # Opens before 10:00: the leading zero must survive.
        assert df["time_raw"].iloc[0] == "090005990"

    def test_anonymous_broker_becomes_null(self, patched_paths, sample_zip_trade_date):
        # BGIV26 is an agricultural derivative: B3 publishes it without brokers.
        place_sample_zip(
            patched_paths, FeedType.DERIV, "sample_deriv.zip", sample_zip_trade_date
        )

        _extractor.extract_to_raw_parquet(trade_date=sample_zip_trade_date, feed=FeedType.DERIV)

        output_file = patched_paths["deriv_raw_parquet"] / f"{sample_zip_trade_date}.parquet"
        df = pd.read_parquet(output_file)
        anonymous = df[df["symbol"] == "BGIV26"]
        assert anonymous["buyer_broker"].isna().all()
        assert anonymous["seller_broker"].isna().all()
        assert df["buyer_broker"].notna().sum() == 19


class TestExtractorContractRules:
    """Hand-made TXTs covering the malformed inputs real fixtures lack."""

    @staticmethod
    def _extract(patched_paths, trade_date, lines, **zip_options) -> pd.DataFrame:
        write_tick_zip(patched_paths, FeedType.RV, trade_date, lines, **zip_options)
        _extractor.extract_to_raw_parquet(trade_date=trade_date, feed=FeedType.RV)
        return pd.read_parquet(patched_paths["rv_raw_parquet"] / f"{trade_date}.parquet")

    def test_unreadable_time_is_kept_verbatim(self, patched_paths, sample_zip_trade_date):
        df = self._extract(
            patched_paths, sample_zip_trade_date, [tick_line(), tick_line(time_raw="")]
        )
        assert df["time_raw"].tolist() == ["100000006", ""]

    def test_empty_broker_becomes_null(self, patched_paths, sample_zip_trade_date):
        df = self._extract(
            patched_paths, sample_zip_trade_date, [tick_line(buyer="", seller="0")]
        )
        assert df["buyer_broker"].isna().all()
        # A real "0" stays a code: only an empty cell means "not disclosed".
        assert df["seller_broker"].tolist() == ["0"]

    def test_blank_lines_are_skipped_and_line_numbers_kept(
        self, patched_paths, sample_zip_trade_date
    ):
        df = self._extract(
            patched_paths, sample_zip_trade_date, [tick_line(), "", tick_line(trade_id="20")]
        )
        assert df["source_row"].tolist() == [2, 4]
        assert df["trade_id"].tolist() == [10, 20]

    def test_wrong_header_fails(self, patched_paths, sample_zip_trade_date):
        with pytest.raises(ContractViolationError, match="Unexpected header"):
            self._extract(
                patched_paths, sample_zip_trade_date, [tick_line()], header="a;b;c"
            )

    @pytest.mark.parametrize("line", ["2026-06-30;BAIQ39", tick_line() + ";extra"])
    def test_wrong_field_count_fails(self, patched_paths, sample_zip_trade_date, line):
        with pytest.raises(ContractViolationError, match="first at line 3"):
            self._extract(patched_paths, sample_zip_trade_date, [tick_line(), line])

    @pytest.mark.parametrize(
        ("override", "column"),
        [
            ({"quantity": "abc"},          "QuantidadeNegociada"),
            ({"quantity": "4294967296"},   "QuantidadeNegociada"),
            ({"update_action": "256"},     "AcaoAtualizacao"),
            ({"update_action": ""},        "AcaoAtualizacao"),
            ({"price": "1,2,3"},           "PrecoNegocio"),
            ({"trade_id": "-1"},           "CodigoIdentificadorNegocio"),
            ({"trade_date": "2026-02-30"}, "DataNegocio"),
            # Must be the file's date: the timestamp is built from it.
            ({"reference_date": "2026-07-01"}, "DataReferencia"),
        ],
    )
    def test_unconvertible_value_fails(
        self, patched_paths, sample_zip_trade_date, override, column
    ):
        with pytest.raises(ContractViolationError, match=rf"{column}.*TXT line 3"):
            self._extract(
                patched_paths, sample_zip_trade_date, [tick_line(), tick_line(**override)]
            )

    def test_more_than_one_txt_fails(self, patched_paths, sample_zip_trade_date):
        with pytest.raises(ContractViolationError, match="more than one TXT"):
            self._extract(
                patched_paths, sample_zip_trade_date, [tick_line()],
                extra_txt_names=("30-06-2026_NEGOCIOSAVISTA_RV_revised.txt",),
            )