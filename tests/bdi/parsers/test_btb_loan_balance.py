# tests/bdi/parsers/test_btb_loan_balance.py

"""
Unit tests for bdi/parsers/_btb_loan_balance.py — parsing and dispatch
through the public package. No I/O beyond in-memory bytes.
"""

from __future__ import annotations

import pandas as pd
import pytest

from b3_data_collector.bdi.parsers import read_bdi_report_file
from b3_data_collector.bdi.parsers._btb_loan_balance import read_btb_loan_balance

_SAMPLE_BTB_CSV = (
    "Descriptive paragraph line, discarded\n"
    "Glossary link, discarded\n"
    "\n"
    "Merged group labels, discarded\n"
    "Real column headers, discarded\n"
    "30/06/2026;12345;BRXYZW00001;Example Company SA;BOVESPA;10;1000;50000,00;"
    "1,50;2,00;2,50;2,92;3,10;3,50\n"
    "30/06/2026;54321;BRABCD00002;Another Company SA;BOVESPA;;500;25000,00;"
    "1,00;1,20;1,40;1,80;2,00;2,20\n"
).encode("utf-8-sig")


class TestReadBtbLoanBalance:
    def test_parses_from_bytes(self):
        df = read_btb_loan_balance(_SAMPLE_BTB_CSV)

        assert len(df) == 2
        assert list(df.columns) == [
            "trade_date", "if_code", "isin_code", "company_name", "market",
            "contracts_count", "assets_quantity", "value_brl",
            "donor_rate_min", "donor_rate_avg", "donor_rate_max",
            "borrower_rate_min", "borrower_rate_avg", "borrower_rate_max",
        ]

    def test_parses_from_path(self, tmp_path):
        csv_path = tmp_path / "sample_btb.csv"
        csv_path.write_bytes(_SAMPLE_BTB_CSV)

        df = read_btb_loan_balance(csv_path)

        assert len(df) == 2

    def test_trade_date_is_datetime(self):
        df = read_btb_loan_balance(_SAMPLE_BTB_CSV)

        assert pd.api.types.is_datetime64_any_dtype(df["trade_date"])
        assert df["trade_date"].iloc[0] == pd.Timestamp("2026-06-30")

    def test_contracts_count_is_nullable_int64(self):
        df = read_btb_loan_balance(_SAMPLE_BTB_CSV)

        assert df["contracts_count"].dtype == "Int64"
        assert df["contracts_count"].iloc[0] == 10
        assert pd.isna(df["contracts_count"].iloc[1])

    def test_percentage_columns_converted_to_float(self):
        df = read_btb_loan_balance(_SAMPLE_BTB_CSV)

        assert df["borrower_rate_min"].iloc[0] == pytest.approx(0.0292)
        assert df["donor_rate_avg"].iloc[1] == pytest.approx(0.0120)


class TestReadBdiReportFileDispatch:
    def test_dispatches_to_registered_parser(self):
        df = read_bdi_report_file(_SAMPLE_BTB_CSV, report_name="BTBLoanBalance")

        assert len(df) == 2

    def test_unregistered_report_raises_not_implemented_error(self):
        with pytest.raises(NotImplementedError, match="No parser registered"):
            read_bdi_report_file(_SAMPLE_BTB_CSV, report_name="SomeReportWithNoParser")