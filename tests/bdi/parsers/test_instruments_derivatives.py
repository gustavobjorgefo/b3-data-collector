# tests/bdi/parsers/test_instruments_derivatives.py

"""
Unit tests for bdi/parsers/_instruments_derivatives.py — parsing and
dispatch through the public package. No I/O beyond in-memory bytes.
"""

from __future__ import annotations

import pandas as pd
import pytest

from b3_data_collector.bdi.parsers import read_bdi_report_file
from b3_data_collector.bdi.parsers._instruments_derivatives import (
    read_instruments_derivatives,
)

# Real rows pulled from a full B3 export, covering four distinct shapes:
# a plain future, a rollover/spread (with leg columns populated), an
# option (with a real exercise price and premium indicator), and a
# future with a genuine comma-decimal contract multiplier ("0,2").
_SAMPLE_DERIVATIVES_CSV = (
    "Descriptive paragraph line, discarded\n"
    "Glossary link, discarded\n"
    "\n"
    "Real column headers, discarded\n"
    "ABEVOU26;ABEVO;FUTURO DE ABEV3;FINANCIAL;FUTURE;STOCK FUTURE;18/09/2026;U26;"
    "13/07/2026;18/09/2026;-;-;-;-;BRABEV390045;FFSCSX;-;-;-;1;1;100;BRL;Financial;"
    "14;14;21;-;-;-;-;-;-;-;-;Price;-;-\n"
    "AF1U26V26;AF1;ROLAGEM DE AFS;FINANCIAL;FUTURE;ROLLOVER;15/09/2026;U6V6;"
    "17/08/2026;14/09/2026;-;-;-;-;BRBMEFAF1201;KFXXXX;-;-;-;-;-;1;ZAR;-;-;-;-;"
    "Last Price;-;SELL;AFSU26;BUYI;AFSV26;-;-;-;-;-\n"
    "BBC0001U26C075000;BBC;Opcoes Binarias de BIT a vista;FINANCIAL;OPTIONS ON SPOT;"
    "-;01/09/2026;UV86;25/08/2026;01/09/2026;-;-;-;-;BRBMEFBC4KE8;OCEMCS;-;-;Call;1;"
    "1;1;BRL;-;2;2;4;-;-;-;-;-;-;75000;EURO;-;TRUE;31/08/2026\n"
    "WING27;WIN;Minicontrato de Ibovespa;FINANCIAL;FUTURE;-;17/02/2027;G27;"
    "12/08/2026;17/02/2027;-;-;-;-;BRBMEFWIN3Z9;FFICSX;-;-;-;0,2;1;1;BRL;Financial;"
    "115;113;173;-;-;-;-;-;-;-;-;Price;-;-\n"
    "Informamos que a tabela acima não será publicada no Boletim Completo\n"
).encode("utf-8-sig")

_COLUMN_NAMES = [
    "ticker_symbol", "asset_code", "asset_description", "segment", "market",
    "category", "expiration_date", "expiration_code", "trading_start_date",
    "trading_end_date", "base_code", "conversion_criteria", "maturity_date_target",
    "required_conversion_indicator", "isin_code", "cfi_code",
    "delivery_notice_start_date", "delivery_notice_end_date", "option_type",
    "contract_multiplier", "asset_quotation_quantity", "allocation_lot_size",
    "trading_currency", "delivery_type", "withdrawal_days", "working_days",
    "calendar_days", "rollover_base_price", "opening_future_position_day",
    "side_type_code_1", "underlying_ticker_symbol_1", "side_type_code_2",
    "underlying_ticker_symbol_2", "exercise_price", "option_style", "value_type",
    "premium_upfront_indicator", "opening_position_limit_date",
]


class TestReadInstrumentsDerivatives:
    def test_parses_from_bytes(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        assert len(df) == 4
        assert list(df.columns) == _COLUMN_NAMES

    def test_parses_from_path(self, tmp_path):
        csv_path = tmp_path / "sample_derivatives.csv"
        csv_path.write_bytes(_SAMPLE_DERIVATIVES_CSV)

        df = read_instruments_derivatives(csv_path)

        assert len(df) == 4

    def test_footer_row_is_discarded(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        # The footer's first "field" would otherwise show up as a bogus
        # ticker_symbol value here.
        assert "Informamos" not in df["ticker_symbol"].to_string()

    def test_dash_is_parsed_as_null(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        # base_code is "-" for every row in this sample.
        assert df["base_code"].isna().all()

    def test_contract_multiplier_parses_comma_decimal(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        wing27 = df[df["ticker_symbol"] == "WING27"].iloc[0]
        assert wing27["contract_multiplier"] == pytest.approx(0.2)

    def test_exercise_price_populated_only_for_options(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        option_row = df[df["ticker_symbol"] == "BBC0001U26C075000"].iloc[0]
        future_row = df[df["ticker_symbol"] == "ABEVOU26"].iloc[0]

        assert option_row["exercise_price"] == pytest.approx(75000.0)
        assert pd.isna(future_row["exercise_price"])

    def test_indicator_columns_are_nullable_boolean(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        option_row = df[df["ticker_symbol"] == "BBC0001U26C075000"].iloc[0]
        future_row = df[df["ticker_symbol"] == "ABEVOU26"].iloc[0]

        assert df["premium_upfront_indicator"].dtype == "boolean"
        assert bool(option_row["premium_upfront_indicator"]) is True
        assert pd.isna(future_row["premium_upfront_indicator"])

    def test_integer_columns_are_nullable_int64(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        future_row = df[df["ticker_symbol"] == "ABEVOU26"].iloc[0]

        assert df["working_days"].dtype == "Int64"
        assert future_row["working_days"] == 14

    def test_leg_columns_populated_only_for_rollover_instruments(self):
        df = read_instruments_derivatives(_SAMPLE_DERIVATIVES_CSV)

        rollover_row = df[df["ticker_symbol"] == "AF1U26V26"].iloc[0]
        future_row = df[df["ticker_symbol"] == "ABEVOU26"].iloc[0]

        assert rollover_row["side_type_code_1"] == "SELL"
        assert rollover_row["underlying_ticker_symbol_1"] == "AFSU26"
        assert pd.isna(future_row["side_type_code_1"])
        assert pd.isna(future_row["underlying_ticker_symbol_1"])


class TestReadBdiReportFileDispatch:
    def test_dispatches_to_registered_parser(self):
        df = read_bdi_report_file(_SAMPLE_DERIVATIVES_CSV, report_name="InstrumentsDerivatives")

        assert len(df) == 4