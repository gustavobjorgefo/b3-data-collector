# tests/bdi/parsers/test_instruments_equities.py

"""
Unit tests for bdi/parsers/_instruments_equities.py — parsing and
dispatch through the public package. No I/O beyond in-memory bytes.
"""

from __future__ import annotations

import pandas as pd
import pytest

from b3_data_collector.bdi.parsers import read_bdi_report_file
from b3_data_collector.bdi.parsers._common import PERPETUAL_EXPIRY
from b3_data_collector.bdi.parsers._instruments_equities import (
    read_instruments_equities,
)

# Minimal, structurally faithful sample: 4 discarded header rows, 2 data
# rows (a plain equity and an option, exercising different null/value
# paths), and 1 discarded disclaimer footer row.
_SAMPLE_INSTRUMENTS_CSV = (
    "Descriptive paragraph line, discarded\n"
    "Glossary link, discarded\n"
    "\n"
    "Real column headers, discarded\n"
    "TICKERA;ASSETA;Asset A description;CASH;EQUITY-CASH;SHARES;-;30/07/2026;31/12/9999;"
    "BRTICKERA001;ESVUFR;-;1;BRL;-;-;-;-;100;1;2;-;-;FALSE;ON;Company A SA;31/12/9999;"
    "FUNGIBLE;1000000;-\n"
    "TICKERB;ASSETB;Asset B description;OPTION;EQUITY-OPTIONS;OPTIONS;15/08/2050;30/07/2026;"
    "31/12/9999;BRTICKERB001;CEOGMU;Call;-;BRL;FINANCIAL;7,96;EURO;TRUE;100;1;1;"
    "SEM CORRECAO;TRUE;FALSE;CI;-;31/12/9999;FUNGIBLE;-;-\n"
    "Informamos que a tabela acima não será publicada no Boletim Completo\n"
).encode("utf-8-sig")


class TestReadInstrumentsEquities:
    def test_parses_from_bytes(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        assert len(df) == 2
        assert list(df.columns) == [
            "ticker_symbol", "asset_code", "asset_description", "segment", "market",
            "category", "expiration_date", "trading_start_date", "trading_end_date",
            "isin_code", "cfi_code", "option_type", "allocation_lot_size",
            "trading_currency", "delivery_type", "exercise_price", "option_style",
            "premium_upfront_indicator", "distribution_id", "price_factor",
            "days_to_settlement", "series_type", "protection_flag",
            "automatic_exercise_indicator", "specification_code", "corporation_name",
            "corporate_action_start_date", "custody_treatment_type",
            "market_capitalisation", "corporate_governance_level",
        ]

    def test_parses_from_path(self, tmp_path):
        csv_path = tmp_path / "sample_instruments.csv"
        csv_path.write_bytes(_SAMPLE_INSTRUMENTS_CSV)

        df = read_instruments_equities(csv_path)

        assert len(df) == 2

    def test_footer_row_is_discarded(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        # The footer's first "field" would otherwise show up as a bogus
        # ticker_symbol value here.
        assert "Informamos" not in df["ticker_symbol"].to_string()

    def test_dash_is_parsed_as_null(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        assert pd.isna(df["expiration_date"].iloc[0])
        assert pd.isna(df["option_type"].iloc[0])

    def test_perpetual_sentinel_date_is_preserved_not_nulled(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        assert df["trading_end_date"].iloc[0] == pd.Timestamp(PERPETUAL_EXPIRY)
        assert not pd.isna(df["trading_end_date"].iloc[0])

    def test_exercise_price_parses_comma_decimal(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        assert pd.isna(df["exercise_price"].iloc[0])
        assert df["exercise_price"].iloc[1] == pytest.approx(7.96)

    def test_indicator_columns_are_nullable_boolean(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        assert df["premium_upfront_indicator"].dtype == "boolean"
        assert pd.isna(df["premium_upfront_indicator"].iloc[0])
        assert df["premium_upfront_indicator"].iloc[1] == True # noqa: E712

    def test_integer_columns_are_nullable_int64(self):
        df = read_instruments_equities(_SAMPLE_INSTRUMENTS_CSV)

        assert df["market_capitalisation"].dtype == "Int64"
        assert df["market_capitalisation"].iloc[0] == 1000000
        assert pd.isna(df["market_capitalisation"].iloc[1])


class TestReadBdiReportFileDispatch:
    def test_dispatches_to_registered_parser(self):
        df = read_bdi_report_file(_SAMPLE_INSTRUMENTS_CSV, report_name="InstrumentsEquities")

        assert len(df) == 2