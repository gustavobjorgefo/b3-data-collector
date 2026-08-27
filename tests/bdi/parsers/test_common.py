# tests/bdi/parsers/test_common.py

"""Unit tests for bdi/parsers/_common.py — shared parsing helpers."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from b3_data_collector.bdi.parsers._common import PERPETUAL_EXPIRY, parse_percentage


class TestPerpetualExpiry:
    def test_is_year_9999_december_31st(self):
        assert PERPETUAL_EXPIRY == date(9999, 12, 31)


class TestParsePercentage:
    def test_converts_comma_decimal_to_float(self):
        series = pd.Series(["2,92", "1,50"])

        result = parse_percentage(series)

        assert result.iloc[0] == pytest.approx(0.0292)
        assert result.iloc[1] == pytest.approx(0.0150)

    def test_strips_percent_sign(self):
        series = pd.Series(["2,92%"])

        result = parse_percentage(series)

        assert result.iloc[0] == pytest.approx(0.0292)

    def test_returns_float64_dtype(self):
        series = pd.Series(["1,00"])

        result = parse_percentage(series)

        assert result.dtype == "float64"