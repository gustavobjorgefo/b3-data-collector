# src/b3_data_collector/bdi/parsers/_instruments_derivatives.py

"""Parser for InstrumentsDerivatives ("Cadastro de instrumentos")."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Final

import pandas as pd

from ._registry import register_parser

_COLUMN_NAMES: Final[list[str]] = [
    "ticker_symbol",
    "asset_code",
    "asset_description",
    "segment",
    "market",
    "category",
    "expiration_date",
    "expiration_code",
    "trading_start_date",
    "trading_end_date",
    "base_code",
    "conversion_criteria",
    "maturity_date_target",
    "required_conversion_indicator",
    "isin_code",
    "cfi_code",
    "delivery_notice_start_date",
    "delivery_notice_end_date",
    "option_type",
    "contract_multiplier",
    "asset_quotation_quantity",
    "allocation_lot_size",
    "trading_currency",
    "delivery_type",
    "withdrawal_days",
    "working_days",
    "calendar_days",
    "rollover_base_price",
    "opening_future_position_day",
    "side_type_code_1",
    "underlying_ticker_symbol_1",
    "side_type_code_2",
    "underlying_ticker_symbol_2",
    "exercise_price",
    "option_style",
    "value_type",
    "premium_upfront_indicator",
    "opening_position_limit_date",
]

_DATE_COLUMNS: Final[tuple[str, ...]] = (
    "expiration_date",
    "trading_start_date",
    "trading_end_date",
    "maturity_date_target",
    "delivery_notice_start_date",
    "delivery_notice_end_date",
    "opening_position_limit_date",
)

_INT_COLUMNS: Final[tuple[str, ...]] = (
    "asset_quotation_quantity",
    "allocation_lot_size",
    "withdrawal_days",
    "working_days",
    "calendar_days",
    "opening_future_position_day",
)

_BOOLEAN_COLUMNS: Final[tuple[str, ...]] = (
    "required_conversion_indicator",
    "premium_upfront_indicator",
)


@register_parser("InstrumentsDerivatives")
def read_instruments_derivatives(source: str | Path | bytes) -> pd.DataFrame:
    """
    Parse an "InstrumentsDerivatives" (Cadastro de instrumentos) BDI report.

    File layout, as exported from B3 (';'-separated, UTF-8 with BOM):

        Row 1   : long descriptive paragraph — discarded
        Row 2   : glossary link — discarded
        Row 3   : blank — discarded
        Row 4   : actual column headers — discarded, replaced by canonical names
        Row 5+  : one row per derivative instrument (futures, options, and
                  rollover/spread strategies)
        Last row: disclaimer footer, not a data row — discarded

    Same trailing-footer shape as ``InstrumentsEquities``, so this parser
    reuses the same ``skipfooter=1`` / Python-engine trade-off already
    measured as negligible on files of this size. See
    ``_instruments_equities.read_instruments_equities`` for the reasoning.

    Every column may contain ``"-"`` as B3's "not applicable" marker,
    parsed here as a universal null across all 38 columns via
    ``na_values``. Unlike ``InstrumentsEquities``, no ``"31/12/9999"``
    perpetual-expiry sentinel was observed in this report — derivatives
    always carry a contractual maturity, so every date column here is
    expected to resolve to a real date or a null, never the sentinel.

    Two columns are strategy "legs" (``side_type_code_1/2`` and
    ``underlying_ticker_symbol_1/2``), populated only for rollover and
    spread instruments; they're null for plain futures and options.

    Parameters
    ----------
    source : str | Path | bytes
        Path to a downloaded CSV, or its raw bytes.

    Returns
    -------
    pd.DataFrame
        One row per derivative instrument. Date columns are datetime64;
        ``contract_multiplier`` and ``exercise_price`` are float64
        (B3's comma-decimal notation, e.g. "0,2", handled directly by
        ``decimal=","`` at read time); quantity/day-count columns are
        nullable Int64; the two indicator columns are nullable boolean.
    """
    buffer = io.BytesIO(source) if isinstance(source, bytes) else source

    df = pd.read_csv(
        buffer,
        sep=";",
        skiprows=4,
        skipfooter=1,
        header=None,
        names=_COLUMN_NAMES,
        encoding="utf-8-sig",
        decimal=",",
        na_values=["-"],
        engine="python",
    )

    for col in _DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

    for col in _INT_COLUMNS:
        df[col] = df[col].astype("Int64")

    for col in _BOOLEAN_COLUMNS:
        df[col] = df[col].astype("boolean")

    return df