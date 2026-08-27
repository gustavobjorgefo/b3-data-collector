# src/b3_data_collector/bdi/parsers/_instruments_equities.py

"""Parser for InstrumentsEquities ("Cadastro de instrumentos")."""

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
    "trading_start_date",
    "trading_end_date",
    "isin_code",
    "cfi_code",
    "option_type",
    "allocation_lot_size",
    "trading_currency",
    "delivery_type",
    "exercise_price",
    "option_style",
    "premium_upfront_indicator",
    "distribution_id",
    "price_factor",
    "days_to_settlement",
    "series_type",
    "protection_flag",
    "automatic_exercise_indicator",
    "specification_code",
    "corporation_name",
    "corporate_action_start_date",
    "custody_treatment_type",
    "market_capitalisation",
    "corporate_governance_level",
]

_DATE_COLUMNS: Final[tuple[str, ...]] = (
    "expiration_date",
    "trading_start_date",
    "trading_end_date",
    "corporate_action_start_date",
)

_INT_COLUMNS: Final[tuple[str, ...]] = (
    "allocation_lot_size",
    "distribution_id",
    "price_factor",
    "days_to_settlement",
    "market_capitalisation",
)

_BOOLEAN_COLUMNS: Final[tuple[str, ...]] = (
    "premium_upfront_indicator",
    "protection_flag",
    "automatic_exercise_indicator",
)


@register_parser("InstrumentsEquities")
def read_instruments_equities(source: str | Path | bytes) -> pd.DataFrame:
    """
    Parse an "InstrumentsEquities" (Cadastro de instrumentos) BDI report.

    File layout, as exported from B3 (';'-separated, UTF-8 with BOM):

        Row 1   : long descriptive paragraph — discarded
        Row 2   : glossary link — discarded
        Row 3   : blank — discarded
        Row 4   : actual column headers — discarded, replaced by canonical names
        Row 5+  : one row per instrument
        Last row: disclaimer footer, not a data row — discarded

    Unlike other BDI reports, this one has a trailing footer row, which
    forces the pandas Python engine (``skipfooter`` isn't supported by
    the faster C engine). Measured negligible (~1s) impact even on the
    full ~165k-row file, so simplicity won over the C engine's speed.

    Every column may contain ``"-"`` as B3's "not applicable" marker,
    parsed here as a universal null across all 30 columns via
    ``na_values``. Some date columns use ``"31/12/9999"`` instead to
    mean "no expiration / still active" — this is a real value, not a
    null, and is preserved as-is (pandas 3.x safely represents dates
    that far out). See ``_common.PERPETUAL_EXPIRY`` for the shared
    sentinel constant used to compare against these dates downstream.

    Parameters
    ----------
    source : str | Path | bytes
        Path to a downloaded CSV, or its raw bytes.

    Returns
    -------
    pd.DataFrame
        One row per instrument. Date columns are datetime64 (with
        ``"31/12/9999"`` preserved as a real, non-null date);
        ``exercise_price`` is float64 (B3's comma-decimal notation,
        e.g. "7,96", handled directly by ``decimal=","`` at read
        time); allocation/distribution/settlement/market-cap columns
        are nullable Int64; the three indicator columns are nullable
        boolean.
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