# src/b3_data_collector/bdi/parsers/_common.py

"""Shared parsing helpers reused across individual report parsers."""

from __future__ import annotations

from datetime import date
from typing import Final

import pandas as pd

# Sentinel expiry/end date BDI uses for instruments with no fixed maturity
# or closing date (e.g. common equities, ETFs still trading). Not applied
# automatically by any parser here — exposed as one canonical constant so
# downstream code can compare against it (e.g. to flag "still open"
# instruments) instead of hardcoding "9999-12-31" in multiple places.
PERPETUAL_EXPIRY: Final[date] = date(9999, 12, 31)


def parse_percentage(series: pd.Series) -> pd.Series:
    """
    Convert a '2,92%'-style string column to float.

    Parameters
    ----------
    series : pd.Series
        Column of percentage strings using comma as decimal separator
        (e.g. ``"2,92"`` or ``"2,92%"``).

    Returns
    -------
    pd.Series
        Float64 column, e.g. ``2,92`` -> ``0.0292``.
    """
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype("float64")
        / 100
    )