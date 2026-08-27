# src/b3_data_collector/bdi/parsers/_btb_loan_balance.py

"""Parser for BTBLoanBalance ("Empréstimos registrados")."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Final

import pandas as pd

from ._common import parse_percentage
from ._registry import register_parser

_COLUMN_NAMES: Final[list[str]] = [
    "trade_date",
    "if_code",
    "isin_code",
    "company_name",
    "market",
    "contracts_count",
    "assets_quantity",
    "value_brl",
    "donor_rate_min",
    "donor_rate_avg",
    "donor_rate_max",
    "borrower_rate_min",
    "borrower_rate_avg",
    "borrower_rate_max",
]

_RATE_COLUMNS: Final[tuple[str, ...]] = (
    "donor_rate_min",
    "donor_rate_avg",
    "donor_rate_max",
    "borrower_rate_min",
    "borrower_rate_avg",
    "borrower_rate_max",
)


@register_parser("BTBLoanBalance")
def read_btb_loan_balance(source: str | Path | bytes) -> pd.DataFrame:
    """
    Parse a "BTBLoanBalance" (Empréstimos registrados) BDI report.

    File layout, as exported from B3 (';'-separated, UTF-8 with BOM):

        Row 1 : long descriptive paragraph — discarded
        Row 2 : glossary link — discarded
        Row 3 : blank — discarded
        Row 4 : merged "Taxa doador" / "Taxa tomador" group labels — discarded
        Row 5 : actual column headers — discarded, replaced by canonical names
        Row 6+: one row per asset, no footer

    Parameters
    ----------
    source : str | Path | bytes
        Path to a downloaded CSV, or its raw bytes (e.g. as fetched from
        S3 or the BDI export endpoint).

    Returns
    -------
    pd.DataFrame
        One row per asset. Donor/borrower min/avg/max rates are floats
        (e.g. 0.0292 for "2,92%"); ``trade_date`` is datetime64;
        ``contracts_count`` is nullable Int64 (some assets have zero
        contracts and an empty value in the source file).
    """
    buffer = io.BytesIO(source) if isinstance(source, bytes) else source

    df = pd.read_csv(
        buffer,
        sep=";",
        skiprows=5,
        header=None,
        names=_COLUMN_NAMES,
        encoding="utf-8-sig",
        decimal=",",
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%d/%m/%Y")
    df["contracts_count"] = df["contracts_count"].astype("Int64")

    for col in _RATE_COLUMNS:
        df[col] = parse_percentage(df[col])

    return df