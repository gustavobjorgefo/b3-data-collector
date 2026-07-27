# src/b3_data_collector/bdi/_parsers.py

"""
BDI report parsers.

BDI reports don't share a common layout — header rows, footers, merged
cells, and even column meaning vary per report. Rather than write one
generic reader for all 63 reports in the catalog (most of which aren't
used downstream by this project), this module follows an extensible
registry pattern: one dedicated parser function per report, looked up
by its BDI ``api_name``.

Parsers here take raw bytes (or a file path) and return a DataFrame —
they perform no I/O of their own and know nothing about where the bytes
came from. This keeps them reusable by both the reader (fetching bytes
from S3) and any local/example script (reading bytes from disk).

Only one parser is implemented so far — ``BTBLoanBalance`` ("Empréstimos
registrados"), the report this project currently consumes downstream.

Adding support for another report is the main way this module grows:

    1. Write a ``read_<report_name>(source) -> pd.DataFrame`` function,
       following the same shape as ``read_btb_loan_balance`` below.
    2. Register it in ``_READERS`` with its BDI ``api_name``
       (see ``b3_data_collector.bdi._catalog`` for the full report list).
"""

from __future__ import annotations

import io
from collections.abc import Callable
from pathlib import Path

import pandas as pd

# --- Parser: BTBLoanBalance ("Empréstimos registrados") ---

_BTB_COLUMN_NAMES: list[str] = [
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


def _parse_percentage(series: pd.Series) -> pd.Series:
    """Convert a '2,92%'-style string column to float (e.g. 0.0292)."""
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype("float64")
        / 100
    )


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
        names=_BTB_COLUMN_NAMES,
        encoding="utf-8-sig",
        decimal=",",
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%d/%m/%Y")
    df["contracts_count"] = df["contracts_count"].astype("Int64")

    rate_columns = (
        "donor_rate_min", "donor_rate_avg", "donor_rate_max",
        "borrower_rate_min", "borrower_rate_avg", "borrower_rate_max",
    )
    for col in rate_columns:
        df[col] = _parse_percentage(df[col])

    return df


# --- Registry: BDI api_name -> parser function ---

_READERS: dict[str, Callable[[str | Path | bytes], pd.DataFrame]] = {
    "BTBLoanBalance": read_btb_loan_balance,
}


def read_bdi_report_file(source: str | Path | bytes, report_name: str) -> pd.DataFrame:
    """
    Parse any BDI report whose parser is registered.

    Parameters
    ----------
    source : str | Path | bytes
        Path to a downloaded CSV, or its raw bytes.
    report_name : str
        The report's ``api_name``, as defined in
        ``b3_data_collector.bdi._catalog`` (e.g. ``"BTBLoanBalance"``).

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    NotImplementedError
        If no parser is registered yet for ``report_name``. See this
        module's docstring for how to add one.
    """
    try:
        parser = _READERS[report_name]
    except KeyError:
        raise NotImplementedError(
            f"No parser registered for report '{report_name}'. "
            f"Currently supported: {sorted(_READERS)}. "
            "See b3_data_collector.bdi._parsers docstring to add a new one."
        ) from None

    return parser(source)