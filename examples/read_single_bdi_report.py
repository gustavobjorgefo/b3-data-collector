# examples/read_single_bdi_report.py

"""
Example: reading a single downloaded BDI report file.

BDI reports don't share a common layout — header rows, footers, merged
cells, and even column meaning vary per report. Rather than write one
generic reader for all 63 reports in the catalog (most of which aren't
used downstream by this project), this example shows the extensible
pattern the project follows: a small registry mapping a BDI `api_name`
to its own dedicated parser function.

Only one parser is implemented here — BTBLoanBalance ("Empréstimos
registrados"), the report this project actually consumes. Adding support
for another report is a good first contribution for anyone extending
this project:

    1. Write a `read_<report_name>(path) -> pd.DataFrame` function,
       following the same shape as `read_btb_loan_balance` below.
    2. Register it in `_READERS` with its BDI `api_name`
       (see `b3_data_collector.bdi._catalog` for the full report list).

Run this script directly to see it in action against the sample file
in examples/sample_data/.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

_SAMPLE_DATA_DIR = Path(__file__).resolve().parent / "sample_data"


# --- Parser: BTBLoanBalance ("Empréstimos registrados") ---

_BTB_COLUMN_NAMES = [
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


def read_btb_loan_balance(path: str | Path) -> pd.DataFrame:
    """
    Read a "BTBLoanBalance" (Empréstimos registrados) BDI report CSV.

    File layout, as exported from B3 (';'-separated, UTF-8 with BOM):

        Row 1 : long descriptive paragraph — discarded
        Row 2 : glossary link — discarded
        Row 3 : blank — discarded
        Row 4 : merged "Taxa doador" / "Taxa tomador" group labels — discarded
        Row 5 : actual column headers — discarded, replaced by canonical names
        Row 6+: one row per asset, no footer

    Parameters
    ----------
    path : str | Path
        Path to the downloaded CSV.

    Returns
    -------
    pd.DataFrame
        One row per asset. Donor/borrower min/avg/max rates are floats
        (e.g. 0.0292 for "2,92%"); ``trade_date`` is datetime64;
        ``contracts_count`` is nullable Int64 (some assets have zero
        contracts and an empty value in the source file).
    """
    df = pd.read_csv(
        path,
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

_READERS: dict[str, Callable[[str | Path], pd.DataFrame]] = {
    "BTBLoanBalance": read_btb_loan_balance,
}


def read_bdi_report(path: str | Path, report_name: str) -> pd.DataFrame:
    """
    Read any BDI report CSV whose parser is registered.

    Parameters
    ----------
    path : str | Path
        Path to the downloaded CSV.
    report_name : str
        The report's `api_name`, as defined in
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
            "See this module's docstring to add a new one."
        ) from None

    return parser(path)


# --- Demo ---

if __name__ == "__main__":
    # Glob instead of a hardcoded name — sidesteps space/underscore/accent
    # differences between how browsers and scripts may save the filename.
    candidates = list(_SAMPLE_DATA_DIR.glob("Empr*stimos*registrados*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No BTBLoanBalance sample CSV found in {_SAMPLE_DATA_DIR}"
        )
    sample_path = candidates[0]

    df = read_bdi_report(sample_path, report_name="BTBLoanBalance")

    print(f"File: {sample_path.name}")
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns\n")
    print(df.dtypes)
    print()
    print(df.head())