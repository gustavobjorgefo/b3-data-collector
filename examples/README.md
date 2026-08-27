# Examples

Two small, self-contained scripts showing how to use `b3_data_collector`
directly — reading a BDI report CSV and running the tick-by-tick pipeline
stages on a sample file. Both run against real B3-format data (trimmed for
size), not synthetic/hand-written samples.

## Setup

Run these from the repository root, with the project installed in your
virtual environment (`pip install -e .` — see the main [README](../README.md)
if you haven't done this yet).

## `read_single_bdi_report.py`

Reads a real BDI report CSV: **BTBLoanBalance** ("Empréstimos registrados"),
which lists securities-lending rates (minimum, average, maximum) for donors
and borrowers, per asset.

```bash
python examples/read_single_bdi_report.py
```

BDI reports don't share a common layout — headers, footers, and column
meaning vary per report. Rather than one generic parser for all 63 reports
in the catalog, the library uses a small registry pattern in
`b3_data_collector.bdi.parsers`: one dedicated parser function per report,
looked up by its `api_name`. Only `BTBLoanBalance` is implemented so far,
since it's the only one this project currently consumes downstream. This
script is just a thin demonstration — it calls that shared parsing engine
against a local file; the `reader` subpackage (see the main
[README](../README.md)) calls the exact same parsers against bytes
downloaded from S3.

**Want to add support for another report?** Create  
`src/b3_data_collector/bdi/parsers/_<report_name>.py`, write a
`read_<report_name>(source) -> pd.DataFrame` function decorated with
`@register_parser("<ApiName>")`, following the same shape as
`_btb_loan_balance.py`, and import the module in `bdi/parsers/__init__.py`.
That's a good first contribution — see the full report catalog in
`src/b3_data_collector/bdi/_catalog.py`.

**Expected output:** a DataFrame with 195 rows, 14 columns — asset code,
ISIN, market, contract counts, and donor/borrower rates as floats (e.g.
`0.0292` for `2,92%`).

## `read_tick_by_tick_parquet.py`

Runs the real extraction and partitioning stages — the same code
`run_pipeline()` uses internally — on a sample ZIP, then reads and previews
the resulting tick-level Parquet file.

```bash
python examples/read_tick_by_tick_parquet.py
```

Unlike BDI reports, both tick-by-tick feeds (RV and DERIV) share a single,
well-defined schema, so no per-report registry is needed here — the example
just calls the package's own pipeline stages directly.

All intermediate and output files are written to a temporary directory,
cleaned up automatically when the script finishes. Nothing is written to
`examples/sample_data/` or the project's `data/` folder.

**Expected output:** 20 rows, 11 columns — symbol, timestamp, price,
quantity, and broker codes, extracted and partitioned from
`sample_data/sample_rv.zip`.

## `sample_data/`

Static, versioned sample files used by both examples:

| File | Used by | Source |
|---|---|---|
| `Empréstimos registrados-30-06-2026.csv` | `read_single_bdi_report.py` | Real BDI download, trimmed to 200 rows |
| `sample_rv.zip` | `read_tick_by_tick_parquet.py` | Real B3 RV download, trimmed to 20 rows via `scripts/build_test_fixture.py` |
| `sample_deriv.zip` | *(not yet used in an example — available for a DERIV-based contribution)* | Real B3 DERIV download, trimmed to 20 rows |

These are the same trimmed fixtures used in the test suite (`tests/fixtures/`) — kept here as well so the examples are runnable on their own, without depending on `tests/`.