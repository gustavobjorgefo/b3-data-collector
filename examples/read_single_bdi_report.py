# examples/read_single_bdi_report.py

"""
Example: reading a single downloaded BDI report file.

The actual parsing logic — the registry mapping a BDI ``api_name`` to its
own dedicated parser function — lives in the library itself
(``b3_data_collector.bdi._parsers``), since the reader subpackage needs
the exact same parsers to turn S3 bytes into DataFrames. This script is
just a thin demonstration of calling that shared parsing engine against
a local file.

Only one parser is implemented so far — BTBLoanBalance ("Empréstimos
registrados"), the report this project actually consumes. Adding support
for another report is a good first contribution for anyone extending
this project — see ``b3_data_collector.bdi._parsers`` for how to add one.

Run this script directly to see it in action against the sample file
in examples/sample_data/.
"""

from __future__ import annotations

from pathlib import Path

from b3_data_collector.bdi._parsers import read_bdi_report_file

_SAMPLE_DATA_DIR = Path(__file__).resolve().parent / "sample_data"


if __name__ == "__main__":
    # Glob instead of a hardcoded name — sidesteps space/underscore/accent
    # differences between how browsers and scripts may save the filename.
    candidates = list(_SAMPLE_DATA_DIR.glob("Empr*stimos*registrados*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No BTBLoanBalance sample CSV found in {_SAMPLE_DATA_DIR}"
        )
    sample_path = candidates[0]

    df = read_bdi_report_file(sample_path, report_name="BTBLoanBalance")

    print(f"File: {sample_path.name}")
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns\n")
    print(df.dtypes)
    print()
    print(df.head())