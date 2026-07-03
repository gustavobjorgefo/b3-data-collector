# scripts/build_test_fixture.py

"""
Build a small, real-format ZIP fixture from a full B3 tick-by-tick download.

Takes a real daily ZIP (as downloaded from B3) and produces a much smaller
ZIP containing the same internal TXT filename, the same first line (the
metadata/count line that _extractor.py skips via `skiprows=1`), and a
handful of real data rows after it.

This preserves the exact on-disk format B3 uses (separator, encoding,
column order, price/time formatting) so tests exercise the real parsing
logic — not a hand-written approximation of it.

Usage
-----
    python scripts/build_test_fixture.py \
        --input "data/tick_by_tick/rv/downloads/30-06-2026_NEGOCIOSAVISTA_RV.zip" \
        --output tests/fixtures/sample_rv.zip \
        --lines 20

Not part of the installable package — a one-off developer tool.
"""

from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path


def _find_txt_name(archive: zipfile.ZipFile) -> str:
    """
    Return the single .txt filename inside the archive.

    Raises
    ------
    ValueError
        If zero or more than one .txt file is found — in the latter case,
        the caller should inspect the archive manually and adjust.
    """
    txt_names = [name for name in archive.namelist() if name.endswith(".txt")]

    if not txt_names:
        raise ValueError("No .txt file found inside the ZIP.")

    if len(txt_names) > 1:
        raise ValueError(
            f"Expected exactly one .txt file, found {len(txt_names)}: {txt_names}. "
            "Inspect the archive and adjust this script if needed."
        )

    return txt_names[0]


def build_fixture(input_zip: Path, output_zip: Path, num_lines: int) -> None:
    """
    Trim a real B3 ZIP down to a small sample and write it to disk.

    Parameters
    ----------
    input_zip : Path
        Path to the full, real ZIP downloaded from B3.
    output_zip : Path
        Path where the trimmed sample ZIP will be written.
    num_lines : int
        Number of data rows to keep, in addition to the first line
        (the metadata/count line the extractor skips via `skiprows=1`).
    """
    with zipfile.ZipFile(input_zip) as archive:
        txt_name = _find_txt_name(archive)
        print(f"Found TXT inside archive: {txt_name}")

        with archive.open(txt_name) as raw_file:
            text_stream = io.TextIOWrapper(raw_file, encoding="latin1", newline="")

            # First line is metadata (row count, etc.) — _extractor.py skips
            # it via skiprows=1, but we keep it in the fixture for fidelity.
            first_line = text_stream.readline()

            data_lines = []
            for _ in range(num_lines):
                line = text_stream.readline()
                if not line:
                    print(
                        f"Warning: source file has fewer than {num_lines} "
                        "data lines — using all available."
                    )
                    break
                data_lines.append(line)

    trimmed_content = (first_line + "".join(data_lines)).encode("latin1")

    output_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as out_archive:
        out_archive.writestr(txt_name, trimmed_content)

    print(f"Wrote {len(data_lines)} data line(s) + 1 header line to: {output_zip}")
    print(f"Sample ZIP size: {output_zip.stat().st_size:,} bytes")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Path to the real B3 ZIP.")
    parser.add_argument("--output", required=True, type=Path, help="Path for the trimmed sample ZIP.")
    parser.add_argument("--lines", type=int, default=20, help="Number of data rows to keep (default: 20).")
    args = parser.parse_args()

    build_fixture(args.input, args.output, args.lines)


if __name__ == "__main__":
    main()