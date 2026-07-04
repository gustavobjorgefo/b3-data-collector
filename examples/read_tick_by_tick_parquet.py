# examples/read_tick_by_tick_parquet.py

"""
Example: running the extraction + partitioning stages on a sample ZIP,
then reading the resulting tick-level Parquet file.

Unlike the BDI reports (examples/read_single_bdi_report.py), tick-by-tick
data already has a single, well-defined schema shared by both feeds (RV
and DERIV) — so there's no need for a per-report parser registry here.
This example simply calls the package's own pipeline stages directly,
the same code path run_pipeline() uses internally.

Uses examples/sample_data/sample_rv.zip — the same small, real-format
fixture used in the test suite (see tests/fixtures/ and
scripts/build_test_fixture.py for how it was built from a real B3 download).

All intermediate/output files are written to a temporary directory,
cleaned up automatically when the script finishes — examples/sample_data/
stays as a clean, static set of source files, nothing generated lingers.
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd

from b3_data_collector import paths
from b3_data_collector.tick_by_tick import _extractor, _partitioner
from b3_data_collector.tick_by_tick._feed import FeedType

_SAMPLE_DATA_DIR = Path(__file__).resolve().parent / "sample_data"
_SAMPLE_TRADE_DATE = date(2026, 6, 30)  # matches the date baked into sample_rv.zip


def main() -> None:
    feed = FeedType.RV
    cfg = feed.config

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        downloads_dir = tmp_dir / "downloads"
        raw_dir = tmp_dir / "raw"
        ticks_dir = tmp_dir / "ticks"
        for d in (downloads_dir, raw_dir, ticks_dir):
            d.mkdir()

        # Redirect PATHS_B3 to the temp directory for this run, so the
        # extractor/partitioner (which read this shared dict internally)
        # read/write there instead of the project's real data/ folder.
        paths.PATHS_B3[cfg.paths_key_downloads] = downloads_dir
        paths.PATHS_B3[cfg.paths_key_raw]       = raw_dir
        paths.PATHS_B3[cfg.paths_key_ticks]     = ticks_dir

        # The extractor looks for a ZIP named per zip_name_template — copy
        # the static sample fixture there under that exact expected name.
        expected_zip_name = cfg.zip_name_template.format(date=_SAMPLE_TRADE_DATE)
        shutil.copy(_SAMPLE_DATA_DIR / "sample_rv.zip", downloads_dir / expected_zip_name)

        # --- Stage 1: Extract (ZIP -> normalised raw Parquet) ---
        print(f"Extracting {expected_zip_name} ...")
        extract_status = _extractor.extract_to_raw_parquet(
            trade_date=_SAMPLE_TRADE_DATE, feed=feed
        )
        print(f"  extract status: {extract_status.name}")

        # --- Stage 2: Partition (raw Parquet -> tick-level Parquet) ---
        print("Partitioning to tick-level Parquet ...")
        partition_status, tick_count = _partitioner.partition_to_ticks(
            trade_date=_SAMPLE_TRADE_DATE, feed=feed
        )
        print(f"  partition status: {partition_status.name}, ticks: {tick_count}")

        # --- Read and preview the final result (still inside the temp dir) ---
        ticks_file = ticks_dir / f"{_SAMPLE_TRADE_DATE}.parquet"
        df = pd.read_parquet(ticks_file)

        print(f"\n{len(df)} rows, {len(df.columns)} columns\n")
        print(df.dtypes)
        print()
        print(df.head())
    # tmp directory and everything in it is deleted automatically here


if __name__ == "__main__":
    main()