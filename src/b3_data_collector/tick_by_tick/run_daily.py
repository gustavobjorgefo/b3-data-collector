# b3_data_collector/tick_by_tick/run_daily.py

"""
Daily scheduled entrypoint for the B3 tick-data ingestion pipeline.
...
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Final

_SRC_DIR: Final[Path] = Path(__file__).resolve().parents[2]
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from b3_data_collector.paths import LOGS_DIR
from b3_data_collector.tick_by_tick._feed import FeedType
from b3_data_collector.tick_by_tick.pipeline import run_pipeline

# --- Configuration ---

UPLOAD_TO_S3 : Final[bool] = True
OVERWRITE    : Final[bool] = False


# --- Logging setup ---

def _configure_logging() -> None:
    log_file  = LOGS_DIR / "b3_pipeline.log"
    formatter = logging.Formatter(
        fmt     = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        filename    = log_file,
        maxBytes    = 10 * 1024 * 1024,
        backupCount = 30,
        encoding    = "utf-8",
    )
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)


# --- Entrypoint ---

def main() -> None:
    _configure_logging()

    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("b3-data-collector — Tick-by-Tick Daily Pipeline starting")
    logger.info("=" * 60)

    trade_date = date.today() - timedelta(days=1)
    logger.info("Target date: %s", trade_date)

    # --- RV feed ---
    logger.info("--- Feed: %s ---", FeedType.RV.config.label)
    result_rv = run_pipeline(
        dates        = trade_date,
        feed         = FeedType.RV,
        upload_to_s3 = UPLOAD_TO_S3,
        overwrite    = OVERWRITE,
    )

    # --- DERIV feed ---
    logger.info("--- Feed: %s ---", FeedType.DERIV.config.label)
    result_deriv = run_pipeline(
        dates        = trade_date,
        feed         = FeedType.DERIV,
        upload_to_s3 = UPLOAD_TO_S3,
        overwrite    = OVERWRITE,
    )

    # Both summaries are already logged inside run_pipeline() (via
    # PipelineResult.summary). Exit non-zero if any feed had failures —
    # lets the scheduler flag failed runs at the OS level.
    any_failed = result_rv.failed > 0 or result_deriv.failed > 0
    if any_failed:
        logger.error(
            "Pipeline completed with failures — RV: %d  DERIV: %d",
            result_rv.failed, result_deriv.failed,
        )
        sys.exit(1)

    logger.info("Pipeline completed successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()