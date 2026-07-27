# b3_data_collector/bdi/run_daily.py

"""
Daily scheduled entrypoint for the BDI reports ingestion pipeline.
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
from b3_data_collector.bdi.pipeline import run_bdi_pipeline


def _configure_logging() -> None:
    log_file  = LOGS_DIR / "b3_bdi_pipeline.log"
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


def main() -> None:
    _configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("b3-data-collector — BDI Reports Pipeline starting")
    logger.info("=" * 60)

    trade_date = date.today() - timedelta(days=1)
    logger.info("Target date: %s", trade_date)

    result = run_bdi_pipeline(dates=trade_date)

    if result.failed > 0:
        logger.error("Pipeline completed with %d failure(s).", result.failed)
        sys.exit(1)

    logger.info("Pipeline completed successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()