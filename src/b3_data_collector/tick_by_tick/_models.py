# b3_data_collector/tick_by_tick/_models.py

"""
Pipeline result models for the B3 tick-by-tick ingestion pipeline.

Tracks, per trading date, the outcome of each of the four pipeline
stages (download, extract, partition, and the two S3 uploads — raw ZIP
archive and processed ticks Parquet) plus the resulting tick count.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date

from ..common import StageStatus

__all__ = ["DateResult", "PipelineResult", "StageStatus"]


# --- Per-date result ---

@dataclass
class DateResult:
    """
    Outcome of the full pipeline for a single trading date.

    Parameters
    ----------
    trade_date : date
        The trading date that was processed.
    download : StageStatus
        Outcome of the ZIP download stage.
    extract : StageStatus
        Outcome of the raw Parquet extraction stage.
    partition : StageStatus
        Outcome of the ticks Parquet partitioning stage.
    s3_upload : StageStatus
        Outcome of uploading the raw ZIP archive to S3.
    ticks_s3_upload : StageStatus
        Outcome of uploading the processed ticks Parquet to S3.
    tick_count : int | None
        Number of ticks written, or ``None`` if partitioning did not
        complete successfully.
    error : str | None
        Human-readable error message, or ``None`` on success.
    """

    trade_date      : date
    download        : StageStatus = StageStatus.SKIPPED
    extract         : StageStatus = StageStatus.SKIPPED
    partition        : StageStatus = StageStatus.SKIPPED
    s3_upload        : StageStatus = StageStatus.SKIPPED
    ticks_s3_upload  : StageStatus = StageStatus.SKIPPED
    tick_count       : int | None  = None
    error            : str | None  = None

    @property
    def succeeded(self) -> bool:
        return self.error is None

    @property
    def summary_line(self) -> str:
        status = "OK" if self.succeeded else "FAILED"
        ticks  = f"{self.tick_count:,}" if self.tick_count is not None else "—"
        return (
            f"{self.trade_date}  [{status}]"
            f"  download={self.download.name}"
            f"  extract={self.extract.name}"
            f"  partition={self.partition.name}"
            f"  s3_zip={self.s3_upload.name}"
            f"  s3_ticks={self.ticks_s3_upload.name}"
            f"  ticks={ticks}"
        )


# --- Aggregate pipeline result ---

@dataclass
class PipelineResult:
    """
    Aggregate outcome of a tick-by-tick pipeline run across one or more
    trading dates.

    Parameters
    ----------
    results : list[DateResult]
        Per-date outcomes, in processing order.
    started_at : float
        Monotonic timestamp when the pipeline started.
    finished_at : float
        Monotonic timestamp when the pipeline finished.
    """

    results     : list[DateResult] = field(default_factory=list)
    started_at  : float            = field(default_factory=time.monotonic)
    finished_at : float            = 0.0

    @property
    def duration_seconds(self) -> float:
        return self.finished_at - self.started_at

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.succeeded)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.succeeded)

    @property
    def unavailable(self) -> int:
        return sum(
            1 for r in self.results
            if r.download is StageStatus.UNAVAILABLE
        )

    @property
    def failed_dates(self) -> list[DateResult]:
        return [r for r in self.results if not r.succeeded]

    @property
    def summary(self) -> str:
        lines = [
            "=" * 60,
            "b3-data-collector — Tick-by-Tick Pipeline Run Summary",
            "=" * 60,
            f"  Dates processed : {self.total}",
            f"  Succeeded       : {self.succeeded}",
            f"  Failed          : {self.failed}",
            f"  Unavailable     : {self.unavailable}",
            f"  Duration        : {self.duration_seconds:.1f}s",
            "-" * 60,
        ]
        for result in self.results:
            lines.append(f"  {result.summary_line}")
        if self.failed_dates:
            lines.append("-" * 60)
            lines.append("  FAILURES:")
            for result in self.failed_dates:
                lines.append(f"    {result.trade_date}: {result.error}")
        lines.append("=" * 60)
        return "\n".join(lines)