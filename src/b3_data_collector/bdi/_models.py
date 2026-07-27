# src\b3_data_collector\bdi\_models.py

"""
Result models for the BDI reports ingestion pipeline.

Defines the structured return types used by the pipeline orchestrator
and any caller — including the notification layer.

The granularity here is per-report per-date, not per-date as in the
tick-by-tick pipeline, because each BDI run downloads many reports for
the same date.

Nothing in this module performs I/O or imports third-party libraries.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto

# --- Report status ---

class ReportStatus(Enum):
    """
    Outcome of downloading a single BDI report for one trading date.

    Attributes
    ----------
    SUCCESS :
        Report downloaded and uploaded to S3 successfully.
    SKIPPED :
        Object already exists in S3 and ``overwrite=False``.
    UNAVAILABLE :
        B3 returned 404 or empty content for this report and date.
    FAILED :
        Request or upload raised an exception; see ``ReportResult.error``.
    """

    SUCCESS     = auto()
    SKIPPED     = auto()
    UNAVAILABLE = auto()
    FAILED      = auto()


# --- Per-report result ---

@dataclass
class ReportResult:
    """
    Outcome of downloading a single BDI report for one trading date.

    Parameters
    ----------
    report_name : str
        API name of the report (e.g. ``"DailyAverageStocks"``).
    trade_date : date
        The trading date that was processed.
    status : ReportStatus
        Outcome of the download + upload sequence.
    error : str | None
        Human-readable error message, or ``None`` on success.
    """

    report_name : str
    trade_date  : date
    status      : ReportStatus = ReportStatus.SKIPPED
    error       : str | None   = None

    @property
    def succeeded(self) -> bool:
        """True if status is not FAILED."""
        return self.status is not ReportStatus.FAILED

    @property
    def summary_line(self) -> str:
        """Single-line summary for logging."""
        return (
            f"{self.trade_date}  {self.report_name:<45}"
            f"  [{self.status.name}]"
            + (f"  — {self.error}" if self.error else "")
        )


# --- Aggregate pipeline result ---

@dataclass
class BdiPipelineResult:
    """
    Aggregate outcome of a BDI pipeline run across one or more dates.

    Parameters
    ----------
    results : list[ReportResult]
        Per-report outcomes, in processing order.
    started_at : float
        Monotonic timestamp when the pipeline started.
    finished_at : float
        Monotonic timestamp when the pipeline finished.
    """

    results     : list[ReportResult] = field(default_factory=list)
    started_at  : float              = field(default_factory=time.monotonic)
    finished_at : float              = 0.0

    @property
    def duration_seconds(self) -> float:
        return self.finished_at - self.started_at

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.status is ReportStatus.SUCCESS)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status is ReportStatus.SKIPPED)

    @property
    def unavailable(self) -> int:
        return sum(1 for r in self.results if r.status is ReportStatus.UNAVAILABLE)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status is ReportStatus.FAILED)

    @property
    def failed_results(self) -> list[ReportResult]:
        return [r for r in self.results if r.status is ReportStatus.FAILED]

    @property
    def summary(self) -> str:
        """Multi-line human-readable summary for logging and e-mail body."""
        lines = [
            "=" * 60,
            "ibexQuant — B3 BDI Reports Pipeline Summary",
            "=" * 60,
            f"  Reports processed : {self.total}",
            f"  Succeeded         : {self.succeeded}",
            f"  Skipped           : {self.skipped}",
            f"  Unavailable       : {self.unavailable}",
            f"  Failed            : {self.failed}",
            f"  Duration          : {self.duration_seconds:.1f}s",
            "-" * 60,
        ]
        for result in self.results:
            lines.append(f"  {result.summary_line}")
        if self.failed_results:
            lines.append("-" * 60)
            lines.append("  FAILURES:")
            for result in self.failed_results:
                lines.append(f"    [{result.trade_date}] {result.report_name}: {result.error}")
        lines.append("=" * 60)
        return "\n".join(lines)