# src\b3_data_collector\tick_by_tick\_contract.py

"""
Contract of the tick-by-tick ticks Parquet published to S3.

Single source of truth for what consumers of the ticks Parquet can rely
on: the column order and Arrow types, the time zone of ``timestamp``, the
schema version, and the provenance metadata stored in the Parquet footer.
Both feeds (RV and DERIV) share this contract. It is documented for
consumers in ``docs/tick_by_tick_parquet_contract.md``; the tests check
the files the pipeline produces against this module.

The provenance travels in two steps, because the raw Parquet is deleted
as soon as the ticks Parquet is written:

1. The extractor stores a ``SourceProvenance`` in the raw Parquet footer.
2. The partitioner reads it back, adds what only it knows (row count,
   unparseable timestamps, collector build) and stores a
   ``TicksProvenance`` in the ticks Parquet footer.

Does not parse the TXT, build timestamps or perform any I/O beyond
``git rev-parse`` for the collector commit.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import cache
from typing import Final

import pyarrow as pa

from .. import __version__
from ..common import B3_TIMEZONE
from ..paths import ROOT_DIR

# --- Module constants ---

SCHEMA_VERSION : Final[int]   = 1
TIMEZONE       : Final[str]   = B3_TIMEZONE.key
METADATA_KEY   : Final[bytes] = b"b3_data_collector"

# Order is part of the contract. Only ``timestamp`` (unparseable time) and
# the broker codes (anonymous trades) may be null.
OUTPUT_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field("source_row",     pa.int32(),                       nullable=False),
    pa.field("symbol",         pa.string(),                      nullable=False),
    pa.field("trade_date",     pa.string(),                      nullable=False),
    pa.field("reference_date", pa.string(),                      nullable=False),
    pa.field("timestamp",      pa.timestamp("ms", tz=TIMEZONE),  nullable=True),
    pa.field("time_raw",       pa.string(),                      nullable=False),
    pa.field("price",          pa.float64(),                     nullable=False),
    pa.field("quantity",       pa.uint32(),                      nullable=False),
    pa.field("update_action",  pa.uint8(),                       nullable=False),
    pa.field("session_type",   pa.uint8(),                       nullable=False),
    pa.field("channel_type",   pa.uint8(),                       nullable=False),
    pa.field("trade_id",       pa.int64(),                       nullable=False),
    pa.field("buyer_broker",   pa.string(),                      nullable=True),
    pa.field("seller_broker",  pa.string(),                      nullable=True),
])

_GIT_TIMEOUT_SECONDS: Final[int] = 5


# --- Errors ---

class ContractViolationError(ValueError):
    """
    A source file or an intermediate artifact breaks the tick contract.

    Subclasses ``ValueError`` so the pipeline's existing per-stage error
    handling marks the date as failed without special-casing it.
    """


# --- Provenance ---

@dataclass(frozen=True)
class SourceProvenance:
    """
    Where a day of ticks came from, as recorded by the extractor.

    Parameters
    ----------
    feed : str
        Feed name (``"RV"`` or ``"DERIV"``).
    trade_date : str
        Trading date the file refers to, ``YYYY-MM-DD``.
    source_zip_name : str
        File name of the archived B3 ZIP.
    source_zip_sha256 : str
        Hex SHA-256 of the ZIP bytes.
    source_txt_name : str
        Name of the TXT inside the ZIP.
    source_txt_row_count : int
        Data lines in the TXT: header and blank lines excluded.
    """

    feed                 : str
    trade_date           : str
    source_zip_name      : str
    source_zip_sha256    : str
    source_txt_name      : str
    source_txt_row_count : int

    @classmethod
    def from_schema(cls, schema: pa.Schema) -> SourceProvenance:
        """
        Read the provenance stored by the extractor in a Parquet schema.

        Parameters
        ----------
        schema : pa.Schema
            Schema of the raw Parquet, with its footer metadata.

        Returns
        -------
        SourceProvenance

        Raises
        ------
        ContractViolationError
            If the metadata is missing or incomplete, e.g. a raw Parquet
            left behind by a collector version older than this contract.
        """
        payload = (schema.metadata or {}).get(METADATA_KEY)
        if payload is None:
            raise ContractViolationError(
                "Raw Parquet has no provenance metadata; it predates the tick "
                "contract. Re-run the extraction with overwrite=True."
            )
        try:
            return cls(**json.loads(payload))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ContractViolationError(
                f"Raw Parquet has malformed provenance metadata: {exc}"
            ) from exc


@dataclass(frozen=True)
class TicksProvenance(SourceProvenance):
    """
    Full provenance stored in the footer of a published ticks Parquet.

    Parameters
    ----------
    schema_version : int
        Version of this contract.
    row_count : int
        Rows in the ticks Parquet; always equal to ``source_txt_row_count``.
    timestamp_null_count : int
        Rows whose ``time_raw`` could not be turned into a timestamp.
    timezone : str
        IANA time zone of ``timestamp``.
    collector_version : str
        Version of the ``b3-data-collector`` package.
    collector_commit : str | None
        Git commit the collector ran from, suffixed ``+dirty`` when tracked
        files had local changes; ``None`` outside a git checkout.
    created_at_utc : str
        ISO-8601 UTC time the file was written.

    Other parameters are inherited from ``SourceProvenance``.
    """

    schema_version       : int
    row_count            : int
    timestamp_null_count : int
    timezone             : str
    collector_version    : str
    collector_commit     : str | None
    created_at_utc       : str

    @classmethod
    def build(
        cls,
        source               : SourceProvenance,
        row_count            : int,
        timestamp_null_count : int,
    ) -> TicksProvenance:
        """
        Complete the extractor's provenance with the partitioner's facts.

        Parameters
        ----------
        source : SourceProvenance
            Provenance read from the raw Parquet.
        row_count : int
            Rows written to the ticks Parquet.
        timestamp_null_count : int
            Rows with a null ``timestamp``.

        Returns
        -------
        TicksProvenance
        """
        return cls(
            **asdict(source),
            schema_version       = SCHEMA_VERSION,
            row_count            = row_count,
            timestamp_null_count = timestamp_null_count,
            timezone             = TIMEZONE,
            collector_version    = __version__,
            collector_commit     = collector_commit(),
            created_at_utc       = datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )


def with_provenance(table: pa.Table, provenance: SourceProvenance) -> pa.Table:
    """
    Return ``table`` with ``provenance`` added to its schema metadata.

    Existing metadata (such as pandas' own) is kept.

    Parameters
    ----------
    table : pa.Table
        Table about to be written.
    provenance : SourceProvenance
        A ``SourceProvenance`` or ``TicksProvenance``.

    Returns
    -------
    pa.Table
    """
    payload = json.dumps(asdict(provenance)).encode("utf-8")
    return table.replace_schema_metadata({**(table.schema.metadata or {}), METADATA_KEY: payload})


# --- Collector build ---

def _run_git(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd            = ROOT_DIR,
        capture_output = True,
        text           = True,
        timeout        = _GIT_TIMEOUT_SECONDS,
        check          = False,
    )


@cache
def collector_commit() -> str | None:
    """
    Return the git commit the collector is running from.

    Computed once per process. Only a git checkout of this repository
    counts: a non-editable install lives elsewhere, and asking git from
    there could report an unrelated repository.

    Returns
    -------
    str | None
        Commit hash, suffixed ``+dirty`` when tracked files differ from
        it, or ``None`` when it cannot be determined.
    """
    if not (ROOT_DIR / ".git").exists():
        return None
    try:
        head = _run_git("rev-parse", "HEAD")
        if head.returncode != 0:
            return None
        dirty = _run_git("diff", "--quiet", "HEAD").returncode == 1
    except (OSError, subprocess.TimeoutExpired):
        return None
    commit = head.stdout.strip()
    return f"{commit}+dirty" if dirty else commit
