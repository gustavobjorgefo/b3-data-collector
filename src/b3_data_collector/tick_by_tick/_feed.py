# src\b3_data_collector\tick_by_tick\_feed.py

"""
B3 tick-data feed type definitions.

A ``FeedType`` enum entry is the single source of truth for every piece
of configuration that differs between the equities (RV) and derivatives
(DERIV) intraday feeds:

- Download URL template
- Local ZIP filename template  (format receives a ``date`` object)
- TXT filename prefix inside the ZIP  (same convention)
- S3 key prefix for the raw ZIP archive
- S3 key prefix for the processed ticks Parquet
- Local directory keys in ``PATHS_B3``

Both ZIPs contain a single TXT whose name follows the ``DD-MM-YYYY``
date format, confirmed from live files:

    RV    ZIP : YYYYMMDD_NEGOCIOSAVISTA.zip
          TXT : DD-MM-YYYY_NEGOCIOSAVISTA_RV.txt

    DERIV ZIP : DD-MM-YYYY_NEGOCIOSAVISTA_DRV.zip
          TXT : DD-MM-YYYY_NEGOCIOSAVISTA_DRV.txt

The processed ticks Parquet is partitioned Hive-style by year, mirroring
the convention used by ``bdi/_uploader.py``:

    b3/tick_by_tick/{rv|deriv}/ticks/year={YYYY}/{YYYY-MM-DD}.parquet

Nothing in this module performs I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Final


# --- Feed configuration ---

@dataclass(frozen=True)
class FeedConfig:
    """
    Immutable configuration for a single B3 tick-data feed.

    Parameters
    ----------
    url_template : str
        Download URL with a ``{date}`` placeholder (``YYYY-MM-DD`` format).
    zip_name_template : str
        Local ZIP filename with a ``{date}`` placeholder.  The format
        string receives a ``date`` object — use ``strftime``-style
        directives (e.g. ``{date:%Y%m%d}`` or ``{date:%d-%m-%Y}``).
    txt_prefix_template : str
        Prefix of the TXT file inside the ZIP, with a ``{date}``
        placeholder using the same convention as ``zip_name_template``.
        The extractor matches ``<prefix>*.txt`` to stay resilient against
        optional revision suffixes.
    s3_prefix : str
        S3 key prefix for the raw ZIP archive, under the bucket root
        (e.g. ``"b3/bdi/tick_by_tick_rv/"``).
    s3_prefix_ticks : str
        S3 key prefix for the processed ticks Parquet, under the bucket
        root (e.g. ``"b3/tick_by_tick/rv/ticks/"``). The final key also
        receives a ``year={YYYY}/`` segment and the date-named file,
        appended by the partitioner at upload time.
    paths_key_downloads : str
        Key into ``PATHS_B3`` for the local downloads directory.
    paths_key_raw : str
        Key into ``PATHS_B3`` for the local raw Parquet directory.
    paths_key_ticks : str
        Key into ``PATHS_B3`` for the local processed ticks directory.
    label : str
        Short human-readable label used in log messages and e-mail
        summaries.
    """

    url_template        : str
    zip_name_template   : str
    txt_prefix_template : str
    s3_prefix           : str
    s3_prefix_ticks     : str
    paths_key_downloads : str
    paths_key_raw       : str
    paths_key_ticks     : str
    label               : str


_CONFIGS: Final[dict[str, FeedConfig]] = {
    "RV": FeedConfig(
        url_template        = "https://drp.b3.com.br/rapinegocios/tickercsv/{date}?type=2",
        # ZIP uses YYYYMMDD; TXT inside uses DD-MM-YYYY with _RV suffix.
        zip_name_template   = "{date:%Y%m%d}_NEGOCIOSAVISTA.zip",
        txt_prefix_template = "{date:%d-%m-%Y}_NEGOCIOSAVISTA_RV",
        s3_prefix           = "b3/bdi/tick_by_tick_rv/",
        s3_prefix_ticks     = "b3/tick_by_tick/rv/ticks/",
        paths_key_downloads = "rv_downloads",
        paths_key_raw       = "rv_raw_parquet",
        paths_key_ticks     = "rv_ticks",
        label               = "Tick-by-tick RV (equities)",
    ),
    "DERIV": FeedConfig(
        url_template        = "https://drp.b3.com.br/rapinegocios/tickercsv/{date}?type=1",
        # Both ZIP and TXT use DD-MM-YYYY with _DRV suffix.
        zip_name_template   = "{date:%d-%m-%Y}_NEGOCIOSAVISTA_DRV.zip",
        txt_prefix_template = "{date:%d-%m-%Y}_NEGOCIOSAVISTA_DRV",
        s3_prefix           = "b3/bdi/tick_by_tick_deriv/",
        s3_prefix_ticks     = "b3/tick_by_tick/deriv/ticks/",
        paths_key_downloads = "deriv_downloads",
        paths_key_raw       = "deriv_raw_parquet",
        paths_key_ticks     = "deriv_ticks",
        label               = "Tick-by-tick DERIV (derivatives)",
    ),
}


# --- Public enum ---

class FeedType(Enum):
    """
    B3 intraday tick-data feed selector.

    Attributes
    ----------
    RV :
        Equities feed (``type=2``).
        ZIP  : ``YYYYMMDD_NEGOCIOSAVISTA.zip``
        TXT  : ``DD-MM-YYYY_NEGOCIOSAVISTA_RV.txt``
    DERIV :
        Derivatives feed (``type=1``).
        ZIP  : ``DD-MM-YYYY_NEGOCIOSAVISTA_DRV.zip``
        TXT  : ``DD-MM-YYYY_NEGOCIOSAVISTA_DRV.txt``
    """

    RV    = auto()
    DERIV = auto()

    @property
    def config(self) -> FeedConfig:
        """Return the immutable ``FeedConfig`` for this feed."""
        return _CONFIGS[self.name]