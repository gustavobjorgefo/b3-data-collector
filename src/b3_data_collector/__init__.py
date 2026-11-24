"""
b3-data-collector: collection, archival and read access for B3 market data.

``__version__`` is the single source of the package version: ``pyproject.toml``
reads it at build time, and the tick contract records it in every ticks Parquet.
"""

from __future__ import annotations

from typing import Final

__version__: Final[str] = "0.2.0"
