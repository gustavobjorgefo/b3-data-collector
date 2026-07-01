# src\b3_data_collector\bdi\__init__.py

"""
B3 BDI reports ingestion package.

Public API
----------
run_bdi_pipeline
    Download and upload to S3 all enabled BDI reports for one or more
    trading dates. See ``pipeline.py`` for full parameter documentation.

Examples
--------
Single date:

>>> from b3_data_collector.bdi import run_bdi_pipeline
>>> run_bdi_pipeline("2026-06-26")

Date range:

>>> run_bdi_pipeline(("2026-05-29", "2026-06-27"))
"""

from __future__ import annotations

from .pipeline import run_bdi_pipeline

__all__: list[str] = ["run_bdi_pipeline"]