# src\b3_data_collector\bdi\_client.py

"""
BDI API client — fetches report CSVs from arquivos.b3.com.br.

Responsible for making the POST request to the BDI export endpoint and
returning the raw CSV bytes. Does not touch the filesystem or S3 — that
responsibility belongs to the pipeline orchestrator.

The endpoint accepts a JSON body with the report name and date range,
and responds with the full CSV in a single response (no pagination needed
for the export endpoint, as opposed to the paginated table view used by
the B3 website UI).

Endpoint
--------
POST https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR
Body : {"Name": "<api_name>", "Date": "YYYY-MM-DD",
        "FinalDate": "YYYY-MM-DD", "ClientId": "", "Filters": {}}
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Final

import requests

logger = logging.getLogger(__name__)

# --- Module constants ---

_BDI_EXPORT_URL: Final[str] = (
    "https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR"
)

_REQUEST_TIMEOUT   : Final[int] = 60   # seconds; some large reports are slow
_MIN_CONTENT_BYTES : Final[int] = 10   # below this the response is empty/error


# --- Public API ---

def fetch_report_csv(
    api_name   : str,
    trade_date : date,
    timeout    : int = _REQUEST_TIMEOUT,
) -> bytes | None:
    """
    Fetch the CSV export for a single BDI report and trading date.

    Parameters
    ----------
    api_name : str
        Report identifier as used in the BDI API (e.g. ``"DailyAverageStocks"``).
    trade_date : date
        Trading date to request.
    timeout : int, optional
        HTTP request timeout in seconds. Default is ``60``.

    Returns
    -------
    bytes | None
        Raw CSV content as bytes, or ``None`` if the report is unavailable
        for the requested date (B3 returned 404 or empty body).

    Raises
    ------
    requests.HTTPError
        For HTTP errors other than 404.
    requests.RequestException
        For network-level errors (timeout, connection refused, etc.).
    """
    date_str = f"{trade_date:%Y-%m-%d}"
    payload  = {
        "Name"      : api_name,
        "Date"      : date_str,
        "FinalDate" : date_str,
        "ClientId"  : "",
        "Filters"   : {},
    }

    logger.debug("Fetching BDI report '%s' for %s", api_name, trade_date)

    response = requests.post(_BDI_EXPORT_URL, json=payload, timeout=timeout)

    if response.status_code == 404:
        logger.warning(
            "Report '%s' not available for %s (404).", api_name, trade_date
        )
        return None

    response.raise_for_status()

    content = response.content

    # An empty or near-empty response means the report has no data for this
    # date (holiday, weekend, or report simply not published yet).
    if len(content) < _MIN_CONTENT_BYTES:
        logger.warning(
            "Report '%s' returned empty content for %s.", api_name, trade_date
        )
        return None

    logger.debug(
        "Report '%s' fetched for %s — %d bytes.", api_name, trade_date, len(content)
    )
    return content