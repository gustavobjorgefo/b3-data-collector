# src/b3_data_collector/bdi/parsers/_registry.py

"""
Self-registering parser registry for BDI reports.

Each report's parser function registers itself via ``@register_parser``
at import time. Nothing outside the ``parsers`` package needs to
enumerate reports — the mapping is built as a side effect of importing
``bdi.parsers`` once (see ``__init__.py``).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd

ParserFunc = Callable[[str | Path | bytes], pd.DataFrame]

_READERS: dict[str, ParserFunc] = {}


def register_parser(report_name: str) -> Callable[[ParserFunc], ParserFunc]:
    """
    Register a parser function under a BDI report's ``api_name``.

    Parameters
    ----------
    report_name : str
        The report's ``api_name``, as defined in ``bdi._catalog``.

    Returns
    -------
    Callable[[ParserFunc], ParserFunc]
        Decorator that registers the function unchanged.

    Raises
    ------
    ValueError
        If ``report_name`` is already registered — catches copy-paste
        mistakes where two files claim the same report.
    """
    def decorator(func: ParserFunc) -> ParserFunc:
        if report_name in _READERS:
            raise ValueError(
                f"Parser for '{report_name}' already registered "
                f"(by {_READERS[report_name].__module__})."
            )
        _READERS[report_name] = func
        return func

    return decorator


def read_bdi_report_file(source: str | Path | bytes, report_name: str) -> pd.DataFrame:
    """
    Parse any BDI report whose parser is registered.

    Parameters
    ----------
    source : str | Path | bytes
        Path to a downloaded CSV, or its raw bytes.
    report_name : str
        The report's ``api_name`` (e.g. ``"BTBLoanBalance"``).

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    NotImplementedError
        If no parser is registered yet for ``report_name``.
    """
    try:
        parser = _READERS[report_name]
    except KeyError:
        raise NotImplementedError(
            f"No parser registered for report '{report_name}'. "
            f"Currently supported: {sorted(_READERS)}. "
            "See b3_data_collector.bdi.parsers package docstring to add one."
        ) from None

    return parser(source)