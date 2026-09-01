# src/b3_data_collector/bdi/parsers/__init__.py

"""
BDI report parsers — public surface of the ``parsers`` subpackage.

Importing this package imports every report module below, which
registers its parser via ``@register_parser`` as a side effect.

Adding support for a new report:

    1. Write ``_<report_name>.py`` with a
       ``read_<report_name>(source) -> pd.DataFrame`` function,
       decorated with ``@register_parser("<ApiName>")``.
    2. Add it to the import list below.
"""

from __future__ import annotations

from . import (
    _btb_loan_balance,  # noqa: F401  (imported for @register_parser side effect)
    _instruments_derivatives,  # noqa: F401  (imported for @register_parser side effect)
    _instruments_equities,  # noqa: F401  (imported for @register_parser side effect)
)
from ._registry import read_bdi_report_file

__all__ = ["read_bdi_report_file"]
