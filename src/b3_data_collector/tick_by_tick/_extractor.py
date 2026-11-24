# src\b3_data_collector\tick_by_tick\_extractor.py

"""
B3 raw tick-data extractor — Stage 2.

Responsible for reading the daily ZIP produced by the download stage,
checking the inner TXT against the tick contract (``_contract.py``),
converting every column explicitly, renaming columns to canonical English
names, and persisting the result as a single Parquet file under the
feed-specific raw Parquet directory, with the file's provenance (ZIP
SHA-256, TXT name and row count) stored in the Parquet footer.

Column schema is sourced from the official B3 glossary
(Glossario_NegociosListados_PT, version 3, May/2023). Both the equities
(RV) and derivatives (DERIV) feeds share the same 12-column schema:

    DataReferencia · CodigoInstrumento · AcaoAtualizacao · PrecoNegocio
    QuantidadeNegociada · HoraFechamento · CodigoIdentificadorNegocio
    TipoSessaoPregao · DataNegocio · CodigoParticipanteComprador
    CodigoParticipanteVendedor · TipoDoCanal

Every column is read as text and converted on purpose: letting pandas
infer types means one malformed cell can silently change how a whole
column is parsed (an empty time turns ``HoraFechamento`` into floats and
corrupts every time before 10:00).

Contract rules applied here — any violation fails the date with
``ContractViolationError``:

- The header must list exactly the 12 columns above, in order.
- Every data line must have exactly 12 fields.
- The ZIP must hold exactly one TXT for the date.
- Dates must be valid ``YYYY-MM-DD``; price, quantity, trade id and the
  three code columns must convert to their contract types.

Tolerated: an unreadable ``HoraFechamento`` is kept verbatim in
``time_raw`` (the partitioner turns it into a null timestamp), and an
empty broker code becomes null (B3 publishes agricultural derivatives
anonymously). Blank lines are skipped with a warning.

Feed-specific differences (ZIP path, raw Parquet path, TXT prefix inside
the ZIP) are resolved through ``FeedType.config``.

Does not build timestamps, partition or filter by symbol — that
responsibility belongs to the partitioning stage (_partitioner.py).
"""

from __future__ import annotations

import csv
import hashlib
import logging
import re
import zipfile
from datetime import date
from pathlib import Path
from typing import IO, Final

import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..paths import PATHS_B3
from ._contract import ContractViolationError, SourceProvenance, with_provenance
from ._feed import FeedType
from ._models import StageStatus

logger = logging.getLogger(__name__)


# --- Schema ---

_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "DataReferencia",
    "CodigoInstrumento",
    "AcaoAtualizacao",
    "PrecoNegocio",
    "QuantidadeNegociada",
    "HoraFechamento",
    "CodigoIdentificadorNegocio",
    "TipoSessaoPregao",
    "DataNegocio",
    "CodigoParticipanteComprador",
    "CodigoParticipanteVendedor",
    "TipoDoCanal",
)

_FIELD_SEPARATOR     : Final[str] = ";"
_TXT_ENCODING        : Final[str] = "latin1"
_FIRST_DATA_LINE     : Final[int] = 2  # line 1 is the header
_HASH_CHUNK_SIZE     : Final[int] = 1024 * 1024
_MAX_REPORTED_BLANKS : Final[int] = 10

_DATE_PATTERN  : Final[re.Pattern[str]] = re.compile(r"\d{4}-\d{2}-\d{2}")
_PRICE_PATTERN : Final[str]             = r"-?\d+(?:\.\d+)?"


# --- Internal helpers: file level ---

def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _find_txt_name(archive: zipfile.ZipFile, txt_prefix: str, label: str, zip_name: str) -> str:
    """
    Return the single TXT in ``archive`` whose name starts with ``txt_prefix``.

    Matching by prefix keeps the extractor resilient to optional suffixes
    such as ``"_RV"`` or ``"_revised"``.

    Raises
    ------
    KeyError
        If no TXT matches.
    ContractViolationError
        If more than one TXT matches: picking one would be a guess.
    """
    candidates = [
        name for name in archive.namelist()
        if name.startswith(txt_prefix) and name.endswith(".txt")
    ]
    if not candidates:
        raise KeyError(
            f"[{label}] No TXT matching '{txt_prefix}*.txt' found inside ZIP "
            f"'{zip_name}'. Available files: {archive.namelist()}"
        )
    if len(candidates) > 1:
        raise ContractViolationError(
            f"[{label}] ZIP '{zip_name}' holds more than one TXT matching "
            f"'{txt_prefix}*.txt': {candidates}"
        )
    return candidates[0]


def _scan_lines(raw_file: IO[bytes], label: str, txt_name: str) -> npt.NDArray[np.int32]:
    """
    Check the TXT layout and return the line number of every data line.

    Reads the file once as bytes, independently of pandas, so the row
    count recorded in the provenance does not depend on the parser that
    produced the rows.

    Parameters
    ----------
    raw_file : IO[bytes]
        TXT opened in binary mode, positioned at the start.
    label : str
        Feed label for messages.
    txt_name : str
        TXT name for messages.

    Returns
    -------
    npt.NDArray[np.int32]
        1-based physical line numbers of the data lines, in file order.

    Raises
    ------
    ContractViolationError
        If the header is not the expected one or a line has the wrong
        number of fields.
    """
    expected_header = _FIELD_SEPARATOR.join(_SOURCE_COLUMNS)
    header = raw_file.readline().rstrip(b"\r\n").decode(_TXT_ENCODING)
    if header != expected_header:
        raise ContractViolationError(
            f"[{label}] Unexpected header in '{txt_name}': {header!r}. "
            f"Expected: {expected_header!r}."
        )

    separator = _FIELD_SEPARATOR.encode(_TXT_ENCODING)
    expected_separators = len(_SOURCE_COLUMNS) - 1
    blank_lines: list[int] = []
    malformed_count = 0
    first_malformed: tuple[int, int] | None = None
    last_line = _FIRST_DATA_LINE - 1

    for last_line, line in enumerate(raw_file, start=_FIRST_DATA_LINE):
        content = line.rstrip(b"\r\n")
        if not content:
            blank_lines.append(last_line)
            continue
        separators = content.count(separator)
        if separators != expected_separators:
            malformed_count += 1
            if first_malformed is None:
                first_malformed = (last_line, separators + 1)

    if first_malformed is not None:
        line_number, field_count = first_malformed
        raise ContractViolationError(
            f"[{label}] {malformed_count} line(s) in '{txt_name}' do not have "
            f"{len(_SOURCE_COLUMNS)} fields; first at line {line_number} "
            f"({field_count} fields)."
        )

    line_numbers = np.arange(_FIRST_DATA_LINE, last_line + 1, dtype=np.int32)
    if blank_lines:
        logger.warning(
            "[%s] Skipped %d blank line(s) in '%s': %s",
            label, len(blank_lines), txt_name, blank_lines[:_MAX_REPORTED_BLANKS],
        )
        line_numbers = line_numbers[~np.isin(line_numbers, blank_lines)]
    return line_numbers


def _read_text_columns(raw_file: IO[bytes]) -> pd.DataFrame:
    # Quoting off and NA detection off: the TXT has no quoting, and an
    # empty cell must stay "" so each converter decides what it means.
    return pd.read_csv(
        raw_file,
        sep              = _FIELD_SEPARATOR,
        encoding         = _TXT_ENCODING,
        header           = None,
        skiprows         = 1,
        names            = list(_SOURCE_COLUMNS),
        dtype            = str,
        na_filter        = False,
        quoting          = csv.QUOTE_NONE,
        skip_blank_lines = True,
    )


# --- Internal helpers: column conversion ---

def _reject_invalid(
    column       : str,
    values       : pd.Series,
    invalid      : npt.NDArray[np.bool_],
    line_numbers : npt.NDArray[np.int32],
    label        : str,
) -> None:
    if not invalid.any():
        return
    first = int(invalid.argmax())
    raise ContractViolationError(
        f"[{label}] Column {column}: {int(invalid.sum())} invalid value(s); "
        f"first at TXT line {line_numbers[first]}: {values.iloc[first]!r}."
    )


def _convert_integer(
    column       : str,
    values       : pd.Series,
    dtype        : type[np.integer],
    line_numbers : npt.NDArray[np.int32],
    label        : str,
) -> npt.NDArray[np.integer]:
    """
    Convert non-negative integer text to ``dtype``, rejecting anything else.

    Supports the contract's integer types, up to ``int64``: their largest
    value has at most 19 digits, so every accepted text fits ``uint64``
    before the range check.
    """
    maximum = np.iinfo(dtype).max
    is_digits = values.str.fullmatch(rf"\d{{1,{len(str(maximum))}}}").to_numpy(dtype=bool)
    _reject_invalid(column, values, ~is_digits, line_numbers, label)

    numbers = values.astype("uint64").to_numpy()
    _reject_invalid(column, values, numbers > maximum, line_numbers, label)
    return numbers.astype(dtype)


def _convert_price(
    values       : pd.Series,
    line_numbers : npt.NDArray[np.int32],
    label        : str,
) -> npt.NDArray[np.float64]:
    normalised = values.str.replace(",", ".", regex=False)
    is_number = normalised.str.fullmatch(_PRICE_PATTERN).to_numpy(dtype=bool)
    _reject_invalid("PrecoNegocio", values, ~is_number, line_numbers, label)
    return normalised.astype("float64").to_numpy()


def _validate_dates(
    column       : str,
    values       : pd.Series,
    line_numbers : npt.NDArray[np.int32],
    label        : str,
) -> pd.Series:
    # A day holds one or two distinct dates, so checking the unique values
    # is far cheaper than checking every row.
    invalid_dates = [text for text in values.unique() if not _is_iso_date(text)]
    _reject_invalid(
        column, values, values.isin(invalid_dates).to_numpy(), line_numbers, label
    )
    return values


def _is_iso_date(text: str) -> bool:
    if not _DATE_PATTERN.fullmatch(text):
        return False
    try:
        date.fromisoformat(text)
    except ValueError:
        return False
    return True


def _convert(df: pd.DataFrame, line_numbers: npt.NDArray[np.int32], label: str) -> pd.DataFrame:
    """
    Convert the text columns to their contract types and English names.

    Parameters
    ----------
    df : pd.DataFrame
        TXT content, every column as text, original Portuguese names.
    line_numbers : npt.NDArray[np.int32]
        TXT line number of each row, from ``_scan_lines``.
    label : str
        Feed label for messages.

    Returns
    -------
    pd.DataFrame
        Raw Parquet content, in TXT order.

    Raises
    ------
    ContractViolationError
        On the first column holding a value that cannot be converted.
    """
    def integer(column: str, dtype: type[np.integer]) -> npt.NDArray[np.integer]:
        return _convert_integer(column, df[column], dtype, line_numbers, label)

    def nullable_code(column: str) -> pd.Series:
        return df[column].mask(df[column] == "")

    return pd.DataFrame({
        "source_row"     : line_numbers,
        "symbol"         : df["CodigoInstrumento"],
        "trade_date"     : _validate_dates("DataNegocio", df["DataNegocio"], line_numbers, label),
        "reference_date" : _validate_dates(
            "DataReferencia", df["DataReferencia"], line_numbers, label
        ),
        "time_raw"       : df["HoraFechamento"],
        "price"          : _convert_price(df["PrecoNegocio"], line_numbers, label),
        "quantity"       : integer("QuantidadeNegociada", np.uint32),
        "update_action"  : integer("AcaoAtualizacao", np.uint8),
        "session_type"   : integer("TipoSessaoPregao", np.uint8),
        "channel_type"   : integer("TipoDoCanal", np.uint8),
        "trade_id"       : integer("CodigoIdentificadorNegocio", np.int64),
        "buyer_broker"   : nullable_code("CodigoParticipanteComprador"),
        "seller_broker"  : nullable_code("CodigoParticipanteVendedor"),
    })


# --- Public API ---

def extract_to_raw_parquet(
    trade_date : date,
    feed       : FeedType,
    overwrite  : bool = False,
) -> StageStatus:
    """
    Extract and convert B3 tick data from the daily ZIP into a Parquet file.

    Parameters
    ----------
    trade_date : date
        Trading date to extract.
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    overwrite : bool, optional
        If ``True``, overwrites an existing Parquet file. Default is ``False``.

    Returns
    -------
    StageStatus
        ``SUCCESS`` or ``SKIPPED``.

    Raises
    ------
    FileNotFoundError
        If the expected ZIP file does not exist in the downloads directory.
    KeyError
        If the expected TXT file is not found inside the ZIP archive.
    ContractViolationError
        If the TXT breaks the tick contract (see the module docstring).
    ValueError
        If the ZIP file is corrupt or malformed.
    """
    cfg         = feed.config
    zip_name    = cfg.zip_name_template.format(date=trade_date)
    zip_path    = PATHS_B3[cfg.paths_key_downloads] / zip_name
    output_file = PATHS_B3[cfg.paths_key_raw] / f"{trade_date}.parquet"
    ticks_file  = PATHS_B3[cfg.paths_key_ticks] / f"{trade_date}.parquet"

    if output_file.exists() and not overwrite:
        logger.info(
            "[%s] Skipping extraction for %s — raw Parquet exists.",
            cfg.label, trade_date,
        )
        return StageStatus.SKIPPED

    # The partitioner deletes the raw Parquet once the ticks file is
    # written, so an existing ticks file means this stage already ran.
    if ticks_file.exists() and not overwrite:
        logger.info(
            "[%s] Skipping extraction for %s — ticks Parquet exists.",
            cfg.label, trade_date,
        )
        return StageStatus.SKIPPED

    if not zip_path.exists():
        raise FileNotFoundError(f"[{cfg.label}] ZIP not found: {zip_path}")

    # The TXT prefix is feed-specific and may use a different date format
    # (RV: YYYYMMDD, DERIV: DD-MM-YYYY).
    txt_prefix = cfg.txt_prefix_template.format(date=trade_date)
    logger.info(
        "[%s] Extracting %s*.txt from %s", cfg.label, txt_prefix, zip_path.name
    )

    try:
        with zipfile.ZipFile(zip_path) as archive:
            txt_name = _find_txt_name(archive, txt_prefix, cfg.label, zip_path.name)
            logger.info("[%s] Found TXT: %s", cfg.label, txt_name)

            with archive.open(txt_name) as raw_file:
                line_numbers = _scan_lines(raw_file, cfg.label, txt_name)
            with archive.open(txt_name) as raw_file:
                text_columns = _read_text_columns(raw_file)
    except zipfile.BadZipFile as exc:
        raise ValueError(
            f"[{cfg.label}] Corrupt ZIP file '{zip_path.name}': {exc}"
        ) from exc

    if len(text_columns) != len(line_numbers):
        raise ContractViolationError(
            f"[{cfg.label}] Parser read {len(text_columns)} rows from '{txt_name}', "
            f"but the file has {len(line_numbers)} data lines."
        )

    df = _convert(text_columns, line_numbers, cfg.label)

    provenance = SourceProvenance(
        feed                 = feed.name,
        trade_date           = f"{trade_date}",
        source_zip_name      = zip_path.name,
        source_zip_sha256    = _sha256_of(zip_path),
        source_txt_name      = txt_name,
        source_txt_row_count = len(line_numbers),
    )
    table = with_provenance(pa.Table.from_pandas(df, preserve_index=False), provenance)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_file)

    logger.info(
        "[%s] Extraction complete for %s — %d trades → %s",
        cfg.label, trade_date, len(df), output_file,
    )
    return StageStatus.SUCCESS
