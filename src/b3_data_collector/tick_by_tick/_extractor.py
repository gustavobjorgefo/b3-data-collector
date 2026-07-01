# src\b3_data_collector\tick_by_tick\_extractor.py

"""
B3 raw tick-data extractor — Stage 2.

Responsible for reading the daily ZIP produced by the download stage,
parsing the inner TXT file, normalising column types, renaming columns to
canonical English names, and persisting the result as a single Parquet
file under the feed-specific raw Parquet directory.

Column schema is sourced from the official B3 glossary
(Glossario_NegociosListados_PT, version 3, May/2023). Both the equities
(RV) and derivatives (DERIV) feeds share the same 12-column schema:

    DataReferencia · CodigoInstrumento · AcaoAtualizacao · PrecoNegocio
    QuantidadeNegociada · HoraFechamento · CodigoIdentificadorNegocio
    TipoSessaoPregao · DataNegocio · CodigoParticipanteComprador
    CodigoParticipanteVendedor · TipoDoCanal

Feed-specific differences (ZIP path, raw Parquet path, TXT prefix inside
the ZIP) are resolved through ``FeedType.config``.

Does not partition or filter by symbol — that responsibility belongs to
the partitioning stage (_partitioner.py).
"""

from __future__ import annotations

import logging
import zipfile
from datetime import date
from typing import Final

import pandas as pd

from ..paths import PATHS_B3
from ._feed import FeedType
from ._models import StageStatus

logger = logging.getLogger(__name__)


# --- Schema ---

_COLUMN_NAMES_RAW: Final[list[str]] = [
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
]

_COLUMN_RENAME_MAP: Final[dict[str, str]] = {
    "DataNegocio"                 : "trade_date",
    "CodigoInstrumento"           : "symbol",
    "PrecoNegocio"                : "price",
    "QuantidadeNegociada"         : "quantity",
    "HoraFechamento"              : "time",
    "CodigoParticipanteComprador" : "buyer_broker",
    "CodigoParticipanteVendedor"  : "seller_broker",
    "TipoSessaoPregao"            : "session_type",
    "AcaoAtualizacao"             : "update_action",
    "CodigoIdentificadorNegocio"  : "trade_id",
    "DataReferencia"              : "reference_date",
    "TipoDoCanal"                 : "channel_type",
}


# --- Internal helpers ---

def _parse_time(series: pd.Series) -> pd.Series:
    """
    Convert raw integer time to ``"HH:MM:SS.mmm"`` string format.

    The raw value is a 9-digit integer encoding ``HHMMSSmmm``
    (e.g. ``100000005`` → ``"10:00:00.005"``).
    """
    padded = series.astype(str).str.zfill(9)
    return (
        padded.str.slice(0, 2) + ":" +
        padded.str.slice(2, 4) + ":" +
        padded.str.slice(4, 6) + "." +
        padded.str.slice(6, 9)
    )


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all type normalisations and rename to canonical English schema.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame as read from the TXT, with original Portuguese column
        names.

    Returns
    -------
    pd.DataFrame
        Normalised DataFrame with canonical English column names.
    """
    # Price: comma-decimal string → float64
    df["PrecoNegocio"] = (
        df["PrecoNegocio"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype("float64")
    )

    # Time: raw int (e.g. 100000005) → "HH:MM:SS.mmm"
    df["HoraFechamento"] = _parse_time(df["HoraFechamento"])

    # Participant IDs: nullable int → canonical string
    for col in ("CodigoParticipanteComprador", "CodigoParticipanteVendedor"):
        df[col] = (
            df[col]
            .fillna(0)
            .astype("int64")
            .astype(pd.StringDtype())
        )

    df["AcaoAtualizacao"]     = df["AcaoAtualizacao"].astype("uint8")
    df["TipoSessaoPregao"]    = df["TipoSessaoPregao"].astype("uint8")
    df["QuantidadeNegociada"] = df["QuantidadeNegociada"].astype("uint32")
    df["TipoDoCanal"]         = df["TipoDoCanal"].astype("uint8")

    return df.rename(columns=_COLUMN_RENAME_MAP)


# --- Public API ---

def extract_to_raw_parquet(
    trade_date : date,
    feed       : FeedType,
    overwrite  : bool = False,
) -> StageStatus:
    """
    Extract and normalise B3 tick data from the daily ZIP into a Parquet file.

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
        ``SUCCESS``, ``SKIPPED``, or ``FAILED``.

    Raises
    ------
    FileNotFoundError
        If the expected ZIP file does not exist in the downloads directory.
    KeyError
        If the expected TXT file is not found inside the ZIP archive.
    ValueError
        If the ZIP file is corrupt or malformed.
    """
    cfg         = feed.config
    zip_name    = cfg.zip_name_template.format(date=trade_date)
    zip_path    = PATHS_B3[cfg.paths_key_downloads] / zip_name
    output_file = PATHS_B3[cfg.paths_key_raw] / f"{trade_date}.parquet"

    if output_file.exists() and not overwrite:
        logger.info(
            "[%s] Skipping extraction for %s — raw Parquet exists.",
            cfg.label, trade_date,
        )
        return StageStatus.SKIPPED

    if not zip_path.exists():
        raise FileNotFoundError(f"[{cfg.label}] ZIP not found: {zip_path}")

    # The TXT prefix is feed-specific and may use a different date format
    # (RV: YYYYMMDD, DERIV: DD-MM-YYYY). We match by prefix to remain
    # resilient to optional suffixes such as "_RV" or "_revised".
    txt_prefix = cfg.txt_prefix_template.format(date=trade_date)
    logger.info(
        "[%s] Extracting %s*.txt from %s", cfg.label, txt_prefix, zip_path.name
    )

    try:
        with zipfile.ZipFile(zip_path) as archive:
            candidates = [
                name for name in archive.namelist()
                if name.startswith(txt_prefix) and name.endswith(".txt")
            ]
            if not candidates:
                raise KeyError(
                    f"[{cfg.label}] No TXT matching '{txt_prefix}*.txt' "
                    f"found inside ZIP '{zip_path.name}'. "
                    f"Available files: {archive.namelist()}"
                )

            txt_name = candidates[0]
            if len(candidates) > 1:
                logger.warning(
                    "[%s] Multiple TXT candidates in ZIP '%s': %s — using '%s'.",
                    cfg.label, zip_path.name, candidates, txt_name,
                )
            else:
                logger.info("[%s] Found TXT: %s", cfg.label, txt_name)

            with archive.open(txt_name) as raw_file:
                df = pd.read_csv(
                    raw_file,
                    sep        = ";",
                    encoding   = "latin1",
                    header     = None,
                    skiprows   = 1,
                    low_memory = False,
                    names      = _COLUMN_NAMES_RAW,
                )
    except zipfile.BadZipFile as exc:
        raise ValueError(
            f"[{cfg.label}] Corrupt ZIP file '{zip_path.name}': {exc}"
        ) from exc

    df = _normalise(df)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False)

    logger.info(
        "[%s] Extraction complete for %s — %d trades → %s",
        cfg.label, trade_date, len(df), output_file,
    )
    return StageStatus.SUCCESS