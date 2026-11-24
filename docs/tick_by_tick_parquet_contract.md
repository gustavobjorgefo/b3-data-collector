# Tick-by-tick Parquet contract — schema version 1

This is what a consumer of the tick-by-tick Parquet files in S3 can rely on.
Both feeds, equities (RV) and derivatives (DERIV), follow the same contract.

The code is the source of truth: `src/b3_data_collector/tick_by_tick/_contract.py`
defines the schema, the time zone and the provenance fields, and the tests check
the files the pipeline produces against it. This document explains it.

## Where the files are

One file per feed and trading date:

```text
s3://<bucket>/b3/tick_by_tick/{rv|deriv}/ticks/year={YYYY}/{YYYY-MM-DD}.parquet
```

Each file is built from the B3 ZIP archived, unchanged, at:

```text
s3://<bucket>/b3/bdi/tick_by_tick_rv/{YYYYMMDD}_NEGOCIOSAVISTA.zip
s3://<bucket>/b3/bdi/tick_by_tick_deriv/{DD-MM-YYYY}_NEGOCIOSAVISTA_DRV.zip
```

The ZIP is the immutable source. A ticks file can be rebuilt from it at any time,
and its footer records the SHA-256 of the exact ZIP it came from.

## Columns

In this order, with these Arrow types:

| Column | Arrow type | Nullable | Source (B3 TXT) | Content |
|---|---|---|---|---|
| `source_row` | `int32` | no | — | Physical line number in the TXT. The header is line 1, so the first trade is line 2. |
| `symbol` | `string` | no | `CodigoInstrumento` | Instrument code. |
| `trade_date` | `string` | no | `DataNegocio` | Trading session date, `YYYY-MM-DD`. |
| `reference_date` | `string` | no | `DataReferencia` | Reference date of the record, `YYYY-MM-DD`. |
| `timestamp` | `timestamp[ms, tz=America/Sao_Paulo]` | **yes** | `DataNegocio` + `HoraFechamento` | Trade time. Null when `time_raw` is not a valid time. |
| `time_raw` | `string` | no | `HoraFechamento` | The time exactly as written in the TXT (`HHMMSSmmm`, e.g. `"090005990"`). |
| `price` | `float64` | no | `PrecoNegocio` | Trade price (the TXT's decimal comma becomes a point). |
| `quantity` | `uint32` | no | `QuantidadeNegociada` | Quantity traded. |
| `update_action` | `uint8` | no | `AcaoAtualizacao` | See [codes](#codes). |
| `session_type` | `uint8` | no | `TipoSessaoPregao` | See [codes](#codes). |
| `channel_type` | `uint8` | no | `TipoDoCanal` | See [codes](#codes). |
| `trade_id` | `int64` | no | `CodigoIdentificadorNegocio` | Trade number. |
| `buyer_broker` | `string` | **yes** | `CodigoParticipanteComprador` | Buyer participant code. Null when B3 did not disclose it. |
| `seller_broker` | `string` | **yes** | `CodigoParticipanteVendedor` | Seller participant code. Null when B3 did not disclose it. |

Notes:

- **Broker codes:** null means "not disclosed", never a code. B3 publishes
  agricultural derivatives anonymously, with empty participant fields. A `"0"` in
  the TXT stays `"0"`.
- **Dates** are kept as `YYYY-MM-DD` text. Every value is checked to be a real
  calendar date before the file is written.
- **Price** is a `float64`, so consumers comparing it against the tick size should
  allow for rounding error.

## Time zone

B3 publishes `HoraFechamento` in local time, UTC-3 (glossary v3). `timestamp` is
localised to `America/Sao_Paulo` and stored as UTC with that time zone in the Arrow
type, so readers get a time-zone-aware column without guessing. Brazil has had no
daylight saving time since 2019, so today the two are the same offset.

`timestamp` is built from an explicit format, never from inference. It is null when
`time_raw` is not exactly nine digits forming a valid `HH:MM:SS.mmm` time. In that
case the row is kept, `time_raw` shows the original value, and the row is counted in
`timestamp_null_count`.

## Row order

Rows are in the same order as the TXT, never sorted or filtered. When several trades
share the same millisecond, this order is the only way to tell which came first, for
example the open and close of a bar. `source_row` makes the order explicit, so it
survives any later re-sorting: sort by `source_row` to get it back.

## Codes

From the B3 glossary *Glossario_NegociosListados*, version 3 (May 2023):

| Column | Value | Meaning |
|---|---|---|
| `update_action` | `0` | New trade |
| | `2` | Deleted (cancelled) trade |
| `session_type` | `1` | Regular session |
| | `6` | After hours |
| `channel_type` | — | **Not described in glossary v3.** Values seen in sample files: `2` (RV) and `1` (DERIV). |

The collector does not reject values outside these lists. A new code from B3 is
published as is, and consumers decide how to treat it.

## Provenance (footer metadata)

The Parquet footer holds one key, `b3_data_collector`, whose value is a JSON
object:

| Field | Type | Content |
|---|---|---|
| `schema_version` | int | Version of this contract: `1`. |
| `feed` | string | `"RV"` or `"DERIV"`. |
| `trade_date` | string | Trading date of the file, `YYYY-MM-DD`. |
| `source_zip_name` | string | File name of the archived ZIP. |
| `source_zip_sha256` | string | Hex SHA-256 of the ZIP bytes. |
| `source_txt_name` | string | Name of the TXT inside the ZIP. |
| `source_txt_row_count` | int | Data lines in the TXT: header and blank lines excluded. |
| `row_count` | int | Rows in the file. Always equal to `source_txt_row_count`. |
| `timestamp_null_count` | int | Rows with a null `timestamp`. |
| `timezone` | string | `"America/Sao_Paulo"`. |
| `collector_version` | string | Version of the `b3-data-collector` package. |
| `collector_commit` | string or null | Git commit the collector ran from. A `+dirty` suffix means it had local changes, and null means it did not run from a git checkout. |
| `created_at_utc` | string | ISO-8601 UTC time the file was written. |

The footer can be read without loading the data:

```python
import json
import pyarrow.parquet as pq

metadata = pq.read_schema("2026-06-30.parquet").metadata
provenance = json.loads(metadata[b"b3_data_collector"])
```

A file without this key predates the contract.

## What the collector guarantees

A date is **not published** when its TXT has any of these problems:

- a header that is not exactly the 12 expected columns, in order;
- a line that does not have exactly 12 fields;
- more than one TXT for the date inside the ZIP;
- a date that is not a valid `YYYY-MM-DD`;
- a price, quantity, trade number, `update_action`, `session_type` or `channel_type`
  that cannot be converted to its type;
- a row count different from the TXT's data line count.

A date **is published** with these problems kept visible:

| Problem | How it appears |
|---|---|
| Unreadable time | Null `timestamp`, original value in `time_raw`, counted in `timestamp_null_count`. |
| Undisclosed broker | Null `buyer_broker` / `seller_broker`. |
| Blank lines in the TXT | Skipped and logged. Not counted as data lines, and `source_row` still gives the physical line. |

## Versioning

`schema_version` changes when any of these changes: the columns, their order, their
types or nullability, the meaning of a column, or a provenance field. Every file is
rewritten from its archived ZIP when the version changes, so consumers only need to
read the current version.
