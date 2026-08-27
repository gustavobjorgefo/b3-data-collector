# b3-data-collector

![CI](https://github.com/gustavobjorgefo/b3-data-collector/actions/workflows/ci.yml/badge.svg)

**Automated collection, archival, and read-access for B3 (Brazilian stock exchange) market data — BDI reports and tick-by-tick trades, downloaded to Amazon S3 and readable back as pandas DataFrames.**

---

## Overview

B3 (Brasil, Bolsa, Balcão) publishes market data through its [Daily Bulletin](https://www.b3.com.br/en_us/market-data-and-indices/data-services/market-data/reports/daily-bulletin/chapters-of-the-daily-bulletin/) — dozens of reports covering equities, fixed income, options, and securities lending — plus a tick-by-tick trade feed for equities and derivatives.

The tick-by-tick feed is the most valuable of these for quantitative research, but it comes with a practical constraint: **B3 only keeps the last 20 trading days available for public download.** After that window closes, the data is gone unless someone has already saved it. B3 does offer a paid, institutional-grade data service, but it isn't available to retail investors or individuals — and third-party vendors selling historical tick data are typically expensive, with no easy way to verify data quality against the source.

A look at public GitHub projects for B3 data collection turns up mostly narrow, single-purpose scripts — a one-off downloader for a specific report, or a script tied to one date range or one use case. `b3-data-collector` aims for something broader: a single, maintained pipeline covering the full range of daily BDI reports *and* both tick-by-tick feeds, with automated scheduling, tests, archival, and a read-access layer built in from the start — not a script you run once and adapt by hand each time.

`b3-data-collector` solves the retention problem with a small, reliable daily pipeline: download the reports and tick data as soon as they're published, validate them, and archive them to S3 — before the 20-day window closes. Once a day of data is archived, it's archived for good, and readable back into a DataFrame with a single function call — no need to re-download or re-process anything.

---

## Part of a Larger Ecosystem

This project was originally built as the data-ingestion layer for two other personal projects, where reliable historical B3 data is a shared dependency:

- **[ibexQuant](#)** — quantitative research infrastructure for strategy backtesting and live trading.
- **[DerivsLab](#)** — a research environment for derivatives pricing, volatility modeling, and portfolio-level risk simulation.

Rather than duplicate ingestion logic in both, it was extracted into this standalone repository — usable on its own, with no dependency on either parent project. Both projects consume this library's `reader` subpackage directly, rather than re-implementing S3 access or report parsing.

---

## Scope and Status

| Area | Status | Description |
|---|---|---|
| BDI reports — collection | Implemented | Download and archive to S3 for all enabled reports in the catalog (fixed income, securities lending, indices, and more). |
| Tick-by-tick — collection (RV / DERIV) | Implemented | Download → extract → partition → upload pipeline; both the raw ZIP archive *and* the final tick-level Parquet are archived to S3. |
| Read-access layer (`reader`) | Implemented | `read_bdi_report()` and `read_tick_by_tick()` pull already-collected data back from S3 into a single DataFrame, across one date or a range. |
| BDI report parsers | In progress | One parser implemented (`BTBLoanBalance`, see Examples below) out of 60+ reports in the catalog. Adding a parser for a new report is a small, self-contained, well-documented task — see Roadmap. |
| Scheduling | Implemented | Daily entrypoint scripts (`run_daily.py`), designed for cron / Task Scheduler. |
| Notifications | Not included | Removed from this public version to avoid exposing email/SMTP configuration surface — see Roadmap. |

---

## Repository Structure

```text
b3-data-collector/
├── src/
│   └── b3_data_collector/
│       ├── config.py           Environment-based settings (AWS credentials, bucket)
│       ├── paths.py            Local path resolution for downloads/raw/ticks
│       ├── common.py           Shared date-resolution helpers, S3 client/upload/download
│       │                       helpers, and status vocabulary (StageStatus) — reused by
│       │                       bdi/, tick_by_tick/, and reader/
│       ├── bdi/                 BDI report ingestion (catalog, client, uploader, pipeline)
│       │                       and parsing (parsers/ — one parser per report, by api_name)
│       ├── tick_by_tick/        Tick-by-tick ingestion (downloader, extractor, partitioner,
│       │                       pipeline) for both the RV and DERIV feeds
│       └── reader/              Read-access layer — pulls already-collected CSV/Parquet
│                               data back from S3 into pandas DataFrames
├── tests/                    Unit and integration tests (pytest + moto + requests-mock)
├── examples/                 Runnable examples using real (trimmed) sample data
├── scripts/                  One-off developer tooling (test fixture builder)
├── docs/                     Simplified BDI report catalog reference
├── data/                     Local working directory (gitignored)
└── logs/                     Local log output (gitignored)
```

---

## Architecture

**BDI reports — collection.** One HTTP call per report per date, uploaded directly to S3:

```text
B3 BDI export API  ->  fetch_report_csv()  ->  upload_csv()  ->  S3
                        (bdi/_client.py)       (bdi/_uploader.py)
```

**Tick-by-tick — collection.** A four-stage pipeline per feed (RV / DERIV). The raw ZIP is archived immutably as-is; the raw intermediate Parquet is disposable (reproducible from the ZIP at any time) and never leaves local disk; only the final, canonical ticks Parquet is archived to S3, alongside the ZIP:

```text
B3 distribution ZIP  ->  download  ->  extract  ->  partition  ->  upload ticks
                          (raw ZIP       (normalised   (tick-level    Parquet to S3
                           to S3)         Parquet,       Parquet)      (Hive-partitioned
                                          local only)                  by year)
```

**Read access.** The `reader` subpackage is the inverse of collection — it never talks to B3 directly, only to S3:

```text
S3 (CSV or Parquet)  ->  reader/_client.py  ->  bdi/parsers/ (BDI only)  ->  DataFrame
                         (download bytes,        (per-report parsing;
                          same partition keys     tick-by-tick has a single
                          used at upload time)     canonical schema, no
                                                    parsing needed)
```

Each collection stage records its own outcome (`SUCCESS` / `SKIPPED` / `UNAVAILABLE` / `FAILED`), aggregated into a run summary — so a single failed report or date doesn't stop the rest of the batch from processing. The reader mirrors this philosophy for missing data: a date with nothing in S3 is logged and skipped, not a hard failure — a partial DataFrame across a wide range is more useful than an all-or-nothing exception.

---

## Getting Started

### Requirements

- Python 3.10+
- Git
- An AWS account with an S3 bucket (only required for the upload and read steps — extraction and partitioning work without it)

### Setup

```bash
git clone https://github.com/gustavobjorgefo/b3-data-collector.git
cd b3-data-collector

python -m venv .venv

# Windows
.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -e .
pip install -r requirements-dev.txt
```

Copy `.env.example` to `.env` and fill in your AWS credentials and bucket name:

```text
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_S3_REGION=us-east-1
AWS_S3_BUCKET_B3=
```

To use this library from another project (e.g. `ibexQuant`, `DerivsLab`), install it as a dependency and copy the same four environment variables into that project's own `.env` — the library reads credentials from whichever process imports it, with no awareness of which project that is:

```bash
pip install git+https://github.com/gustavobjorgefo/b3-data-collector.git
```

### Running the collection pipelines

```python
from datetime import date
from b3_data_collector.bdi.pipeline import run_bdi_pipeline
from b3_data_collector.tick_by_tick.pipeline import run_pipeline
from b3_data_collector.tick_by_tick._feed import FeedType

# BDI reports for a single date
run_bdi_pipeline(dates="2026-06-30")

# Tick-by-tick, equities feed, over a date range
run_pipeline(dates=("2026-06-01", "2026-06-30"), feed=FeedType.RV)
```

Or schedule the daily entrypoints (`bdi/run_daily.py`, `tick_by_tick/run_daily.py`) via cron or Windows Task Scheduler — see the docstrings in each file for exact scheduling recommendations.

### Reading already-collected data

```python
from b3_data_collector.reader import read_bdi_report, read_tick_by_tick
from b3_data_collector.tick_by_tick._feed import FeedType

# A BDI report, over a date range — requires a registered parser (see below)
df = read_bdi_report("BTBLoanBalance", dates=("2026-06-01", "2026-06-30"))

# Tick-by-tick, equities feed, a single date
df = read_tick_by_tick(FeedType.RV, dates="2026-06-30")
```

Dates with nothing archived in S3 yet (not collected, or a holiday/weekend) are logged as a warning and skipped, rather than raising — you get back whatever exists for the range requested.

### Running tests

```bash
pytest -v
pytest --cov=b3_data_collector --cov-report=term-missing
```

105 tests, covering pure logic, network (mocked), S3 (mocked via `moto`), real-format file parsing, full pipeline integration, and the reader's read-back path.

---

## Examples

See [`examples/`](examples/) for two runnable scripts against real (trimmed) B3 data:

- **[`read_single_bdi_report.py`](examples/read_single_bdi_report.py)** — calls the library's own BDI parser registry (`b3_data_collector.bdi.parsers`) against a local sample CSV. The same parsers are used by `reader.read_bdi_report()` against bytes downloaded from S3 — this example just demonstrates the parsing step in isolation, without needing S3 access.
- **[`read_tick_by_tick_parquet.py`](examples/read_tick_by_tick_parquet.py)** — runs the real extraction/partitioning pipeline stages and reads the resulting tick-level Parquet.

## Report Catalog

The BDI catalog covers 60+ reports across fixed income, equities, options, and indices.

- [`docs/reports_catalog.md`](docs/reports_catalog.md) — simplified reference table (report name in Portuguese/English, section, API identifier).
- [`docs/report_descriptions_en.md`](docs/report_descriptions_en.md) / [`docs/report_descriptions_pt.md`](docs/report_descriptions_pt.md) — a short description of what each report contains, in English and Portuguese.

---

## Roadmap

- **BDI report parsers** — most reports don't have a dedicated parser yet. Adding one is: create `bdi/parsers/_<report_name>.py` with a `read_<report_name>(source) -> pd.DataFrame` function decorated with `@register_parser("<ApiName>")`, following the shape of `_btb_loan_balance.py`, and import the module in `bdi/parsers/__init__.py` — that's the entire integration surface, since `reader.read_bdi_report()` and the
example script both dispatch through the same registry. A good first contribution.
- **Storage backend abstraction** — S3 is currently hardcoded as the only archival/read destination. A `StorageBackend` interface (S3, local disk, others) would let users choose where files are archived and read from, without modifying pipeline or reader code.
- **Notifications** — a lightweight, optional run-summary notifier (email or webhook), reintroduced without hardcoding any specific provider.
- **DERIV example** — a `read_tick_by_tick_parquet.py`-style example for the derivatives feed (`sample_deriv.zip` is already available in `examples/sample_data/`).

---

## Disclaimer

This repository is for research and educational purposes only. It is not affiliated with, endorsed by, or officially connected to B3 (Brasil, Bolsa, Balcão). Market data made available through this project originates from B3's own public Daily Bulletin; users are responsible for complying with B3's terms of use for any data collected. This project does not constitute financial advice.

---

## Status

`b3-data-collector` is an independent, actively maintained project, originally extracted from a larger personal quant research infrastructure. Feedback, questions, and contributions are welcome — feel free to open an issue.