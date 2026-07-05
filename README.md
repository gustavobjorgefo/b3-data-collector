# b3-data-collector

**Automated collection and archival of B3 (Brazilian stock exchange) market data — BDI reports and tick-by-tick trades, downloaded and archived to Amazon S3.**

---

## Overview

B3 (Brasil, Bolsa, Balcão) publishes market data through its [Daily Bulletin](https://www.b3.com.br/en_us/market-data-and-indices/data-services/market-data/reports/daily-bulletin/chapters-of-the-daily-bulletin/) — dozens of reports covering equities, fixed income, options, and securities lending — plus a tick-by-tick trade feed for equities and derivatives.

The tick-by-tick feed is the most valuable of these for quantitative research, but it comes with a practical constraint: **B3 only keeps the last 20 trading days available for public download.** After that window closes, the data is gone unless someone has already saved it. B3 does offer a paid, institutional-grade data service, but it isn't available to retail investors or individuals — and third-party vendors selling historical tick data are typically expensive, with no easy way to verify data quality against the source.

A look at public GitHub projects for B3 data collection turns up mostly narrow, single-purpose scripts — a one-off downloader for a specific report, or a script tied to one date range or one use case. `b3-data-collector` aims for something broader: a single, maintained pipeline covering the full range of daily BDI reports *and* both tick-by-tick feeds, with automated scheduling, tests, and archival built in from the start — not a script you run once and adapt by hand each time.

`b3-data-collector` solves the retention problem with a small, reliable daily pipeline: download the reports and tick data as soon as they're published, validate them, and archive them to S3 — before the 20-day window closes. Once a day of data is archived, it's archived for good.

---

## Part of a Larger Ecosystem

This project was originally built as the data-ingestion layer for two other personal projects, where reliable historical B3 data is a shared dependency:

- **[ibexQuant](#)** — quantitative research infrastructure for strategy backtesting and live trading.
- **[DerivsLab](#)** — a research environment for derivatives pricing, volatility modeling, and portfolio-level risk simulation.

Rather than duplicate ingestion logic in both, it was extracted into this standalone repository — usable on its own, with no dependency on either parent project.

---

## Scope and Status

| Area | Status | Description |
|---|---|---|
| BDI reports | Implemented | Download and archive to S3 for all enabled reports in the catalog (fixed income, securities lending, indices, and more). |
| Tick-by-tick (RV — equities) | Implemented | Download → extract → partition pipeline, archived to S3 as Parquet. |
| Tick-by-tick (DERIV — derivatives) | Implemented | Same pipeline as RV, for the derivatives feed. |
| Report-specific readers | In progress | One example reader implemented (see Examples below); most of the 60+ BDI reports don't have a dedicated parser yet — this is intentionally left open (see Roadmap). |
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
│       ├── common.py           Shared status vocabulary (StageStatus)
│       ├── bdi/                 BDI report ingestion (catalog, client, uploader, pipeline)
│       └── tick_by_tick/        Tick-by-tick ingestion (downloader, extractor, partitioner, pipeline)
├── tests/                    Unit and integration tests (pytest + moto + requests-mock)
├── examples/                 Runnable examples using real (trimmed) sample data
├── scripts/                  One-off developer tooling (test fixture builder)
├── docs/                     Simplified BDI report catalog reference
├── data/                     Local working directory (gitignored)
└── logs/                     Local log output (gitignored)
```

---

## Architecture

**BDI reports** — one HTTP call per report per date, uploaded directly to S3:

```text
B3 BDI export API  ->  fetch_report_csv()  ->  upload_csv()  ->  S3
                        (bdi/_client.py)       (bdi/_uploader.py)
```

**Tick-by-tick** — a three-stage pipeline per feed (RV / DERIV):

```text
B3 distribution ZIP  ->  download  ->  extract  ->  partition  ->  S3
                          (raw ZIP)     (normalised   (tick-level
                                          Parquet)       Parquet)
```

Each stage records its own outcome (`SUCCESS` / `SKIPPED` / `UNAVAILABLE` / `FAILED`), aggregated into a run summary — so a single failed report or date doesn't stop the rest of the batch from processing.

---

## Getting Started

### Requirements

- Python 3.10+
- Git
- An AWS account with an S3 bucket (only required for the upload step — extraction and partitioning work without it)

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

### Running the pipelines

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

### Running tests

```bash
pytest -v
pytest --cov=b3_data_collector --cov-report=term-missing
```

79 tests, covering pure logic, network (mocked), S3 (mocked via `moto`), real-format file parsing, and full pipeline integration.

---

## Examples

See [`examples/`](examples/) for two runnable scripts against real (trimmed) B3 data:

- **[`read_single_bdi_report.py`](examples/read_single_bdi_report.py)** — parses a real BDI report (securities-lending rates), using an extensible per-report parser registry.
- **[`read_tick_by_tick_parquet.py`](examples/read_tick_by_tick_parquet.py)** — runs the real extraction/partitioning pipeline stages and reads the resulting tick-level Parquet.

## Report Catalog

The BDI catalog covers 60+ reports across fixed income, equities, options, and indices. A simplified reference (report name, section, API identifier, publication time) is available in [`docs/reports_catalog.md`](docs/reports_catalog.md).

---

## Roadmap

- **Report-specific readers** — most BDI reports don't have a dedicated parser yet (see Examples above for the pattern). Contributions adding a reader for a specific report are welcome.
- **Storage backend abstraction** — S3 is currently hardcoded as the only archival destination. A `StorageBackend` interface (S3, local disk, others) would let users choose where files are archived without modifying the pipeline code.
- **Notifications** — a lightweight, optional run-summary notifier (email or webhook), reintroduced without hardcoding any specific provider.
- **DERIV example** — a `read_tick_by_tick_parquet.py`-style example for the derivatives feed (`sample_deriv.zip` is already available in `examples/sample_data/`). 

---

## Disclaimer

This repository is for research and educational purposes only. It is not affiliated with, endorsed by, or officially connected to B3 (Brasil, Bolsa, Balcão). Market data made available through this project originates from B3's own public Daily Bulletin; users are responsible for complying with B3's terms of use for any data collected. This project does not constitute financial advice.

---

## Status

`b3-data-collector` is an independent, actively maintained project, originally extracted from a larger personal quant research infrastructure. Feedback, questions, and contributions are welcome — feel free to open an issue.