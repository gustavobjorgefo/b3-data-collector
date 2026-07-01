# src/b3_data_collector/paths.py

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]   # raiz do repo
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"

PATHS_B3 = {
    "rv_downloads"    : DATA_DIR / "tick_by_tick" / "rv" / "downloads",
    "rv_raw_parquet"  : DATA_DIR / "tick_by_tick" / "rv" / "raw",
    "rv_ticks"        : DATA_DIR / "tick_by_tick" / "rv" / "ticks",
    "deriv_downloads" : DATA_DIR / "tick_by_tick" / "deriv" / "downloads",
    "deriv_raw_parquet": DATA_DIR / "tick_by_tick" / "deriv" / "raw",
    "deriv_ticks"     : DATA_DIR / "tick_by_tick" / "deriv" / "ticks",
}

for path in [*PATHS_B3.values(), LOGS_DIR]:
    path.mkdir(parents=True, exist_ok=True)