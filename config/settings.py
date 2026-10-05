"""Central configuration for the hybrid order data pipeline."""
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", PROJECT_ROOT / "reports"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"

# Threshold in MB for routing between Python Batch and PySpark (Section 6.2: 200 MB)
# orders_small.csv (~0.4 MB) <= 200 MB -> python_batch
# orders_huge_mixed_quality.csv (~12,650 MB) > 200 MB -> pyspark
# Can be overridden via SMALL_FILE_THRESHOLD_MB env variable or CLI argument
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "midterm_pipeline")
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "1500"))

for directory in (DATA_DIR, REPORTS_DIR, RAW_DIR, PROCESSED_DIR, SCREENSHOTS_DIR):
    directory.mkdir(parents=True, exist_ok=True)


def threshold_bytes() -> int:
    return int(SMALL_FILE_THRESHOLD_MB * 1024 * 1024)


def get_short_path(long_path: str | Path) -> str:
    """Return the Windows 8.3 short path to avoid command-line parsing bugs with spaces or parentheses."""
    path_str = str(long_path)
    if sys.platform != "win32":
        return path_str
    try:
        import ctypes
        from ctypes import wintypes
        _GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
        _GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        _GetShortPathNameW.restype = wintypes.DWORD
        buf = ctypes.create_unicode_buffer(1024)
        length = _GetShortPathNameW(path_str, buf, 1024)
        if 0 < length < 1024:
            return buf.value
    except Exception:
        pass
    return path_str


def setup_spark_windows_env() -> None:
    """Configure PySpark environment variables on Windows to prevent parenthesis/space syntax errors."""
    if sys.platform != "win32":
        return
    try:
        import pyspark
        spark_dir = Path(pyspark.__file__).resolve().parent
        short_spark = get_short_path(spark_dir)
        short_py = get_short_path(sys.executable)
        os.environ.setdefault("SPARK_HOME", short_spark)
        os.environ.setdefault("PYSPARK_PYTHON", short_py)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", short_py)
    except Exception:
        pass
