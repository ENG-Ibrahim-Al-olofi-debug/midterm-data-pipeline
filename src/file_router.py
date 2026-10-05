"""Route input files to the appropriate processing engine."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import SMALL_FILE_THRESHOLD_MB


@dataclass(frozen=True)
class RouteDecision:
    file_name: str
    file_size_bytes: int
    file_size_mb: float
    threshold_mb: float
    engine_used: str
    reason: str


def route_file(path: str | Path, threshold_mb: float = SMALL_FILE_THRESHOLD_MB) -> RouteDecision:
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"Input file does not exist: {source}")
    size = source.stat().st_size
    size_mb = size / (1024 * 1024)
    if size_mb <= threshold_mb:
        engine = "python_batch"
        reason = f"file_size_mb={size_mb:.4f} <= threshold_mb={threshold_mb:.4f}"
    else:
        engine = "pyspark"
        reason = f"file_size_mb={size_mb:.4f} > threshold_mb={threshold_mb:.4f}"
    return RouteDecision(source.name, size, round(size_mb, 6), threshold_mb, engine, reason)


def decision_dict(decision: RouteDecision) -> dict:
    return asdict(decision)
