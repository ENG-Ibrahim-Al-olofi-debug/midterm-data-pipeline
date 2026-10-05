"""Persist pipeline metrics for demo and assessment evidence."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def save_results(metrics: dict[str, Any], reports_dir: str | Path) -> tuple[Path, Path]:
    directory = Path(reports_dir)
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / "results.json"
    md_path = directory / "results.md"
    json_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    lines = ["# Pipeline Run Results", "", "| Metric | Value |", "|---|---:|"]
    for key, value in metrics.items():
        if key in ("error_case_counts", "correction_case_counts") and isinstance(value, dict):
            value = ", ".join(f"{name}={count}" for name, count in value.items()) or "None"
        lines.append(f"| `{key}` | `{value}` |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path
