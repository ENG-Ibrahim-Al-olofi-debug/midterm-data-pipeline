"""Create a deterministic CSV sample without loading the entire file into memory (Section 6.1)."""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from config.settings import DATA_DIR


def create_sample(
    source: str | Path,
    output: str | Path | None = None,
    rows: int = 100_000,
) -> int:
    """Extract first N rows from source CSV to output CSV using streaming (no full-file in-memory loading)."""
    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    if output is None:
        output_path = DATA_DIR / f"{source_path.stem}_sample_{rows}.csv"
    else:
        output_path = Path(output)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with source_path.open("r", encoding="utf-8-sig", newline="") as src_h, \
         output_path.open("w", encoding="utf-8", newline="") as out_h:
        reader = csv.reader(src_h)
        writer = csv.writer(out_h)

        # Read and write header
        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            return 0

        for row in reader:
            if count >= rows:
                break
            writer.writerow(row)
            count += 1

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(f"[SUCCESS] Successfully extracted {count:,} rows from {source_path.name} to {output_path}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract a small sample from a large CSV file using streaming (Section 6.1)"
    )
    # Support both named flags matching doc and positional arguments
    parser.add_argument(
        "--input", "-i",
        dest="input_file",
        default=None,
        help="Path to input dirty CSV (e.g. data/orders_huge_mixed_quality.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        dest="output_file",
        default=None,
        help="Path to destination sample CSV (defaults to data/orders_small.csv)",
    )
    parser.add_argument(
        "--rows", "-r",
        type=int,
        default=1000,
        help="Number of data rows to sample (default: 1000)",
    )
    parser.add_argument("positional_args", nargs="*", help="Optional positional [source, output]")

    args = parser.parse_args()

    # Resolve source path
    source = args.input_file
    output = args.output_file

    if not source and args.positional_args:
        source = args.positional_args[0]
        if len(args.positional_args) > 1:
            output = args.positional_args[1]

    if not source:
        # Default fallback
        default_huge = DATA_DIR / "orders_huge_mixed_quality.csv"
        if default_huge.exists():
            source = default_huge
        else:
            parser.error("Please provide --input <path_to_csv>")

    if not output:
        output = DATA_DIR / "orders_small.csv"

    create_sample(source, output, rows=args.rows)


if __name__ == "__main__":
    main()
