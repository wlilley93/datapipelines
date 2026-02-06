#!/usr/bin/env python3
"""
Product Analysis — Fetch and analyse PandaDoc line items with real HubSpot prices.

Examples:
    # Specific SKU + date range
    python financial_analysis/product_analysis.py \\
        --sku 001-adgm-incorp-001 --start-date 2026-01-01 --end-date 2026-01-31

    # Multiple SKUs
    python financial_analysis/product_analysis.py \\
        --sku 001-adgm-incorp-001 001-adgm-subsc-006 --start-date 2025-01-01 --end-date 2025-12-31

    # All products in a date range
    python financial_analysis/product_analysis.py --start-date 2025-01-01 --end-date 2025-12-31

    # All products, all dates
    python financial_analysis/product_analysis.py

    # Force re-fetch from DB + HubSpot API
    python financial_analysis/product_analysis.py --refresh --start-date 2026-01-01 --end-date 2026-01-31
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Ensure project root is on sys.path so `connectors` package is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from helper.data_fetcher import fetch_line_items_with_prices
from helper.excel_formatter import generate_report_excel

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_OUTPUT = str(DEFAULT_OUTPUT_DIR / "product_analysis.xlsx")


def _slugify_for_filename(text: str, *, max_len: int = 80) -> str:
    """Return a filesystem-friendly slug.

    Keeps a-z, 0-9 and hyphens. Converts whitespace/underscores to hyphens.
    """
    text = (text or "").strip().lower()
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"[^a-z0-9-]+", "", text)
    text = re.sub(r"-+", "-", text).strip("-")
    if not text:
        return "unknown"
    return text[:max_len]


def _build_default_report_filename(
    *, sku_label: str, start_date: Optional[str], end_date: Optional[str]
) -> str:
    sku_part = _slugify_for_filename(sku_label)

    if start_date and end_date:
        date_part = f"{start_date}_to_{end_date}"
    elif start_date:
        date_part = f"from_{start_date}"
    elif end_date:
        date_part = f"to_{end_date}"
    else:
        date_part = "all_dates"

    date_part = _slugify_for_filename(date_part, max_len=40)
    return f"product_analysis__{sku_part}__{date_part}.xlsx"


def _resolve_output_path(
    output_arg: str,
    *,
    sku_label: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Path:
    """Resolve output path.

    If --output is the default path OR points to a directory, auto-name the
    report file using SKU/date context.
    """
    raw = (output_arg or "").strip()
    output_path = Path(raw).expanduser() if raw else Path(DEFAULT_OUTPUT)

    is_explicit_dir_hint = raw.endswith(("/", os.sep))
    is_directory = output_path.exists() and output_path.is_dir()
    uses_default = os.path.abspath(str(output_path)) == os.path.abspath(DEFAULT_OUTPUT)

    if uses_default or is_directory or is_explicit_dir_hint:
        out_dir = (
            output_path
            if (is_directory or is_explicit_dir_hint)
            else DEFAULT_OUTPUT_DIR
        )
        filename = _build_default_report_filename(
            sku_label=sku_label,
            start_date=start_date,
            end_date=end_date,
        )
        return (out_dir / filename).resolve()

    # User provided a file path; if they omitted the extension, add .xlsx.
    if output_path.suffix.lower() != ".xlsx":
        if output_path.suffix == "":
            output_path = output_path.with_suffix(".xlsx")
    return output_path.resolve()


def _parse_sku_args(sku_list):
    """Flatten space-separated and comma-separated SKU arguments."""
    skus = []
    for arg in sku_list:
        skus.extend(s.strip() for s in arg.split(",") if s.strip())
    return skus


def main():
    parser = argparse.ArgumentParser(
        description="Product Analysis — PandaDoc line items with real HubSpot prices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--sku",
        nargs="+",
        default=None,
        help="Target SKU(s). Space or comma-separated. Omit to include all products.",
    )
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output .xlsx path, or a directory to auto-name the report",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=None,
        help="Exclude items whose name contains any of these terms (case-insensitive). E.g. --exclude Renewal",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force re-fetch from DB + HubSpot API (ignore cache)",
    )

    args = parser.parse_args()

    # Step 1: Fetch data
    print("Step 1: Fetching line item data with actual prices...")
    df = fetch_line_items_with_prices(
        start_date=args.start_date,
        end_date=args.end_date,
        refresh=args.refresh,
    )
    print(f"  Total line items: {len(df)}")

    if len(df) == 0:
        print("No data found for the specified date range. Exiting.")
        return

    # Step 2: Apply date filter (belt-and-suspenders if loaded from broader cache)
    if args.start_date or args.end_date:
        df["date_completed"] = pd.to_datetime(df["date_completed"])
        if args.start_date:
            start_ts = pd.Timestamp(args.start_date, tz="UTC")
            if df["date_completed"].dt.tz is None:  # type: ignore[attr-defined]
                df["date_completed"] = df["date_completed"].dt.tz_localize("UTC")  # type: ignore[attr-defined]
            df = df[df["date_completed"] >= start_ts]
        if args.end_date:
            end_ts = pd.Timestamp(args.end_date, tz="UTC")
            if df["date_completed"].dt.tz is None:  # type: ignore[attr-defined]
                df["date_completed"] = df["date_completed"].dt.tz_localize("UTC")  # type: ignore[attr-defined]
            df = df[df["date_completed"] <= end_ts]
        print(f"  After date filter: {len(df)}")

    # Step 3: Apply SKU filter
    target_skus = None
    if args.sku:
        target_skus = _parse_sku_args(args.sku)
        target_skus_lower = [s.lower() for s in target_skus]
        df = df[df["sku"].str.lower().isin(target_skus_lower)].copy()  # type: ignore[attr-defined]
        print(f"  After SKU filter ({', '.join(target_skus)}): {len(df)}")

    # Step 4: Exclude items by name
    if args.exclude:
        before = len(df)
        exclude_lower = [t.lower() for t in args.exclude]
        mask = df["item_name"].str.lower().apply(
            lambda name: not any(term in name for term in exclude_lower)
        )
        df = df[mask].copy()
        print(f"  After excluding {', '.join(args.exclude)}: {len(df)} (removed {before - len(df)})")

    if len(df) == 0:
        print("No data matches the specified filters. Exiting.")
        return

    # Step 5: Generate report
    sku_label = ", ".join(target_skus) if target_skus else "All Products"
    print(f"\nStep 2: Generating Excel report for: {sku_label}")

    resolved_output = _resolve_output_path(
        args.output,
        sku_label=sku_label,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    output_path = generate_report_excel(df, str(resolved_output), sku_label=sku_label)
    print(f"\nDone! Report saved to: {output_path}")
    print(f"  {len(df)} line items | Avg price: ${df['price'].mean():.2f}")


if __name__ == "__main__":
    main()
