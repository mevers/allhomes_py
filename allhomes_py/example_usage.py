"""Example script for testing `allhomes_py`.

Run this script from the package directory:
    python3 example_usage.py

If you move it outside the package, use the package import path instead.
"""

from __future__ import annotations

import warnings

from core import get_past_sales_data

def main() -> None:
    suburb = "Belconnen, ACT"
    #suburb = "Abbotsford, NSW"
    year = 2023
    max_entries = 5000

    warnings.filterwarnings("default")
    print(f"Fetching past sales data for {suburb!r} with max_entries={max_entries}...")

    df = get_past_sales_data(suburb, year=year, max_entries=max_entries)

    print(f"Retrieved {df.height} rows and {len(df.columns)} columns.")
    print("First rows:")
    print(df)


if __name__ == "__main__":
    main()
