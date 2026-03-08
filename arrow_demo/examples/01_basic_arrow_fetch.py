#!/usr/bin/env python3
"""
Example 1: Basic Arrow Fetch
=============================
Connect to Trino with Arrow+ZSTD encoding and fetch query results
directly as an Apache Arrow table -- zero-copy, columnar, and fast.

Usage:
    python 01_basic_arrow_fetch.py                # uses tpch (no setup needed)
    python 01_basic_arrow_fetch.py --source iceberg  # uses pre-materialized Iceberg tables
"""

import argparse
import asyncio
import os
import aiotrino


TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8085"))

SOURCES = {
    "tpch":    "tpch.sf1",            # on-the-fly, no setup needed
    "iceberg": "iceberg.tpch_sf10",   # pre-materialized via generate-data script
}


async def main(source: str):
    schema = SOURCES[source]

    # Connect with Arrow+ZSTD encoding
    conn = aiotrino.dbapi.Connection(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="demo",
        source="arrow-demo",
        encoding="arrow+zstd",
    )

    # Use the "segment" cursor style for Arrow support
    async with await conn.cursor(cursor_style="segment") as cur:
        query = f"SELECT * FROM {schema}.lineitem"

        print(f"Source:   {source} ({schema})")
        print(f"Query:    {query}")
        print(f"Encoding: arrow+zstd")
        print()
        await cur.execute(query)

        # Fetch entire result as a single PyArrow Table
        arrow_table = await cur.fetchall_arrow()

        print(f"--- Result ---")
        print(f"Rows:    {len(arrow_table):,}")
        print(f"Columns: {arrow_table.num_columns}")
        print(f"\n--- Schema ---")
        print(arrow_table.schema)

        # Convert to pandas for pretty display
        df = arrow_table.to_pandas()
        print(f"\n--- Sample (first 10 rows) ---")
        print(df.head(10).to_string(index=False))

    await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Basic Arrow fetch from Trino")
    parser.add_argument(
        "--source",
        choices=SOURCES.keys(),
        default="tpch",
        help="Data source: 'tpch' (built-in, no setup) or 'iceberg' (pre-materialized)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.source))
