#!/usr/bin/env python3
"""
Example 2: Streaming Arrow Segments
====================================
Fetch query results segment-by-segment as Arrow tables.
This allows processing large datasets without loading
everything into memory at once.

Usage:
    python 02_streaming_arrow.py                # uses tpch (no setup needed)
    python 02_streaming_arrow.py --source iceberg  # uses pre-materialized Iceberg tables
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

    conn = aiotrino.dbapi.Connection(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="demo",
        source="arrow-streaming-demo",
        encoding="arrow+zstd",
    )

    async with await conn.cursor(cursor_style="segment") as cur:
        query = f"SELECT * FROM {schema}.lineitem"

        print(f"Source:   {source} ({schema})")
        print(f"Query:    {query}")
        print(f"Encoding: arrow+zstd")
        print()
        print("Streaming Arrow segments...")
        await cur.execute(query)

        total_rows = 0
        segment_num = 0

        # fetchone_arrow() returns one Arrow Table per spooling segment
        while (arrow_segment := await cur.fetchone_arrow()) is not None:
            segment_num += 1
            rows_in_segment = len(arrow_segment)
            total_rows += rows_in_segment

            # Memory footprint of this segment
            mem_bytes = arrow_segment.nbytes
            mem_mb = mem_bytes / (1024 * 1024)

            print(
                f"  Segment {segment_num:>3}: "
                f"{rows_in_segment:>8,} rows, "
                f"{mem_mb:>6.2f} MB"
            )

            # Process the segment here -- e.g. write to Parquet, aggregate, etc.
            # The previous segment can be garbage collected while the next is fetched.

        print(f"\nDone. {total_rows:,} total rows across {segment_num} segments.")

    await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Arrow segments from Trino")
    parser.add_argument(
        "--source",
        choices=SOURCES.keys(),
        default="tpch",
        help="Data source: 'tpch' (built-in, no setup) or 'iceberg' (pre-materialized)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.source))
