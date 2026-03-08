#!/usr/bin/env python3
"""
Example 3: Benchmark -- JDBC vs JSON+ZSTD vs Arrow+ZSTD
========================================================
Run the same SELECT * query with all three transports and compare
end-to-end fetch time (query execution + network + deserialization).

JDBC is run via a Java subprocess; JSON and Arrow use aiotrino.

Usage:
    python 03_benchmark_json_vs_arrow.py                # uses tpch (no setup needed)
    python 03_benchmark_json_vs_arrow.py --source iceberg  # uses pre-materialized Iceberg tables
    python 03_benchmark_json_vs_arrow.py --no-jdbc       # skip JDBC (if Java not available)
"""

import argparse
import asyncio
import os
import resource
import shutil
import subprocess
import re
import time
import aiotrino


TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8085"))


def get_cpu_times() -> float:
    """Return total CPU seconds (user + system) for this process and all children."""
    r_self = resource.getrusage(resource.RUSAGE_SELF)
    r_children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return (r_self.ru_utime + r_self.ru_stime +
            r_children.ru_utime + r_children.ru_stime)

SOURCES = {
    "tpch":    "tpch.sf1",            # on-the-fly, no setup needed
    "iceberg": "iceberg.tpch_sf10",   # pre-materialized via generate-data script
}


async def bench_json_zstd(query: str) -> tuple[int, float]:
    """Fetch with JSON+ZSTD using the standard row cursor."""
    conn = aiotrino.dbapi.Connection(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="demo",
        source="bench-json",
        encoding="json+zstd",
    )
    async with await conn.cursor(cursor_style="row") as cur:
        t0 = time.perf_counter()
        await cur.execute(query)
        rows = await cur.fetchall()
        elapsed = time.perf_counter() - t0
    await conn.close()
    return len(rows), elapsed


async def bench_arrow_zstd(query: str) -> tuple[int, float]:
    """Fetch with Arrow+ZSTD using the segment cursor."""
    conn = aiotrino.dbapi.Connection(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="demo",
        source="bench-arrow",
        encoding="arrow+zstd",
    )
    async with await conn.cursor(cursor_style="segment") as cur:
        t0 = time.perf_counter()
        await cur.execute(query)
        table = await cur.fetchall_arrow()
        elapsed = time.perf_counter() - t0
    await conn.close()
    return len(table), elapsed


def bench_jdbc(source: str) -> tuple[int, float] | None:
    """Run the Java JDBC benchmark as a subprocess, parse output."""
    if not shutil.which("java"):
        return None

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    lib_dir = os.path.join(project_dir, "lib")
    jdbc_jar = os.path.join(lib_dir, "trino-jdbc.jar")

    # Look for compiled class in /app/java (Docker) or examples/ (local)
    java_dir = None
    for candidate in ["/app/java", os.path.join(project_dir, "examples")]:
        if os.path.exists(os.path.join(candidate, "JdbcBenchmark.class")):
            java_dir = candidate
            break

    if not java_dir or not os.path.exists(jdbc_jar):
        return None

    sep = ":" if os.name != "nt" else ";"
    cp = f"{java_dir}{sep}{jdbc_jar}"
    env = {
        **os.environ,
        "TRINO_HOST": TRINO_HOST,
        "TRINO_PORT": str(TRINO_PORT),
    }
    result = subprocess.run(
        ["java", "-cp", cp, "JdbcBenchmark", source],
        capture_output=True, text=True, env=env, timeout=1800,
    )
    output = result.stdout + result.stderr
    print(output)

    # Parse: "JDBC fetched 6,001,215 rows in 8.3 s (723,277 rows/sec)"
    m = re.search(r"fetched ([\d,]+) rows in ([\d.]+) s", output)
    if m:
        rows = int(m.group(1).replace(",", ""))
        elapsed = float(m.group(2))
        return rows, elapsed
    return None


async def main(source: str, skip_jdbc: bool):
    schema = SOURCES[source]
    query = f"SELECT * FROM {schema}.lineitem"

    print(f"Source:   {source} ({schema})")
    print(f"Query:    {query}")
    print(f"Trino:    {TRINO_HOST}:{TRINO_PORT}")
    print()

    # --- JDBC ---
    jdbc_rows, jdbc_time, jdbc_cpu, jdbc_throughput = None, None, None, None
    if not skip_jdbc:
        print("--- Running JDBC benchmark ---\n")
        cpu0 = get_cpu_times()
        jdbc_result = bench_jdbc(source)
        jdbc_cpu = get_cpu_times() - cpu0
        if jdbc_result:
            jdbc_rows, jdbc_time = jdbc_result
            jdbc_throughput = jdbc_rows / jdbc_time
        else:
            jdbc_cpu = None
            print("  (JDBC not available -- skipping)")

    # --- JSON+ZSTD ---
    print("\n--- Running JSON+ZSTD benchmark ---\n")
    cpu0 = get_cpu_times()
    json_rows, json_time = await bench_json_zstd(query)
    json_cpu = get_cpu_times() - cpu0
    json_throughput = json_rows / json_time

    # --- Arrow+ZSTD ---
    print("\n--- Running Arrow+ZSTD benchmark ---\n")
    cpu0 = get_cpu_times()
    arrow_rows, arrow_time = await bench_arrow_zstd(query)
    arrow_cpu = get_cpu_times() - cpu0
    arrow_throughput = arrow_rows / arrow_time

    # --- Results ---
    w = 78
    print("=" * w)
    print(f"{'Encoding':<16} {'Rows':>12} {'Time (s)':>10} {'CPU (s)':>10} {'CPU Eff':>9} {'Rows/sec':>14}")
    print("-" * w)
    if jdbc_rows is not None:
        print(f"{'JDBC':<16} {jdbc_rows:>12,} {jdbc_time:>10.1f} {jdbc_cpu:>10.1f} {jdbc_cpu/jdbc_time:>8.2f}x {jdbc_throughput:>14,.0f}")
    print(f"{'JSON+ZSTD':<16} {json_rows:>12,} {json_time:>10.1f} {json_cpu:>10.1f} {json_cpu/json_time:>8.2f}x {json_throughput:>14,.0f}")
    print(f"{'Arrow+ZSTD':<16} {arrow_rows:>12,} {arrow_time:>10.1f} {arrow_cpu:>10.1f} {arrow_cpu/arrow_time:>8.2f}x {arrow_throughput:>14,.0f}")
    print("=" * w)

    print(f"\nArrow+ZSTD vs JSON+ZSTD: {json_time / arrow_time:.1f}x faster")
    if jdbc_time:
        print(f"Arrow+ZSTD vs JDBC:      {jdbc_time / arrow_time:.1f}x faster")

    print(
        "\nCPU Eff = CPU time / wall time. Values > 1.0x indicate parallel CPU use.\n"
        "Arrow+ZSTD leverages a thread pool for GIL-free parallel deserialization.\n"
        "\n"
        "Note: On a single-node local setup the speedup is modest.\n"
        "With larger clusters and multi-million-row queries,\n"
        "Arrow+ZSTD can be 50x+ faster due to GIL-free parallel\n"
        "deserialization and zero-copy columnar transfer."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark JDBC vs JSON vs Arrow fetch from Trino")
    parser.add_argument(
        "--source",
        choices=SOURCES.keys(),
        default="tpch",
        help="Data source: 'tpch' (built-in, no setup) or 'iceberg' (pre-materialized)",
    )
    parser.add_argument(
        "--no-jdbc",
        action="store_true",
        help="Skip the JDBC benchmark (useful if Java is not installed)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.source, args.no_jdbc))
