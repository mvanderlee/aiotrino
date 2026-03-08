#!/usr/bin/env python3
"""Simple benchmark using command-line arguments for configuration."""

import asyncio
import time
import csv
import os
import sys
import argparse
import aiotrino

def parse_args():
    parser = argparse.ArgumentParser(description="Run a single Trino benchmark and append results to CSV.")
    parser.add_argument("--host", type=str, default="localhost", help="Trino server host")
    parser.add_argument("--port", type=int, default=8092, help="Trino server port")
    parser.add_argument("--user", type=str, default="test", help="Trino user")
    parser.add_argument("--query", type=str, required=True, help="SQL query to execute")
    parser.add_argument("--encoding", type=str, default="arrow+zstd", help="Encoding (e.g., arrow+zstd, json+zstd)")
    parser.add_argument("--outdir", type=str, required=True, help="Directory to save CSV results")
    return parser.parse_args()

async def run_benchmark():
    """Run the benchmark with current configuration."""
    args = parse_args()
    
    conn = aiotrino.dbapi.Connection(
        host=args.host,
        port=args.port,
        user=args.user,
        source="benchmark",
        max_attempts=1,
        encoding=args.encoding
    )
    
    cursor_style = "segment" if args.encoding.startswith("arrow") else "row"
    
    print(f"🚀 Running benchmark with {args.encoding} encoding...")
    print(f"📝 Query: {args.query}")
    print()
    
    async with await conn.cursor(cursor_style=cursor_style) as cur:
        start_time = time.perf_counter()
        await cur.execute(args.query)
        
        if args.encoding.startswith("arrow"):
            data = await cur.fetchall_arrow()
        else:
            data = await cur.fetchall()
        actual_rows = len(data)
        
        elapsed_time = time.perf_counter() - start_time
        throughput = actual_rows / elapsed_time
        
        print(f"📊 {actual_rows:>8,} rows in {elapsed_time*1000:>6.0f} ms ({throughput:>8.0f} rows/s)")
        
        result = {
            'rows': actual_rows,
            'time_ms': elapsed_time * 1000,
        }
    
    await conn.close()
    
    # Append to CSV (write header if file doesn't exist)
    outdir_abs = os.path.abspath(args.outdir)
    os.makedirs(outdir_abs, exist_ok=True)
    filename = os.path.join(outdir_abs, f"benchmark_results_{args.encoding.replace('+', '_')}.csv")
    fieldnames = ['rows', 'time_ms']
    file_exists = os.path.exists(filename)
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: result[k] for k in fieldnames})
    
    print()
    print(f"📊 Results exported to {filename}")

def main():
    """Main function."""
    try:
        asyncio.run(run_benchmark())
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 