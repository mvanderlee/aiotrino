#!/bin/bash

# Simple script to run specific Trino benchmarks
set -e

# Parse command line arguments
HOST="localhost"
PORT="8092"
USER="test"
QUERY=""
OUTDIR=""
BENCHMARKS="python-json,python-arrow,java"

usage() {
    echo "Usage: $0 --host HOST --port PORT --user USER --query \"SQL\" --outdir /path/to/results [--benchmarks TYPES]"
    echo ""
    echo "Options:"
    echo "  --host        Trino server host (default: localhost)"
    echo "  --port        Trino server port (default: 8092)"
    echo "  --user        Trino user (default: test)"
    echo "  --query       SQL query to execute (required)"
    echo "  --outdir      Output directory for results (required)"
    echo "  --benchmarks  Comma-separated list (default: python-json,python-arrow,java)"
    echo ""
    echo "Benchmark types: python-json, python-arrow, java"
    echo ""
    echo "Examples:"
    echo "  $0 --host server --port 8080 --user test --query \"SELECT 1\" --outdir results/test"
    echo "  $0 --benchmarks \"python-arrow,java\" --query \"SELECT 1\" --outdir results/test"
    echo "  $0 --benchmarks \"python-arrow\" --query \"SELECT 1\" --outdir results/test"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --query)
            QUERY="$2"
            shift 2
            ;;
        --outdir)
            OUTDIR="$2"
            shift 2
            ;;
        --benchmarks)
            BENCHMARKS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [[ -z "$QUERY" || -z "$OUTDIR" ]]; then
    echo "❌ Missing required arguments"
    usage
fi

echo "🚀 Running benchmarks: $BENCHMARKS"
echo "📝 Host: $HOST:$PORT"
echo "👤 User: $USER"
echo "🗂️  Output: $OUTDIR"
echo "📋 Query: $QUERY"
echo

# Create output directory
mkdir -p "$OUTDIR"

# Check which benchmarks to run
if [[ "$BENCHMARKS" == *"python-json"* ]]; then
    echo "=== Python JSON+ZSTD Benchmark ==="
    python3 run_benchmark.py --host "$HOST" --port "$PORT" --user "$USER" --encoding json+zstd --query "$QUERY" --outdir "$OUTDIR"
    echo
fi

if [[ "$BENCHMARKS" == *"python-arrow"* ]]; then
    echo "=== Python Arrow+ZSTD Benchmark ==="
    python3 run_benchmark.py --host "$HOST" --port "$PORT" --user "$USER" --encoding arrow+zstd --query "$QUERY" --outdir "$OUTDIR"
    echo
fi

if [[ "$BENCHMARKS" == *"java"* ]]; then
    echo "=== Java JDBC Benchmark ==="
    cd java-jdbc
    # Use single quotes around entire args and escape internal quotes properly
    mvn -q exec:java -Dexec.args='--host '"$HOST"' --port '"$PORT"' --user '"$USER"' --query "'"$QUERY"'" --outdir ../'"$OUTDIR"
    cd ..
    echo
fi

echo "🎉 Benchmarks completed!"
echo
echo "📊 Generated files:"
ls -la "$OUTDIR"/*.csv 2>/dev/null || echo "No CSV files found"

echo
echo "💡 To compare results, run:"
echo "  python3 compare_all_results.py \"$OUTDIR\""