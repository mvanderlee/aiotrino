#!/bin/sh
# generate-data.sh - Materialize TPC-H data into Iceberg tables via Trino CLI
set -e

TRINO_SERVER="${TRINO_SERVER:-http://trino:8080}"
SQL_FILE="/tmp/generate-data.sql"

echo "============================================="
echo " Generating TPC-H SF10 data in Iceberg"
echo " Trino server: $TRINO_SERVER"
echo "============================================="
echo ""

# Check if data already exists (idempotent)
EXISTING=$(trino --server "$TRINO_SERVER" --execute \
    "SELECT count(*) FROM iceberg.tpch_sf10.lineitem" 2>/dev/null || echo "0")
EXISTING=$(echo "$EXISTING" | tr -d '"' | tr -d ' ')

if [ "$EXISTING" != "0" ]; then
    echo "Iceberg data already exists (${EXISTING} lineitem rows). Skipping generation."
    exit 0
fi

echo "This will create ~86M rows across 8 tables."
echo "Expected time: 2-10 min depending on hardware."
echo ""

trino --server "$TRINO_SERVER" --catalog iceberg --file "$SQL_FILE"

echo ""
echo "============================================="
echo " Data generation complete!"
echo "============================================="

# Verify lineitem row count
ROWS=$(trino --server "$TRINO_SERVER" --execute \
    "SELECT count(*) FROM iceberg.tpch_sf10.lineitem" 2>/dev/null || echo "?")
ROWS=$(echo "$ROWS" | tr -d '"' | tr -d ' ')
echo "Lineitem rows: $ROWS"
