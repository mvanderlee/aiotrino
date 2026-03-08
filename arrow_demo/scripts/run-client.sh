#!/bin/sh
# run-client.sh - Client entrypoint: run benchmarks
set -e

TRINO_HOST="${TRINO_HOST:-trino}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_URL="http://${TRINO_HOST}:${TRINO_PORT}"

echo "============================================="
echo " Trino Arrow+ZSTD Demo Client"
echo " Trino: ${TRINO_URL}"
echo "============================================="
echo ""

# -----------------------------------------------
# 1. Wait for Trino to be reachable
# -----------------------------------------------
echo "Waiting for Trino to be ready..."
until curl -sf "${TRINO_URL}/v1/info" | grep -q '"starting":false'; do
    sleep 2
done
echo "Trino is ready."
echo ""

# -----------------------------------------------
# 2. Verify Iceberg data exists
# -----------------------------------------------
echo "Checking Iceberg data..."
CHECK_RESULT=$(python3 -c "
import aiotrino, asyncio, os
async def check():
    conn = aiotrino.dbapi.Connection(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', '8080')),
        user='demo',
    )
    try:
        async with await conn.cursor() as cur:
            await cur.execute(\"SELECT count(*) FROM iceberg.tpch_sf10.lineitem\")
            rows = await cur.fetchall()
            print(rows[0][0])
    except Exception:
        print('0')
    finally:
        await conn.close()
asyncio.run(check())
" 2>/dev/null || echo "0")

if [ "$CHECK_RESULT" = "0" ]; then
    echo "WARNING: Iceberg data not found. The 'iceberg' benchmark will fail."
    echo "         Run 'docker compose up generate-data' first, or use '--source tpch'."
else
    echo "Iceberg data present (${CHECK_RESULT} lineitem rows)."
fi
echo ""

# -----------------------------------------------
# 3. Run benchmarks
# -----------------------------------------------
echo "============================================="
echo " Benchmark 1/2: TPC-H SF1 (tpch connector)"
echo "============================================="
echo ""
python3 /app/examples/03_benchmark_json_vs_arrow.py --source tpch

echo ""
echo ""
echo "============================================="
echo " Benchmark 2/2: TPC-H SF10 (Iceberg)"
echo "============================================="
echo ""
python3 /app/examples/03_benchmark_json_vs_arrow.py --source iceberg

echo ""
echo "============================================="
echo " All benchmarks complete!"
echo "============================================="
