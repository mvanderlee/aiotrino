# Trino Client Benchmarks

Simple benchmark suite to compare Trino client performance across different encodings.

## Benchmarks Included

- **Python Arrow+ZSTD**: aiotrino client with Arrow+ZSTD encoding
- **Python JSON+ZSTD**: aiotrino client with JSON+ZSTD encoding  
- **Java JDBC**: Standard Trino JDBC driver

## Usage

### Run All Benchmarks
```bash
./run_all.sh --host HOST --port PORT --user USER --query "SQL" --outdir /path/to/results
```

### Individual Benchmarks

#### Python Benchmark
```bash
python run_benchmark.py --host HOST --port PORT --user USER --encoding ENCODING --query "SQL" --outdir /path/to/results
```

#### Java Benchmark
```bash
cd java-jdbc
mvn exec:java -Dexec.args="--host HOST --port PORT --user USER --query SQL --outdir /path/to/results"
```

### Compare Results
```bash
python compare_all_results.py /path/to/results
```

## Examples

```bash
# Run all benchmarks
./run_all.sh --host localhost --port 8080 --user test --query "SELECT * FROM table LIMIT 1000000" --outdir results/my_test

# Individual Python Arrow benchmark
python run_benchmark.py --host localhost --port 8080 --user test --encoding arrow+zstd --query "SELECT * FROM table LIMIT 1000000" --outdir results/my_test

# Compare results
python compare_all_results.py results/my_test
```

## Output

- **CSV Files**: `benchmark_results_{encoding}.csv` with columns: `rows`, `time_ms`
- **Comparison Graph**: `benchmark_comparison.png` with throughput analysis

## Requirements

- Python 3.8+ with aiotrino package
- Java 11+ with Maven
- Running Trino server with TPC-DS data
- matplotlib and pandas for visualization
