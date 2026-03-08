# Simple Trino JDBC Benchmark

A basic Java program to test Trino JDBC performance.

## Run

```bash
# Compile and run
mvn exec:java

# Or compile and run manually
mvn compile
java -cp target/classes:~/.m2/repository/io/trino/trino-jdbc/476/trino-jdbc-476.jar SimpleBenchmark
```

## What it does

- Connects to Trino at `localhost:8092`
- Runs queries with increasing row counts (2^10 to 2^24)
- Prints timing and throughput results

## Customize

Edit `SimpleBenchmark.java` to change:
- Connection URL
- Query 
- Row counts to test 