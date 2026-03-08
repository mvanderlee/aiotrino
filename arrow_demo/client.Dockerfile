FROM python:3.11-slim-bookworm

# Install Java for JDBC benchmark, plus git for pip install from GitHub
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (aiotrino from GitHub)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Download Trino JDBC driver
ARG TRINO_JDBC_VERSION=479
RUN mkdir -p /app/lib && \
    curl -fSL -o /app/lib/trino-jdbc.jar \
    "https://repo1.maven.org/maven2/io/trino/trino-jdbc/${TRINO_JDBC_VERSION}/trino-jdbc-${TRINO_JDBC_VERSION}.jar"

# Copy and compile Java benchmark
COPY examples/JdbcBenchmark.java /app/java/
RUN javac -cp /app/lib/trino-jdbc.jar /app/java/JdbcBenchmark.java

# Copy Python examples and entrypoint script
COPY examples/ /app/examples/
COPY scripts/run-client.sh /app/run-client.sh

# Default env for running inside docker network
ENV TRINO_HOST=trino
ENV TRINO_PORT=8080

# Run benchmarks automatically; override with 'sleep infinity' for interactive use
CMD ["sh", "/app/run-client.sh"]
