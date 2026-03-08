FROM jonasbrami/trino-arrow:479-03d1b24

# Download SQLite JDBC driver for the Iceberg catalog
ARG SQLITE_JDBC_VERSION=3.45.3.0
RUN curl -fSL -o /usr/lib/trino/plugin/iceberg/sqlite-jdbc.jar \
    "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${SQLITE_JDBC_VERSION}/sqlite-jdbc-${SQLITE_JDBC_VERSION}.jar"

# Bake Trino configuration into the image
COPY etc/config.properties /etc/trino/config.properties
COPY etc/jvm.config /etc/trino/jvm.config
COPY etc/spooling-manager.properties /etc/trino/spooling-manager.properties
COPY etc/catalog/ /etc/trino/catalog/

# Copy data generation scripts (used by the generate-data service)
COPY scripts/generate-data.sh /tmp/generate-data.sh
COPY scripts/generate-data.sql /tmp/generate-data.sql
