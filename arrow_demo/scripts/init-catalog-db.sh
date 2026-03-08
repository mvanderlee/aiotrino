#!/bin/sh
# init-catalog-db.sh - Create the SQLite database with Iceberg JDBC catalog tables (V1 schema)
set -e

DB_DIR="/var/trino"
DB_PATH="$DB_DIR/iceberg-catalog.db"

echo "Initializing Iceberg JDBC catalog database at $DB_PATH ..."

sqlite3 "$DB_PATH" <<'EOF'
CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(5500),
    property_value VARCHAR(5500),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(5500),
    previous_metadata_location VARCHAR(5500),
    iceberg_type VARCHAR(5),
    record_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);
EOF

# Make the DB file and directory writable by any user (Trino runs as non-root uid 1000)
chmod 777 "$DB_DIR"
chmod 666 "$DB_PATH"
echo "Catalog DB initialized successfully (V1 schema)."
