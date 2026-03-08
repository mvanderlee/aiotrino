-- generate-data.sql
-- Materialize TPC-H tables into Iceberg (Parquet on S3/MinIO)
-- Scale factor: sf10 ~ 86M rows total

CREATE SCHEMA IF NOT EXISTS iceberg.tpch_sf10;

-- region: 5 rows
CREATE TABLE iceberg.tpch_sf10.region AS
SELECT * FROM tpch.sf10.region;

-- nation: 25 rows
CREATE TABLE iceberg.tpch_sf10.nation AS
SELECT * FROM tpch.sf10.nation;

-- supplier: ~100K rows
CREATE TABLE iceberg.tpch_sf10.supplier AS
SELECT * FROM tpch.sf10.supplier;

-- customer: ~1.5M rows
CREATE TABLE iceberg.tpch_sf10.customer AS
SELECT * FROM tpch.sf10.customer;

-- part: ~2M rows
CREATE TABLE iceberg.tpch_sf10.part AS
SELECT * FROM tpch.sf10.part;

-- partsupp: ~8M rows
CREATE TABLE iceberg.tpch_sf10.partsupp AS
SELECT * FROM tpch.sf10.partsupp;

-- orders: ~15M rows
CREATE TABLE iceberg.tpch_sf10.orders AS
SELECT * FROM tpch.sf10.orders;

-- lineitem: ~60M rows (largest table)
CREATE TABLE iceberg.tpch_sf10.lineitem AS
SELECT * FROM tpch.sf10.lineitem;
