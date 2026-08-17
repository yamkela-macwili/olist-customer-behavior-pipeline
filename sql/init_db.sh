#!/bin/bash
# ── Olist database + schema initialisation ────────────────────────────────────
# Runs once on first container start (postgres docker-entrypoint-initdb.d).
# Creates the olist database and its three working schemas so the pipeline
# doesn't need CREATE DATABASE permissions at runtime.

set -e

echo ">>> Creating olist database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE olist;
EOSQL

echo ">>> Creating schemas in olist database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "olist" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS marts;
    COMMENT ON SCHEMA raw   IS 'Faithful copy of source CSVs — no transforms applied';
    COMMENT ON SCHEMA marts IS 'Cleaned fact/dim tables and per-customer feature tables';
EOSQL

echo ">>> Database initialisation complete."
