#!/bin/sh
set -e

# ── Wait for PostgreSQL ──────────────────────────────────────
echo "Waiting for PostgreSQL at hive-metastore-db:5432..."
until pg_isready -h hive-metastore-db -p 5432; do
  sleep 2
done
echo "PostgreSQL is ready."

# ── Initialize metastore schema (idempotent) ────────────────
echo "Initializing Hive Metastore schema..."
/opt/hive/bin/schematool -dbType postgres -initSchema 2>&1 || \
  echo "Schema may already exist — continuing."
echo "Schema initialization complete."

# ── Start metastore service ──────────────────────────────────
echo "Starting Hive Metastore Server..."
exec /opt/hive/bin/hive --service metastore
