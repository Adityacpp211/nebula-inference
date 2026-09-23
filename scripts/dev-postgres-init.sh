#!/usr/bin/env bash
# Runs once when the development PostgreSQL volume is first created.
#
# Creates the test database alongside the development one, so `make
# test-integration` has somewhere to create its per-test databases without
# touching development data.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    SELECT 'CREATE DATABASE nebula_test'
     WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'nebula_test')\gexec

    -- The integration suite creates and drops databases of its own, so the
    -- development user needs CREATEDB. This is a development container only.
    ALTER ROLE nebula CREATEDB;
EOSQL

echo "nebula: development and test databases ready"
