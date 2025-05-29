#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --port "$POSTGRESQL_PORT_NUMBER" <<-EOSQL
    CREATE USER postgres_exporter WITH PASSWORD 'postgres_exporter' CONNECTION LIMIT 10;
    GRANT pg_monitor to postgres_exporter;
    CREATE DATABASE db2;
    CREATE DATABASE db3;
EOSQL