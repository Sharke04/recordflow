#!/usr/bin/env bash
# Local development only: creates the Recordflow role with the privileges from
# the production setup, plus a test table, a publication and a replication slot.
# Runs once, when the PostgreSQL data volume is initialized.

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v rf_password="$RECORDFLOW_PG_PASSWORD" <<'SQL'
CREATE ROLE recordflow WITH LOGIN REPLICATION PASSWORD :'rf_password';
SELECT format('GRANT CONNECT ON DATABASE %I TO recordflow', current_database()) \gexec

CREATE TABLE public.orders (
    id         bigint        PRIMARY KEY,
    customer   text          NOT NULL,
    amount     numeric(12,2),
    note       text,
    created_at timestamptz   NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA public TO recordflow;
GRANT SELECT ON public.orders TO recordflow;

CREATE PUBLICATION recordflow_pub FOR TABLE public.orders;
SELECT pg_create_logical_replication_slot('recordflow_slot', 'pgoutput');
SQL
