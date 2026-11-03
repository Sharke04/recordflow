#!/usr/bin/env bash
# Local development only: creates the Recordflow database and the users from the
# production setup. Runs once, when the ClickHouse data volume is initialized.

clickhouse client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS recordflow;

CREATE USER IF NOT EXISTS recordflow IDENTIFIED WITH sha256_password BY '${RECORDFLOW_CH_PASSWORD}';
GRANT CREATE TABLE, CREATE VIEW, INSERT, SELECT, TRUNCATE ON recordflow.* TO recordflow;

CREATE USER IF NOT EXISTS analyst IDENTIFIED WITH sha256_password BY '${ANALYST_CH_PASSWORD}';
GRANT SELECT ON recordflow.* TO analyst;
SQL
