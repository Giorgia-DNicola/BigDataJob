#!/bin/bash

# Wait for the PostgreSQL container to be ready
until pg_isready -h postgres -p 5432; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Start Hive services
schematool -initSchema -dbType postgres

# Run Hive queries to create tables and load data
hive -e "
CREATE TABLE IF NOT EXISTS historical_stocks (
    ticker STRING,
    name STRING,
    sector STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/hive/data/new_historical_stocks.csv' INTO TABLE historical_stocks;

CREATE TABLE IF NOT EXISTS historical_stock_prices (
    ticker STRING,
    close DOUBLE,
    low DOUBLE,
    high DOUBLE,
    volume DOUBLE,
    date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/hive/data/new_historical_stock_prices.csv' INTO TABLE historical_stock_prices;
"
