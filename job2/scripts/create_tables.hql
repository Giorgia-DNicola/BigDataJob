CREATE TABLE historical_stock_prices (
    ticker STRING
    close FLOAT,
    low FLOAT,
    high FLOAT,
    volume INT,
    date STRING
)
STORED AS PARQUET;

CREATE TABLE historical_stocks (
    ticker STRING,
    name STRING,
    sector STRING
)
STORED AS PARQUET;
