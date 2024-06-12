#!/usr/bin/env python3

import argparse

from pyspark.sql import SparkSession

# Setting up the argument parser
parser = argparse.ArgumentParser(description='Load and process stock data')
parser.add_argument('--warehouse_path', type=str, required=True, help='Path to the Spark SQL warehouse directory')
parser.add_argument('--stock_prices_table_input_path', type=str, required=True, help='Input path for stock prices CSV')
parser.add_argument('--historical_stocks_table_input_path', type=str, required=True, help='Input path for historical stocks CSV')

# Parse arguments
args = parser.parse_args()

stock_prices_table_name = "new_historical_stock_prices"
historical_stocks_table_name = "new_historical_stocks"
annual_stock_statistics_table_name = "annual_stock_statistics"

spark = SparkSession.builder \
        .appName("Job one") \
        .config("spark.sql.warehouse.dir", args.warehouse_path) \
        .config("spark.driver.host", "localhost") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .enableHiveSupport() \


# Add this configuration if running on local not cluster
#        .config("spark.hadoop.fs.defaultFS", "file:///") \

if "hdfs:" in str(args.stock_prices_table_input_path):
        spark = SparkSession.builder \
                .appName("Job one") \
                .config("spark.sql.warehouse.dir", args.warehouse_path) \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .enableHiveSupport() \

spark = spark.getOrCreate()

# Add this configuration if running on local not cluster
#        .config("spark.hadoop.fs.defaultFS", "file:///") \


#spark.sql("DROP TABLE IF EXISTS " + historical_stocks_table_name)
spark.sql(f"""CREATE TABLE IF NOT EXISTS new_historical_stocks (
            ticker VARCHAR(255),
            name VARCHAR(255),
            sector VARCHAR(255)
        )
        USING csv
        OPTIONS (header 'true', sep ',', inferSchema 'true', quote '\"')
        LOCATION '{args.warehouse_path}'""")

#spark.sql("DROP TABLE IF EXISTS " + stock_prices_table_name)
spark.sql(f"""CREATE TABLE IF NOT EXISTS new_historical_stock_prices (
            ticker VARCHAR(255),
            close DOUBLE,
            low DOUBLE,
            high DOUBLE,
            volume DOUBLE,
            date TIMESTAMP
        ) USING csv
        OPTIONS (header 'true', sep ',', inferSchema 'true', quote '\"')
        LOCATION '{args.warehouse_path}'""")

# Load the data into DataFrame
df1 = spark.read.csv(args.stock_prices_table_input_path, header=True, inferSchema=True)
df2 = spark.read.csv(args.historical_stocks_table_input_path, header=True, inferSchema=True)


# Write the DataFrame into the Hive table
df1.write.mode('append').insertInto(stock_prices_table_name)
df2.write.mode('append').insertInto(historical_stocks_table_name)

all_items_DF1 = spark.sql("SELECT * FROM " + stock_prices_table_name)
all_items_DF1. show()
all_items_DF1.printSchema()

all_items_DF2 = spark.sql("SELECT * FROM " + historical_stocks_table_name)
all_items_DF2. show()
all_items_DF2.printSchema()

# Passo 1: Creazione di una vista con le date di inizio e fine per ogni ticker e anno
first_last_dates_query = """
CREATE OR REPLACE TEMP VIEW first_last_dates AS
SELECT 
    ticker,
    EXTRACT(YEAR FROM date) AS year,
    MIN(date) AS first_date,
    MAX(date) AS last_date
FROM 
    new_historical_stock_prices
GROUP BY 
    ticker, EXTRACT(YEAR FROM date)
"""

first_last_prices_query = """
CREATE OR REPLACE TEMP VIEW first_last_prices AS
SELECT 
    fl.ticker,
    fl.year,
    fp.close AS first_close,
    lp.close AS last_close
FROM 
    first_last_dates fl
JOIN 
    new_historical_stock_prices fp ON fl.ticker = fp.ticker AND fl.first_date = fp.date
JOIN 
    new_historical_stock_prices lp ON fl.ticker = lp.ticker AND fl.last_date = lp.date
"""

# Passo 3: Calcolo delle statistiche annuali per ogni ticker
statistics_query = """
CREATE OR REPLACE TEMP VIEW annual_statistics AS
SELECT 
    flp.ticker,
    flp.year,
    ROUND(((MAX(flp.last_close) - MIN(flp.first_close)) / MIN(flp.first_close)) * 100, 2) AS percentage_change,
    MIN(sp.low) AS min_price,
    MAX(sp.high) AS max_price,
    AVG(sp.volume) AS avg_volume
FROM 
    first_last_prices flp
JOIN 
    new_historical_stock_prices sp ON flp.ticker = sp.ticker AND EXTRACT(YEAR FROM sp.date) = flp.year
GROUP BY 
    flp.ticker, flp.year
"""

# Passo 4: Unione dei dati con le informazioni sulle aziende
final_query = """
INSERT INTO annual_stock_statistics
SELECT 
    DISTINCT s.ticker,
    s.name,
    a.year,
    a.percentage_change,
    a.min_price,
    a.max_price,
    a.avg_volume
FROM 
    new_historical_stocks s
JOIN 
    annual_statistics a ON s.ticker = a.ticker
"""

# Esecuzione delle query
spark.sql(first_last_dates_query)
spark.sql(first_last_prices_query)
spark.sql(statistics_query)
spark.sql(final_query)

all_items_DF3 = spark.sql("SELECT * FROM " + annual_stock_statistics_table_name)
all_items_DF3. show()
all_items_DF3.printSchema()




