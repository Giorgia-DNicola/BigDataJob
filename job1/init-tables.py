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
        .enableHiveSupport() \


# Add this configuration if running on local not cluster
#        .config("spark.hadoop.fs.defaultFS", "file:///") \

if "hdfs:" in str(args.stock_prices_table_input_path):
        spark = SparkSession.builder \
                .appName("Job one") \
                .config("spark.sql.warehouse.dir", args.warehouse_path) \
                .config("spark.driver.host", "localhost") \
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




