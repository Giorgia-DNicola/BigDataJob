import argparse

from pyspark.sql import SparkSession

warehouse_path = "/Users/kaguyasama/GitHub/BigDataJob/job1"
stock_prices_table_name = "new_historical_stock_prices"
historical_stocks_table_name = "new_historical_stocks"
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
# parse arguments
args = parser.parse_args()
stock_prices_table_input_path = "/Users/kaguyasama/GitHub/BigDataJob/data_cleaning_and_db_loading/data/new_historical_stock_prices.csv"
historical_stocks_table_input_path = "/Users/kaguyasama/GitHub/BigDataJob/data_cleaning_and_db_loading/data/new_historical_stocks.csv"

spark = SparkSession.builder \
        .appName("Job one") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .config("spark.driver.host", "localhost") \
        .enableHiveSupport() \
        .getOrCreate()

#spark.sql("DROP TABLE IF EXISTS " + historical_stocks_table_name)
spark.sql("""CREATE TABLE IF NOT EXISTS new_historical_stocks (
            ticker VARCHAR(255),
            name VARCHAR(255),
            sector VARCHAR(255)
        )
        USING csv
        OPTIONS (header 'true', sep ',', inferSchema 'true', quote '\"')
        LOCATION '/Users/kaguyasama/GitHub/BigDataJob/job1/spark-warehouse'""")

#spark.sql("DROP TABLE IF EXISTS " + stock_prices_table_name)
spark.sql("""CREATE TABLE IF NOT EXISTS new_historical_stock_prices (
            ticker VARCHAR(255),
            close DOUBLE,
            low DOUBLE,
            high DOUBLE,
            volume DOUBLE,
            date TIMESTAMP
        ) USING csv
        OPTIONS (header 'true', sep ',', inferSchema 'true', quote '\"')
        LOCATION '/Users/kaguyasama/GitHub/BigDataJob/job1/spark-warehouse'""")

"""if input_path.startswith("hdfs://"):
    spark.sql("LOAD DATA INPATH '" + input_path + "' INTO TABLE " + stock_prices_table_name)
else:
    spark.sql("LOAD DATA LOCAL INPATH '" + input_path + "' INTO TABLE " + stock_prices_table_name)"""

# Load the data into DataFrame
df1 = spark.read.csv(stock_prices_table_input_path, header=True, inferSchema=True)
df2 = spark.read.csv(historical_stocks_table_input_path, header=True, inferSchema=True)


# Write the DataFrame into the Hive table
df1.write.mode('append').insertInto(stock_prices_table_name)
df2.write.mode('append').insertInto(historical_stocks_table_name)

all_items_DF1 = spark.sql("SELECT * FROM " + stock_prices_table_name)
all_items_DF1. show()
all_items_DF1.printSchema()

all_items_DF2 = spark.sql("SELECT * FROM " + historical_stocks_table_name)
all_items_DF2. show()
all_items_DF2.printSchema()







