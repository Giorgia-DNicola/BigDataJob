import argparse

from pyspark.sql import SparkSession

warehouse_path = "/Users/kaguyasama/GitHub/BigDataJob/job1"
stock_prices_table_name = "new_historical_stock_prices"

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
# parse arguments
args = parser.parse_args()
input_path = "/Users/kaguyasama/GitHub/BigDataJob/data_cleaning_and_db_loading/data/new_historical_stock_prices.csv"


spark = SparkSession.builder \
        .appName("Job one") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .config("spark.driver.host", "localhost") \
        .enableHiveSupport() \
        .getOrCreate()

spark.sql("DROP TABLE IF EXISTS " + stock_prices_table_name)
spark.sql("""CREATE TABLE new_historical_stock_prices (
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
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Write the DataFrame into the Hive table
df.write.mode('append').insertInto(stock_prices_table_name)

all_items_DF = spark.sql("SELECT * FROM " + stock_prices_table_name)
all_items_DF. show()
all_items_DF.printSchema()







