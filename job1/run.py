import time
from pyspark.sql import SparkSession


def create_hive_table():
    # Initialize a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("Hive Example") \
        .config("spark.sql.warehouse.dir", "/Users/kaguyasama/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "../data_cleaning_and_db_loading/data/new_historical_stock_prices.csv")
    df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "../data_cleaning_and_db_loading/data/new_historical_stocks.csv")

    # Create Temporary View
    df1.createOrReplaceTempView("historical_stock_prices_table")
    df2.createOrReplaceTempView("historical_stocks_table")
    start_time = time.time()
    # Execute SQL Query
    result_df1 = spark.sql("SELECT * FROM historical_stock_prices_table ")
    result_df2 = spark.sql("SELECT * FROM historical_stocks_table ")
    result_df1.show()
    result_df2.show()
    end_time = time.time()

    # Stop Spark Session
    spark.stop()


if __name__ == "__main__":
    create_hive_table()
