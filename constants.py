import os

# Absolute path of the current script
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
# historical stock prices path
warehouse_path = script_dir + "/job1"
stock_prices_table_name = "new_historical_stock_prices"
historical_stocks_table_name = "new_historical_stocks"

stock_prices_table_input_path = script_dir + "/data_cleaning_and_db_loading/data/new_historical_stock_prices.csv"
historical_stocks_table_input_path = script_dir + "/data_cleaning_and_db_loading/data/new_historical_stocks.csv"

