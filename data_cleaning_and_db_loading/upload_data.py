import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text

# paths to files
historical_stocks_path = "./data/historical_stocks.csv"
new_historical_stocks_path = "./data/new_historical_stocks.csv"

historical_stock_prices_path = "./data/historical_stock_prices.csv"
new_historical_stock_prices_path = "./data/new_historical_stock_prices.csv"

# read historical_stocks.csv
df1 = pd.read_csv(new_historical_stocks_path)

# read historical_stock_prices.csv
df2 = pd.read_csv(new_historical_stock_prices_path)

# Replace with your actual database URI
DATABASE_URI = 'postgresql://postgres:postgres@localhost/postgres'

try:
    # Create an engine instance
    engine = create_engine(DATABASE_URI)

    # Connect to the database
    connection = engine.connect()

    # If the connection is successful, print a success message
    print("Successfully connected to the database.")

    df1.to_sql('historical_stocks', con=engine, index=False, if_exists='append')
    df2.to_sql('historical_stock_prices', con=engine, index=False, if_exists='append')

    connection.commit()
    print("Data uploaded successfully.")
except Exception as e:
    print("An error occurred:", e)
