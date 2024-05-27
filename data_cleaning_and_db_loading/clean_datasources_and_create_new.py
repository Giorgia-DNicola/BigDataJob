import pandas as pd

# paths to files
historical_stocks_path = "./data/historical_stocks.csv"
new_historical_stocks_path = "./data/new_historical_stocks.csv"

historical_stock_prices_path = "./data/historical_stock_prices.csv"
new_historical_stock_prices_path = "./data/new_historical_stock_prices.csv"

# more visual information
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 50)

# read historical_stocks.csv
df = pd.read_csv(historical_stocks_path)

# Remove columns
df.drop(['exchange', 'industry'], axis=1, inplace=True)

# Replace 'N/A' with None in historical_stocks.csv, better approach for PostgreSQL management
df.replace('N/A', None, inplace=True)

df.to_csv(new_historical_stocks_path, index=False)

updated_df = pd.read_csv(new_historical_stocks_path)
print(updated_df.head())

# read historical_stock_prices.csv
df = pd.read_csv(historical_stock_prices_path)

# Remove columns
df.drop(['open', 'adj_close'], axis=1, inplace=True)

# Replace 'N/A' with None in historical_stocks.csv, better approach for PostgreSQL management
df.replace('N/A', None, inplace=True)

df.to_csv(new_historical_stock_prices_path, index=False)

updated_df = pd.read_csv(new_historical_stock_prices_path)
print(updated_df.head())
