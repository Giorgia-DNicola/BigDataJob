-- Load historical stock prices data into Hive
LOAD DATA LOCAL INPATH './data/new_historical_stock_prices.csv' 
OVERWRITE INTO TABLE historical_stock_prices;

-- Load historical stocks data into Hive
LOAD DATA LOCAL INPATH './data/new_historical_stocks.csv' 
OVERWRITE INTO TABLE historical_stocks;
