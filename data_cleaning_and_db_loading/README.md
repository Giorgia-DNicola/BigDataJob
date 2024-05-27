# Cleaning operations

This document describes cleaning operations on _historical_stocks.csv_ and _historical_stock_prices.csv_ datasources.
Further information regarding the dasources structures are available [here](./data/README.md).

## Chosen exercises

For the current project exercises 1 and 2 have been selected.

## Required columns

historical_stock_prices.csv:
* **ticker**
* **close**
* **volume**
* **low**
* **high**
* **date**

historical_stocks.csv:
* **ticker**
* **name**
* **sector**

## Cleanable columns

historical_stock_prices.csv:
* **open**
* **adj_close**

historical_stock.csv:
* **exchange**
* **industry**

## Cleanable cells

Some cells contain the string _N/A_, it means that no data is available and those cells can be removed.
In particular, only _historical_stocks.csv_ contains _N/A_ cells.
