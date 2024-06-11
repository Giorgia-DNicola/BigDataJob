#!/bin/bash

# Run Hive scripts to set up tables, load data, and run queries
hive -f scripts/create_tables.hql
hive -f scripts/load_data.hql
hive -f scripts/industry_annual_report.hql
