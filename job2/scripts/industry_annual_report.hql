-- Add necessary jar files

--importa pyhive

-- Register Python UDFs
CREATE TEMPORARY FUNCTION calculate_percent_change AS 'udfs.calculate_percent_change' USING '../scripts/helper_scripts/udfs.py' FROM FILE;
CREATE TEMPORARY FUNCTION extract_year AS 'udfs.extract_year' USING '../scripts/helper_scripts/udfs.py' FROM FILE;
CREATE TEMPORARY FUNCTION calculate_volume AS 'udfs.calculate_volume' USING '../scripts/helper_scripts/udfs.py' FROM FILE;

-- Create table to store the annual statistics of each stock
CREATE TABLE annual_stock_stats STORED AS PARQUET AS
SELECT
    hsp.ticker,
    hs.industry,
    extract_year(hsp.date) AS year,
    calculate_percent_change(
        FIRST_VALUE(hsp.close) OVER (PARTITION BY hsp.ticker ORDER BY hsp.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
        LAST_VALUE(hsp.close) OVER (PARTITION BY hsp.ticker ORDER BY hsp.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    ) AS percent_change,
    MIN(hsp.low) OVER (PARTITION BY hsp.ticker, extract_year(hsp.date)) AS min_price,
    MAX(hsp.high) OVER (PARTITION BY hsp.ticker, extract_year(hsp.date)) AS max_price,
    calculate_volume(
        COLLECT_LIST(hsp.volume) OVER (PARTITION BY hsp.ticker, extract_year(hsp.date))
    ) AS avg_volume
FROM
    historical_stock_prices hsp
JOIN
    historical_stocks hs
ON
    hsp.ticker = hs.ticker;

-- Create the final report table
CREATE TABLE industry_annual_report STORED AS PARQUET AS
SELECT
    industry,
    year,
    SUM(percent_change) AS industry_percent_change,
    MAX(percent_change) AS max_increment,
    MAX_VOLUME.volume AS max_volume,
    MAX_VOLUME.ticker AS max_volume_ticker
FROM (
    SELECT
        industry,
        year,
        percent_change,
        ticker,
        RANK() OVER (PARTITION BY industry, year ORDER BY percent_change DESC) AS rank_change,
        RANK() OVER (PARTITION BY industry, year ORDER BY avg_volume DESC) AS rank_volume
    FROM
        annual_stock_stats
) AS ranked_data
JOIN (
    SELECT
        industry,
        year,
        ticker,
        avg_volume,
        RANK() OVER (PARTITION BY industry, year ORDER BY avg_volume DESC) AS volume_rank
    FROM
        annual_stock_stats
) AS MAX_VOLUME
ON
    ranked_data.industry = MAX_VOLUME.industry
    AND ranked_data.year = MAX_VOLUME.year
    AND MAX_VOLUME.volume_rank = 1
WHERE
    rank_change = 1
GROUP BY
    industry, year
ORDER BY
    industry_percent_change DESC;
