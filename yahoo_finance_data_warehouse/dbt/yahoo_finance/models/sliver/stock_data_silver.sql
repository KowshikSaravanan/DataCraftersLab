{{ config(
    materialized='incremental',
    unique_key=['Date', 'Symbol']
) }}

WITH cleaned_data AS (
    SELECT
        Date,
        Symbol,
        Open,
        High,
        Low,
        Close,
        Volume,
        Dividends,
        Stock_Splits,
        LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) AS Prev_Close,
        Close - LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) AS Daily_Change,
        (Close - LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date)) / LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) * 100 AS Daily_Change_Percent
    FROM {{ ref('stock_data_bronze') }}
    WHERE Date IS NOT NULL
      AND Symbol IS NOT NULL
)

, volatility_data AS (
    SELECT
        Date,
        Symbol,
        Open,
        High,
        Low,
        Close,
        Volume,
        Dividends,
        Stock_Splits,
        COALESCE(Prev_Close, Close) AS Prev_Close,
        COALESCE(Daily_Change, 0) AS Daily_Change,
        COALESCE(Daily_Change_Percent, 0) AS Daily_Change_Percent,
        AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Weekly_MA,
        AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Monthly_MA
    FROM cleaned_data
)

SELECT 
    Date,
    Symbol,
    Open,
    High,
    Low,
    Close,
    Volume,
    Dividends,
    Stock_Splits,
    Prev_Close,
    Daily_Change,
    Daily_Change_Percent,
    Weekly_MA,
    Monthly_MA,
    STDDEV(Daily_Change_Percent) OVER (PARTITION BY Symbol, DATE_TRUNC('month', Date)) AS Monthly_Volatility
FROM volatility_data
