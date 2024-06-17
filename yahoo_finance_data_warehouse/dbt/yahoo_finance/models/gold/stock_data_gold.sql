{{ config(
    materialized='table'
) }}

WITH daily_summary AS (
    SELECT
        Date,
        Symbol,
        AVG(Open) AS avg_open,
        AVG(High) AS avg_high,
        AVG(Low) AS avg_low,
        AVG(Close) AS avg_close,
        SUM(Volume) AS total_volume,
        SUM(Dividends) AS total_dividends,
        SUM(Stock_Splits) AS total_stock_splits,
        AVG(Daily_Change) AS avg_daily_change,
        AVG(Daily_Change_Percent) AS avg_daily_change_percent,
        AVG(Weekly_MA) AS avg_weekly_ma,
        AVG(Monthly_MA) AS avg_monthly_ma,
        AVG(Monthly_Volatility) AS avg_monthly_volatility
    FROM {{ ref('stock_data_silver') }}
    GROUP BY Date, Symbol
),

anomalies AS (
    SELECT
        Date,
        Symbol,
        avg_close,
        avg_daily_change_percent,
        CASE
            WHEN avg_daily_change_percent > 3 THEN 'High Positive Change'
            WHEN avg_daily_change_percent < -3 THEN 'High Negative Change'
            ELSE 'Normal'
        END AS anomaly_type
    FROM daily_summary
)

SELECT 
    ds.Date,
    ds.Symbol,
    ds.avg_open,
    ds.avg_high,
    ds.avg_low,
    ds.avg_close,
    ds.total_volume,
    ds.total_dividends,
    ds.total_stock_splits,
    ds.avg_daily_change,
    ds.avg_daily_change_percent,
    ds.avg_weekly_ma,
    ds.avg_monthly_ma,
    ds.avg_monthly_volatility,
    a.anomaly_type
FROM daily_summary ds
LEFT JOIN anomalies a ON ds.Date = a.Date AND ds.Symbol = a.Symbol
