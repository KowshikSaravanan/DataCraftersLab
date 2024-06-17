{{ config(
    materialized='incremental',
    unique_key=['Date', 'Symbol']
) }}

WITH source_data AS (
    SELECT
        Date,
        Symbol,
        Open,
        High,
        Low,
        Close,
        Volume,
        Dividends,
        Stock_Splits
    FROM {{ source('public', 'stock_data') }}
)

SELECT * FROM source_data
