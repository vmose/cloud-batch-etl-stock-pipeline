-- models/marts/fct_stock_prices_daily.sql
-- Analysis-ready daily stock price fact table.
-- Grain: 1 row per symbol per calendar date.

{{ config(materialized='table') }}

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    tick_count,
    intraday_return_pct,

    -- 7-day rolling average close, for quick trend context downstream
    avg(close_price) over (
        partition by symbol
        order by unix_date(trade_date)
        range between 6 preceding and current row
    ) as close_price_7d_avg

from {{ ref('int_daily_ohlc') }}
