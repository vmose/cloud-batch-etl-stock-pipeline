-- models/intermediate/int_daily_ohlc.sql
-- Aggregates intraday ticks into daily OHLC (open/high/low/close) bars.
-- Grain: 1 row per symbol per calendar date.

{{ config(materialized='view') }}

with ticks as (

    select * from {{ ref('stg_stock_ticks') }}

),

daily as (

    select
        symbol,
        date(tick_at)                                          as trade_date,

        -- open = the earliest tick's open price that day
        array_agg(open_price order by tick_at asc limit 1)[offset(0)]   as open_price,
        max(high_price)                                          as high_price,
        min(low_price)                                           as low_price,
        -- close = the latest tick's close price that day
        array_agg(close_price order by tick_at desc limit 1)[offset(0)] as close_price,

        sum(volume)                                               as total_volume,
        count(*)                                                  as tick_count

    from ticks
    group by symbol, trade_date

)

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    tick_count,
    safe_divide(close_price - open_price, nullif(open_price, 0)) as intraday_return_pct

from daily
