-- models/staging/stg_stock_ticks.sql
-- 1:1 with the raw source, typed/renamed, deduplicated.
-- Grain: 1 row per symbol per timestamp (deduped in case a DAG re-run
-- appended the same day's data twice).

{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'stock_ticks') }}

),

renamed as (

    select
        upper(trim(symbol))                as symbol,
        cast(timestamp as timestamp)        as tick_at,
        cast(open as numeric)               as open_price,
        cast(high as numeric)               as high_price,
        cast(low as numeric)                as low_price,
        cast(close as numeric)              as close_price,
        cast(volume as int64)               as volume,
        _ingested_at,
        _source_file

    from source
    where symbol is not null
      and timestamp is not null

),

-- keep the most recently ingested row for a given (symbol, tick_at) pair,
-- in case of a re-run appending duplicate raw data
deduped as (

    select
        *,
        row_number() over (
            partition by symbol, tick_at
            order by _ingested_at desc
        ) as row_num

    from renamed

)

select
    symbol,
    tick_at,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    _ingested_at,
    _source_file
from deduped
where row_num = 1
