
with source as (
    select * from {{ source('raw', 'flu_surveillance_raw') }}
),

cleaned as (
    select
        run_id,
        snapshot_date,
        ingested_at,
        state,
        activity_level
    from source
)

select * from cleaned