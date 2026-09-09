
with source as (
    select * from {{ source('raw', 'covid_deaths_raw') }}
),

cleaned as (
    select
        run_id,
        snapshot_date,
        ingested_at,
        state,
        covid_19_deaths,
        total_deaths,
        pneumonia_deaths,
        pneumonia_and_covid_19_deaths,
        influenza_deaths
    from source
)

select * from cleaned