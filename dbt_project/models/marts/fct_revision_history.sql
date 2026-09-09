with versions as (
    select
        state,
        snapshot_date,
        run_id,
        ingested_at,
        covid_19_deaths,
        row_number() over (
            partition by snapshot_date, state
            order by ingested_at asc
        ) as version_number
    from {{ ref('fct_cases_as_reported') }}
),

with_previous as (
    select
        state,
        snapshot_date,
        run_id,
        ingested_at,
        version_number,
        covid_19_deaths as current_value,
        lag(covid_19_deaths) over (
            partition by snapshot_date, state
            order by version_number
        ) as previous_value
    from versions
)

select
    state,
    snapshot_date,
    run_id,
    ingested_at,
    version_number,
    previous_value,
    current_value,
    current_value - previous_value as revision_delta
from with_previous
where previous_value is not null
  and current_value != previous_value