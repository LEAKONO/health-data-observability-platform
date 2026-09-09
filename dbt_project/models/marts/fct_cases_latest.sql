with ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date, state
            order by ingested_at desc
        ) as recency_rank
    from {{ ref('fct_cases_as_reported') }}
)

select
    state_key,
    run_id,
    snapshot_date,
    ingested_at,
    state,
    covid_19_deaths,
    total_deaths,
    pneumonia_deaths,
    pneumonia_and_covid_19_deaths,
    influenza_deaths
from ranked
where recency_rank = 1