select
    {{ dbt_utils.generate_surrogate_key(['state']) }} as state_key,
    run_id,
    snapshot_date,
    ingested_at,
    state,
    covid_19_deaths,
    total_deaths,
    pneumonia_deaths,
    pneumonia_and_covid_19_deaths,
    influenza_deaths
from {{ ref('stg_covid_deaths') }}