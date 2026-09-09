with states as (
    select distinct state
    from {{ ref('stg_covid_deaths') }}
    where state is not null

    union

    select distinct state
    from {{ ref('stg_flu_surveillance') }}
    where state is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['state']) }} as state_key,
    state
from states