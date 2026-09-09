
with latest_per_state as (
    select
        state,
        max(snapshot_date) as latest_snapshot_date,
        max(ingested_at) as latest_ingested_at
    from {{ ref('fct_cases_latest') }}
    group by state
)

select
    state,
    latest_snapshot_date,
    latest_ingested_at,
    datediff('day', latest_snapshot_date, current_date()) as days_since_latest_snapshot,
    case
        when datediff('day', latest_snapshot_date, current_date()) > 14 then true
        else false
    end as is_stale
from latest_per_state
order by days_since_latest_snapshot desc