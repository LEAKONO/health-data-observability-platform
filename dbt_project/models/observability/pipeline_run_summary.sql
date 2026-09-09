with logs as (
    select * from {{ source('observability', 'pipeline_logs') }}
)

select
    pipeline_name,
    stage,
    count(*) as total_runs,
    sum(case when status = 'success' then 1 else 0 end) as successful_runs,
    sum(case when status = 'failure' then 1 else 0 end) as failed_runs,
    round(
        100.0 * sum(case when status = 'success' then 1 else 0 end) / count(*),
        1
    ) as success_rate_pct,
    round(avg(duration_ms), 0) as avg_duration_ms,
    max(duration_ms) as max_duration_ms,
    sum(row_count_in) as total_rows_processed,
    max(logged_at) as last_run_at
from logs
group by pipeline_name, stage
order by pipeline_name, stage