-- Fails when any container still has non-increasing event_time after silver cleansing.
select
    container_id,
    event_time,
    lag(event_time) over (
        partition by container_id
        order by event_time, event_type
    ) as prior_event_time

from {{ ref('fct_movement_events') }}

qualify prior_event_time is not null
   and event_time <= prior_event_time
