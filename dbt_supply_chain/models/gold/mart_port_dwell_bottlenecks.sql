with movement_events as (

    select *
    from {{ ref('fct_movement_events') }}

),

-- 1. Use window functions to grab the timestamps of the specific milestones per container
milestone_timestamps as (

    select
        container_id,
        carrier_name,
        origin_country,
        estimated_value,
        event_type,
        event_time,
        -- Get the timestamp of the origin departure
        max(case when event_type = 'DEPART_ORIGIN' then event_time end) over (partition by container_id) as depart_origin_at,
        -- Get the timestamp of the port arrival
        max(case when event_type = 'ARRIVE_PORT' then event_time end) over (partition by container_id) as arrive_port_at,
        -- Get the timestamp of the port departure
        max(case when event_type = 'DEPART_PORT' then event_time end) over (partition by container_id) as depart_port_at,
        -- Grab the estimated value specifically at the time of arrival
        max(case when event_type = 'ARRIVE_PORT' then estimated_value end) over (partition by container_id) as estimated_value_at_arrival

    from movement_events

),

-- 2. Count how many intermediate rows exist between arrival and departure milestones
intermediate_counts as (

    select
        container_id,
        count(*) as total_event_count
    from movement_events
    where event_type not in ('DEPART_ORIGIN', 'ARRIVE_PORT', 'DEPART_PORT')
    group by container_id

),

-- 3. Consolidate into a single record per container and calculate metrics
final_metrics as (

    select distinct
        m.container_id,
        m.carrier_name,
        m.origin_country,
        m.depart_origin_at,
        m.arrive_port_at,
        m.depart_port_at,
        datediff('day', m.arrive_port_at, m.depart_port_at) as port_dwell_days,
        coalesce(c.total_event_count, 0) as intermediate_event_count,
        m.estimated_value_at_arrival
    from milestone_timestamps m
    left join intermediate_counts c 
        on m.container_id = c.container_id
    where m.depart_origin_at is not null 
      and m.arrive_port_at is not null 
      and m.depart_port_at is not null

)

select * from final_metrics
where port_dwell_days > 5