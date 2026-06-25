with staged as (

    select *
    from {{ ref('stg_movement_events') }}

),

validated as (

    select
        staged.*,
        lag(staged.event_time) over (
            partition by staged.container_id
            order by staged.event_time, staged.event_type
        ) as prior_event_time

    from staged
    where staged.container_id is not null
      and staged.event_time is not null
      and staged.event_type is not null
      and staged.carrier_name is not null
      and staged.origin_country is not null
      and staged.estimated_value is not null
      and staged.event_type in (
          'DEPART_ORIGIN',
          'ARRIVE_PORT',
          'PORT_DWELL',
          'DEPART_PORT',
          'ARRIVE_DC'
      )

),

cleaned as (

    select
        {{ dbt_utils.generate_surrogate_key(['container_id', 'event_time', 'event_type']) }}
            as movement_event_key,
        container_id,
        carrier_name,
        event_time,
        event_type,
        estimated_value,
        origin_country,
        row_number() over (
            partition by container_id
            order by event_time, event_type
        ) as event_sequence_number,
        _dbt_loaded_at

    from validated
    where prior_event_time is null
       or event_time > prior_event_time

)

select * from cleaned
