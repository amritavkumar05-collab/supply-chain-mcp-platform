with source as (

    select *
    from {{ source('raw', 'movement_events') }}

),

renamed as (

    select
        container_id::varchar(64) as container_id,
        carrier_name::varchar(255) as carrier_name,
        event_time::timestamp_ntz as event_time,
        upper(trim(event_type::varchar(64))) as event_type,
        estimated_value::number(18, 2) as estimated_value,
        origin_country::varchar(128) as origin_country,
        current_timestamp() as _dbt_loaded_at

    from source

)

select * from renamed
