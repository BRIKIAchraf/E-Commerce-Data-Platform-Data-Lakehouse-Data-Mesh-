with source as (
    select * from {{ source('internal_raw', 'raw_web_events') }}
),
renamed as (
    select
        event_id::varchar as event_id,
        session_id::varchar as session_id,
        user_id::varchar as user_id,
        event_type::varchar as event_type,
        product_id::varchar as product_id,
        category::varchar as category,
        to_timestamp(timestamp / 1000) as event_timestamp,
        ip_address::varchar as ip_address,
        device_type::varchar as device_type
    from source
)
select * from renamed
