with source as (
    select * from {{ source('internal_raw', 'raw_orders') }}
),
renamed as (
    select
        order_id::varchar as order_id,
        user_id::varchar as user_id,
        total_amount::double as total_amount,
        status::varchar as order_status,
        payment_method::varchar as payment_method,
        shipping_address::varchar as shipping_address,
        to_timestamp(created_at / 1000) as created_at,
        to_timestamp(updated_at / 1000) as updated_at
    from source
)
select * from renamed
