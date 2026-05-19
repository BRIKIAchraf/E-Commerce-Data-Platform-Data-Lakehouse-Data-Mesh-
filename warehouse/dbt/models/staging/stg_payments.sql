with source as (
    select * from {{ source('internal_raw', 'raw_stripe_payments') }}
),
renamed as (
    select
        payment_id::varchar as payment_id,
        order_id::varchar as order_id,
        amount::double as amount_paid,
        currency::varchar as currency,
        status::varchar as payment_status,
        to_timestamp(created_at / 1000) as payment_date
    from source
)
select * from renamed
