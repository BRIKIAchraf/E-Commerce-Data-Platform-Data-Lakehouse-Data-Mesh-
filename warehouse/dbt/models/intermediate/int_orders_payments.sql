with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
joined as (
    select
        o.order_id,
        o.user_id,
        o.total_amount,
        o.order_status,
        coalesce(p.amount_paid, 0.0) as amount_paid,
        coalesce(p.payment_status, 'unpaid') as payment_status,
        o.created_at
    from orders o
    left join payments p on o.order_id = p.order_id
)
select * from joined
