with int_orders as (
    select * from {{ ref('int_orders_payments') }}
)
select
    order_id,
    user_id,
    total_amount,
    order_status,
    amount_paid,
    payment_status,
    created_at as ordered_at,
    case 
        when order_status = 'completed' and payment_status = 'succeeded' then true
        else false
    end as is_revenue_confirmed
from int_orders
