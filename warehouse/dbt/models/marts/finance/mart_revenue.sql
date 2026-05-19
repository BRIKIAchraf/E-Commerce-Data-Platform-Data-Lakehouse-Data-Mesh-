with mart_orders as (
    select * from {{ ref('mart_orders') }}
)
select
    date_trunc('day', ordered_at) as revenue_date,
    count(distinct order_id) as total_orders,
    sum(case when is_revenue_confirmed then amount_paid else 0.0 end) as net_revenue,
    sum(total_amount) as gross_revenue
from mart_orders
group by 1
order by 1 desc
