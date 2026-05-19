with events as (
    select * from {{ ref('stg_web_events') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
user_conversions as (
    select
        e.user_id,
        e.device_type,
        e.category as primary_category_viewed,
        count(distinct o.order_id) as total_purchases
    from events e
    left join orders o on e.user_id = o.user_id
    group by 1, 2, 3
)
select * from user_conversions
