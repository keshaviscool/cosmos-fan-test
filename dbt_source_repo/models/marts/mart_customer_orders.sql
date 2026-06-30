{{ config(tags=['marts', 'refresh:hourly']) }}

select
    c.customer_id,
    c.name,
    count(o.order_id) as order_count,
    coalesce(sum(o.amount), 0) as total_amount
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
group by 1, 2