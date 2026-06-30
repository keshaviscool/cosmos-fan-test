{{ config(tags=['marts', 'refresh:hourly']) }}

select
    customer_id,
    case when order_count = 0 then 0 else total_amount / order_count end as avg_order_value
from {{ ref('mart_customer_orders') }}