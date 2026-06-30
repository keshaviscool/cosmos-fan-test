{{ config(tags=['marts', 'refresh:hourly']) }}

select customer_id, order_count
from {{ ref('mart_customer_orders') }}