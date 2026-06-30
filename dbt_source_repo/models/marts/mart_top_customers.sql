{{ config(tags=['marts', 'refresh:hourly']) }}

select customer_id, name, total_amount
from {{ ref('mart_customer_orders') }}
order by total_amount desc
limit 3