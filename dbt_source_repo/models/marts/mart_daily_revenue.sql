{{ config(tags=['marts', 'refresh:hourly']) }}

select order_date, sum(amount) as revenue
from {{ ref('stg_orders') }}
group by 1