{{ config(tags=['staging']) }}

select order_id, customer_id, amount, order_date
from {{ ref('raw_orders') }}