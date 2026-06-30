{{ config(tags=['marts', 'refresh:hourly']) }}

select
    customer_id,
    case
        when total_amount >= 150 then 'high'
        when total_amount >= 50 then 'medium'
        else 'low'
    end as segment
from {{ ref('mart_customer_orders') }}