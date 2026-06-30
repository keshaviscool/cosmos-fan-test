{{ config(tags=['staging']) }}

select customer_id, name
from {{ ref('raw_customers') }}