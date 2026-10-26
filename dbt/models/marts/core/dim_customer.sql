-- Grain: one row per customer. Key: customer_id. SCD Type 1 (current attributes).
select customer_id, email, full_name, country_code, created_at, source_updated_at
from {{ ref('stg_customers') }}
