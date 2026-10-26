-- Arithmetic identity of the mart: net = captured - refunded, booked >= 0.
select revenue_date
from {{ ref('mart_daily_revenue') }}
where net_collected_cents <> captured_cents - refunded_cents
   or booked_revenue_cents < 0
   or orders_cancelled > orders_placed
