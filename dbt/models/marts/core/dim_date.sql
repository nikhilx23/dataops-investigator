-- Grain: one row per calendar date (UTC). Key: date_key (yyyymmdd).
select
    to_char(d, 'YYYYMMDD')::integer      as date_key,
    d::date                              as calendar_date,
    extract(isodow from d)::integer      as iso_day_of_week,
    extract(isodow from d) in (6, 7)     as is_weekend,
    extract(week from d)::integer        as iso_week,
    extract(month from d)::integer       as month_number,
    to_char(d, 'YYYY-MM')                as year_month,
    extract(year from d)::integer        as year_number
from generate_series('2020-01-01'::date, '2027-12-31'::date, interval '1 day') as d
