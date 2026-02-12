-- {{ config(materialized='table') }}

-- WITH calendar AS (
--   SELECT
--     d AS date
--   FROM
--     UNNEST(GENERATE_DATE_ARRAY(
--       DATE '2015-01-01',
--       DATE '2035-12-31',
--       INTERVAL 1 DAY
--     )) AS d
-- )

-- SELECT
--   CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key,
--   date,
--   EXTRACT(DAY FROM date) AS day,
--   EXTRACT(MONTH FROM date) AS month,
--   FORMAT_DATE('%B', date) AS month_name,
--   EXTRACT(YEAR FROM date) AS year,
--   EXTRACT(QUARTER FROM date) AS quarter,
--   EXTRACT(WEEK FROM date) AS week_of_year,

--   -- BigQuery: 1=Sunday, 7=Saturday
--   EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
--   FORMAT_DATE('%A', date) AS day_name,

--   EXTRACT(DAYOFWEEK FROM date) IN (1,7) AS is_weekend,
--   date = LAST_DAY(date) AS is_month_end,
--   date = DATE(EXTRACT(YEAR FROM date), 12, 31) AS is_year_end

-- FROM calendar
-- ORDER BY date

{{ config(materialized='table') }}

with calendar as (
  select
    d as full_date
  from unnest(generate_date_array(
    date '2015-01-01',
    date '2035-12-31',
    interval 1 day
  )) as d
)

select
  cast(format_date('%Y%m%d', full_date) as int64) as date_key,

  -- Keep existing column name to avoid breaking downstream models
  full_date,

  -- New attributes
  extract(day from full_date) as day,
  extract(month from full_date) as month,
  format_date('%B', full_date) as month_name,
  extract(year from full_date) as year,
  extract(quarter from full_date) as quarter,
  extract(week from full_date) as week_of_year,

  -- BigQuery: 1=Sunday, 7=Saturday
  extract(dayofweek from full_date) as day_of_week,
  format_date('%A', full_date) as day_name,

  extract(dayofweek from full_date) in (1, 7) as is_weekend,
  full_date = last_day(full_date) as is_month_end,
  full_date = date(extract(year from full_date), 12, 31) as is_year_end,

  -- Keep this for existing gold monthly models
  cast(format_date('%Y%m%d', date_trunc(full_date, month)) as int64) as month_start_date_key

from calendar
order by full_date