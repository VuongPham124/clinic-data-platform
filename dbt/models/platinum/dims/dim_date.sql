{{ config(materialized='table') }}

WITH calendar AS (
  SELECT
    d AS date
  FROM
    UNNEST(GENERATE_DATE_ARRAY(
      DATE '2015-01-01',
      DATE '2035-12-31',
      INTERVAL 1 DAY
    )) AS d
)

SELECT
  CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key,
  date,
  EXTRACT(DAY FROM date) AS day,
  EXTRACT(MONTH FROM date) AS month,
  FORMAT_DATE('%B', date) AS month_name,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(QUARTER FROM date) AS quarter,
  EXTRACT(WEEK FROM date) AS week_of_year,

  -- BigQuery: 1=Sunday, 7=Saturday
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  FORMAT_DATE('%A', date) AS day_name,

  EXTRACT(DAYOFWEEK FROM date) IN (1,7) AS is_weekend,
  date = LAST_DAY(date) AS is_month_end,
  date = DATE(EXTRACT(YEAR FROM date), 12, 31) AS is_year_end

FROM calendar
ORDER BY date;