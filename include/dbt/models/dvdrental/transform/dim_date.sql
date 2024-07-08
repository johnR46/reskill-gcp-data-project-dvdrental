SELECT DISTINCT
    CAST(FORMAT_DATE('%Y%m%d', DATE(payment_date)) AS INT64) AS date_key,
    DATE(payment_date) AS date,
    EXTRACT(YEAR FROM payment_date) AS year,
    EXTRACT(QUARTER FROM payment_date) AS quarter,
    EXTRACT(MONTH FROM payment_date) AS month,
    EXTRACT(DAY FROM payment_date) AS day,
    EXTRACT(WEEK FROM payment_date) AS week,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM payment_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM
    {{ source('dvdrental', 'raw_payment') }}
