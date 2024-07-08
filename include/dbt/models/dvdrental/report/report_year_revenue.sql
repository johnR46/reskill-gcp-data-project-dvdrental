SELECT 
    dim_date.year,
    dim_date.month,
    count(distinct dim_movie.title) as num_title,
    sum(sales_amount) as total_revenue
FROM {{ ref('fact_sales') }} fact_sales
    JOIN {{ ref('dim_movie') }} dim_movie on dim_movie.movie_key = fact_sales.movie_key
    JOIN {{ ref('dim_date') }} dim_date on dim_date.date_key = fact_sales.date_key
    JOIN {{ ref('dim_customer') }} dim_customer on dim_customer.customer_key = fact_sales.customer_key
group by
    dim_date.year,
    dim_date.month
order by dim_date.year,
    dim_date.month