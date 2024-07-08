SELECT 
    dim_date.year,
    dim_date.month,
    count(distinct dim_movie.title) as num_title,
    sum(sales_amount) as total_revenue
FROM `deep-equator-427111-t7`.`dvdrental`.`fact_sales` fact_sales
    JOIN `deep-equator-427111-t7`.`dvdrental`.`dim_movie` dim_movie on dim_movie.movie_key = fact_sales.movie_key
    JOIN `deep-equator-427111-t7`.`dvdrental`.`dim_date` dim_date on dim_date.date_key = fact_sales.date_key
    JOIN `deep-equator-427111-t7`.`dvdrental`.`dim_customer` dim_customer on dim_customer.customer_key = fact_sales.customer_key
group by
    dim_date.year,
    dim_date.month
order by dim_date.year,
    dim_date.month