
  
    

    create or replace table `deep-equator-427111-t7`.`dvdrental`.`report_revenue`
    
    

    OPTIONS()
    as (
      -- start schema
SELECT dim_movie.title,
    dim_date.month,
    dim_customer.city,
    sum(sales_amount) as revenue
FROM `deep-equator-427111-t7`.`dvdrental`.`fact_sales` fact_sales
    JOIN `deep-equator-427111-t7`.`dvdrental`.`dim_movie` dim_movie on dim_movie.movie_key = fact_sales.movie_key
    JOIN `deep-equator-427111-t7`.`dvdrental`.`dim_date` dim_date on dim_date.date_key = fact_sales.date_key
    JOIN `deep-equator-427111-t7`.`dvdrental`.`dim_customer` dim_customer on dim_customer.customer_key = fact_sales.customer_key
group by dim_movie.title,
    dim_date.month,
    dim_customer.city
order by dim_movie.title,
    dim_date.month,
    dim_customer.city,
    revenue desc
    );
  