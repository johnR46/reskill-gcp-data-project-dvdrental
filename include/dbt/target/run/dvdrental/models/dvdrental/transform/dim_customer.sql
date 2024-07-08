
  
    

    create or replace table `deep-equator-427111-t7`.`dvdrental`.`dim_customer`
    
    

    OPTIONS()
    as (
      select 
	c.customer_id as customer_key,
	c.customer_id,
	c.first_name,
	c.last_name,
	c.email,
	a.address,
	a.address2,
	a.district,
	ci.city,
	co.country,
	a.postal_code,
	a.phone,
	c.active,
	c.create_date,
	CURRENT_DATE() as start_date,
	CURRENT_DATE() as end_date
from
	`deep-equator-427111-t7`.`dvdrental`.`raw_customer` c
join `deep-equator-427111-t7`.`dvdrental`.`raw_address` a on
	c.address_id = a.address_id
join `deep-equator-427111-t7`.`dvdrental`.`raw_city` ci on
	a.city_id = ci.city_id
join `deep-equator-427111-t7`.`dvdrental`.`raw_country` co on
	ci.country_id = co.country_id
    );
  