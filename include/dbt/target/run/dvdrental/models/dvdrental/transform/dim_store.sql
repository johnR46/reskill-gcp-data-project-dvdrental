
  
    

    create or replace table `deep-equator-427111-t7`.`dvdrental`.`dim_store`
    
    

    OPTIONS()
    as (
      select
	s.store_id as store_key,
	s.store_id,
	a.address,
	a.address2,
	a.district,
	c.city,
	co.country,
	a.postal_code,
	st.first_name as manager_first_name,
	st.last_name as manager_last_name,
	CURRENT_DATE() as start_date,
	CURRENT_DATE() as end_date
FROM
	`deep-equator-427111-t7`.`dvdrental`.`raw_store` s
JOIN `deep-equator-427111-t7`.`dvdrental`.`raw_staff` st ON s.manager_staff_id = st.staff_id
JOIN `deep-equator-427111-t7`.`dvdrental`.`raw_address` a ON s.address_id = a.address_id
JOIN `deep-equator-427111-t7`.`dvdrental`.`raw_city` c ON a.city_id = c.city_id
JOIN `deep-equator-427111-t7`.`dvdrental`.`raw_country` co ON c.country_id = co.country_id
    );
  