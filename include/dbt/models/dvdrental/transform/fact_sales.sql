select
	CAST(
		FORMAT_DATE ('%Y%m%d', DATE (payment_date)) AS INT64
	) AS date_key,
	p.customer_id as customer_key,
	i.film_id as movie_key,
	i.store_id as store_key,
	p.amount as sales_amount
from
	 {{ source('dvdrental', 'raw_payment') }} p
join {{ source('dvdrental', 'raw_rental') }} r on p.rental_id = r.rental_id
join {{ source('dvdrental', 'raw_inventory') }} i on r.inventory_id = i.inventory_id