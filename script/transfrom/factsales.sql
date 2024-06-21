insert
	into
	factsales
(
	date_key,
	customer_key,
	movie_key,
	store_key,
	sales_amount
)

select
	to_char(payment_date :: DATE, 'yyyMMDD') ::integer as date_key,
	p.customer_id as customer_key,
	i.film_id as movie_key,
	i.store_id as store_key,
	p.amount as sales_amount
from
	payment p
join rental r on p.rental_id  = r.rental_id
join inventory i on r.inventory_id = i.inventory_id 