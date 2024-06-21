insert into  dimdate (
date_key,
date,
year,
quarter,
month,
day,
week,
is_weekend
)
select
    distinct (to_char(payment_date :: DATE, 'yyyMMDD') ::integer) as date_key,
	date(payment_date) as date,
	extract (year from payment_date) as year,
	extract(quarter from payment_Date) as quarter,
	extract(month from payment_Date) as month,
	extract(day from payment_Date) as day,
	extract(week from payment_Date) as week,
	case
		when EXTRACT(ISODOW from payment_date)in (6,7) then true
		else false
	end as is_weekend
	
from
	payment ;


--select * from dimdate d 