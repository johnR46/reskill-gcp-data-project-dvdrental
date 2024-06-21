insert
	into
	dimmovie
(
	movie_key,
	film_id,
	title,
	description,
	release_year,
	"language",
	original_language,
	rental_duration,
	length,
	rating,
	special_features
)
select
	f.film_id as movie_key,
	f.film_id,
	f.title,
	f.description,
	f.release_year,
	l.name as language,
	orig_lang.name as original_language,
	f.rental_duration,
	f.length,
	f.rating,
	f.special_features
FROM film f
JOIN language l              ON (f.language_id=l.language_id)
LEFT JOIN language orig_lang ON (f.language_id = orig_lang.language_id);