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
FROM `deep-equator-427111-t7`.`dvdrental`.`raw_film` f
JOIN `deep-equator-427111-t7`.`dvdrental`.`raw_language` l              ON f.language_id=l.language_id
LEFT JOIN `deep-equator-427111-t7`.`dvdrental`.`raw_language` orig_lang ON f.language_id = orig_lang.language_id