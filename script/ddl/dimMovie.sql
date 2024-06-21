CREATE TABLE dimMovie
    (
      movie_key          SERIAL PRIMARY KEY,
      film_id            smallint NOT NULL,
      title              varchar(255) NOT NULL,
      description        text,
      release_year       year,
      language           varchar(20) NOT NULL,
      original_language  varchar(20),
      rental_duration    smallint NOT NULL,
      length             smallint NOT NULL,
      rating             varchar(5) NOT NULL,
      special_features   varchar(60) NOT NULL
    );