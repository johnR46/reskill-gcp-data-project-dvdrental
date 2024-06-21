CREATE TABLE dimDate
    (
      date_key integer NOT NULL PRIMARY KEY,
        date date NOT NULL,
        year smallint NOT NULL,
        quarter smallint NOT NULL,
        month smallint NOT NULL,
        day smallint NOT NULL,
        week smallint NOT NULL,
        is_weekend boolean
    );