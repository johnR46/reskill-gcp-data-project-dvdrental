DROP TABLE IF EXISTS factSales;
CREATE TABLE factSales
    (
        sales_key SERIAL PRIMARY KEY,
        date_key integer REFERENCES dimDate (date_key),
        customer_key integer REFERENCES dimCustomer (customer_key),
        movie_key integer REFERENCES dimMovie (movie_key),
        store_key integer REFERENCES dimStore (store_key),
        sales_amount numeric
    );
