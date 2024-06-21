drop table if exists dimCustomer;
create table dimCustomer
    (
      customer_key SERIAL primary key,
      customer_id smallint not null,
      first_name varchar(45) not null,
      last_name varchar(45) not null,
      email varchar(50),
      address varchar(50) not null,
      address2 varchar(50),
      district varchar(20) not null,
      city varchar(50) not null,
      country varchar(50) not null,
      postal_code varchar(10),
      phone varchar(20) not null,
      active smallint not null,
      create_date timestamp not null,
      start_date date not null,
      end_date date not null
    );