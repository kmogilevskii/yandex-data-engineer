# Задание 2. Проектирование снежинки.

```SQL
DROP TABLE IF EXISTS dds.fct_product_sales;
DROP TABLE IF EXISTS dds.srv_wf_settings;
DROP TABLE IF EXISTS dds.dm_orders;
DROP TABLE IF EXISTS dds.dm_products;
DROP TABLE IF EXISTS dds.dm_restaurants;
DROP TABLE IF EXISTS dds.dm_users;
DROP TABLE IF EXISTS dds.dm_timestamps;


CREATE TABLE dds.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    workflow_key varchar UNIQUE,
    workflow_settings text
);

CREATE TABLE dds.dm_restaurants(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    restaurant_id varchar NOT NULL,
    restaurant_name text NOT NULL,

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
);

CREATE TABLE dds.dm_products (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    
    product_id varchar NOT NULL,
    product_name text NOT NULL,
    product_price numeric(19, 5) NOT NULL DEFAULT 0 CHECK (product_price >= 0),

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL,
    
    restaurant_id int NOT NULL REFERENCES dds.dm_restaurants(id)
);

CREATE TABLE dds.dm_timestamps(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    ts timestamp NOT NULL,

    year int NOT NULL CHECK(year >= 2020 AND year < 2500),
    month int NOT NULL CHECK(month >= 0 AND month <= 12),
    day int NOT NULL CHECK(day >= 0 AND day <= 31),
    time time NOT NULL,
    date date NOT NULL
);

CREATE TABLE dds.dm_users(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    user_id varchar NOT NULL,
    user_name varchar NOT NULL,
    user_login varchar NOT NULL
);

CREATE TABLE dds.dm_orders(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    order_key varchar NOT NULL,
    order_status varchar NOT NULL,
    
    restaurant_id int NOT NULL REFERENCES dds.dm_restaurants(id),
    timestamp_id int NOT NULL REFERENCES dds.dm_timestamps(id),
    user_id int NOT NULL REFERENCES dds.dm_users(id)
);


CREATE TABLE dds.fct_product_sales (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id int NOT NULL REFERENCES dds.dm_products(id),
    order_id int NOT NULL REFERENCES dds.dm_orders(id),
    count int NOT NULL DEFAULT 0 CHECK (count >= 0),
    price numeric(19, 5) NOT NULL DEFAULT 0 CHECK (price >= 0),
    total_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (total_sum >= 0),
    bonus_payment numeric(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0),
    bonus_grant numeric(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0)
);

GRANT SELECT ON all tables IN SCHEMA dds TO sp5_de_tester;
```
