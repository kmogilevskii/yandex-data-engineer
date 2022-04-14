DROP SCHEMA IF EXISTS analysis CASCADE;
DROP SCHEMA IF EXISTS production CASCADE;

CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS analysis;

-------------

CREATE TABLE production.Products(
    id int NOT NULL PRIMARY KEY,
    name varchar(2048) NOT NULL,
    price numeric(19, 5) NOT NULL DEFAULT 0 CHECK(price >= 0)
);

CREATE TABLE production.OrderStatuses(
    id int NOT NULL PRIMARY KEY, 
    key varchar(255) NOT NULL
);

CREATE TABLE production.Users(
    id int NOT NULL PRIMARY KEY, 
    name varchar(2048) NULL,
    login varchar(2048) NOT NULL
);

CREATE TABLE production.Orders(
    order_id int NOT NULL PRIMARY KEY,
    order_ts timestamp NOT NULL,
    user_id int NOT NULL,
    bonus_payment numeric(19, 5) NOT NULL DEFAULT 0,
    payment numeric(19, 5) NOT NULL DEFAULT 0 ,
    cost numeric(19, 5) NOT NULL DEFAULT 0 CHECK (cost = payment + bonus_payment),
    bonus_grant numeric(19, 5) NOT NULL DEFAULT 0,
    status int NOT NULL
);

CREATE TABLE production.OrderItems(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id int NOT NULL REFERENCES production.Products(id),
    order_id int NOT NULL REFERENCES production.Orders(order_id),
    name varchar(2048) NOT NULL,
    price numeric(19, 5) NOT NULL DEFAULT 0 CHECK(price >= 0),
    discount numeric(19, 5) NOT NULL DEFAULT 0 CHECK(discount >= 0 AND discount <= price),
    quantity int NOT NULL CHECK(quantity > 0),
    UNIQUE(order_id, product_id)
);

CREATE TABLE production.OrderStatusLog(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id int NOT NULL REFERENCES production.Orders(order_id),
    status_id int NOT NULL REFERENCES production.OrderStatuses(id),
    dttm timestamp NOT NULL,
    UNIQUE(order_id, status_id)
);

