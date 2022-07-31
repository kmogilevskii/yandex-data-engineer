CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id INT4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id VARCHAR NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    delivery_id INT NOT NULL,
    courier_id INT NOT NULL,
    address TEXT NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    sum NUMERIC(14, 2) NOT NULL
);