CREATE TABLE IF NOT EXISTS stg.couriers (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);