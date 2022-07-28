DROP TABLE IF EXISTS stg.srv_wf_settings;
DROP TABLE IF EXISTS stg.bonussystem_ranks;
DROP TABLE IF EXISTS stg.bonussystem_users;
DROP TABLE IF EXISTS stg.bonussystem_events;

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar UNIQUE,
    workflow_settings text
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id int NOT NULL PRIMARY KEY,
    name varchar(2048) NOT NULL,
    bonus_percent numeric(19, 5) NOT NULL DEFAULT 0 CHECK(bonus_percent >= 0),
    min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0 CHECK(bonus_percent >= 0)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
    id int NOT NULL PRIMARY KEY,
    order_user_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id int NOT NULL PRIMARY KEY,
    event_ts timestamp NOT NULL,
    event_type varchar NOT NULL,
    event_value text NOT NULL
);
CREATE INDEX IF NOT EXISTS IDX_bonussystem_events__event_ts ON stg.bonussystem_events (event_ts);

--GRANT SELECT ON all tables IN SCHEMA stg TO sp5_de_tester;