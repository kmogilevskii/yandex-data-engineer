CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id INT4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR UNIQUE,
    workflow_settings TEXT
);