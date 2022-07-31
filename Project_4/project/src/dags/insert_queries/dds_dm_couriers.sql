insert into dds.dm_couriers(courier_id, courier_name, active_from, active_to)
    SELECT object_id, (object_value::JSON->> 'name') as name, update_ts, '2099-01-01'
    FROM stg.project_couriers as stg_c
    ON CONFLICT(courier_id) DO NOTHING;
