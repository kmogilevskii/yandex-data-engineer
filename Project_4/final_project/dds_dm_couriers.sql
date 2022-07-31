insert into dds.dm_couriers(courier_id, courier_name, active_from)
    SELECT object_id, (object_value::JSON->> 'name') as name, update_ts
    FROM stg.project_couriers as stg_c
    ON CONFLICT(courier_id) DO NOTHING;
    update dds.dm_couriers as v
    set
    active_to = now()
    where    NOT    EXISTS(
          SELECT    FROM    stg.project_couriers    c1
            WHERE c1.object_id = v.courier_id);