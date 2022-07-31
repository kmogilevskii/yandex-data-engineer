insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
    SELECT object_id, (object_value::JSON->>'name') as name, update_ts, '2099-12-31 00:00:00.000'
    FROM stg.project_restaurants as stg_c
    ON CONFLICT (restaurant_id) DO NOTHING;
    update dds.dm_restaurants as v
    set active_to = now()
    where NOT EXISTS (
    SELECT FROM stg.project_restaurants c1
    WHERE  c1.object_id  = v.restaurant_id
    );