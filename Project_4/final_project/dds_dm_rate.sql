insert into dds.dm_rate (delivery_id, rate)
    SELECT
	   (object_value::JSON->>'delivery_id') as delivery_id,
	   (object_value::JSON->>'rate')::smallint  as rate
    FROM stg.project_deliveries
    ON CONFLICT (delivery_id) DO NOTHING;