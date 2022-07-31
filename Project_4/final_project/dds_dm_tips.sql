insert into dds.dm_tips (delivery_id, tip_sum)
    SELECT
	   (object_value::JSON->>'delivery_id') as delivery_id,
	   (object_value::JSON->>'tip_sum')::numeric as tip_sum
    FROM stg.project_deliveries
    ON CONFLICT (delivery_id) DO NOTHING;