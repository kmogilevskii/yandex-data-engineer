insert into dds.dm_deliveries (order_id,order_ts,delivery_id,courier_id,address,delivery_ts,sum)
    SELECT object_id as order_id,
	   (object_value::JSON->>'order_ts')::date as order_ts,
	   (object_value::JSON->>'delivery_id') as delivery_id,
	   (object_value::JSON->>'courier_id') as courier_id,
	   (object_value::JSON->>'address') as address,
	   (object_value::JSON->>'delivery_ts')::date as delivery_ts,
	   (object_value::JSON->>'sum')::numeric as sum
    FROM stg.project_deliveries
    ON CONFLICT (delivery_id) DO NOTHING;
