INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT
	customer_id, first_name, last_name, min(city_id)
FROM 
	stage.user_orders_log 
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM mart.d_customer)
GROUP BY customer_id, first_name, last_name;