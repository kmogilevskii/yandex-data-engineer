UPDATE mart.d_item
SET item_name = (SELECT DISTINCT s.name FROM  
(SELECT DISTINCT item_id, last_value(item_name) OVER(PARTITION BY item_id ORDER BY date_time DESC) AS name 
FROM stage.user_orders_log) s WHERE s.item_id = mart.d_item.item_id);


INSERT INTO mart.d_item (item_id, item_name)
SELECT 
    DISTINCT item_id, LAST_VALUE(item_name) OVER(PARTITION BY item_id ORDER BY date_time DESC) AS name
FROM stage.user_orders_log
WHERE item_id NOT IN (SELECT DISTINCT item_id FROM mart.d_item)
ORDER BY item_id;