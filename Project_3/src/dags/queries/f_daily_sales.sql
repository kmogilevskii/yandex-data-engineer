DELETE FROM mart.f_daily_sales
WHERE date_id IN (SELECT distinct dc.date_id 
                  FROM stage.user_orders_log 
                  JOIN mart.d_calendar dc ON dc.day_num = EXTRACT(DAY FROM date_time)
                                          AND dc.month_num = EXTRACT(MONTH FROM date_time)
                                          AND dc.year_num = EXTRACT(YEAR FROM date_time)
                  WHERE date_time = %(increment_date)s);



INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, quantity, payment_amount, status)
SELECT DISTINCT dc.date_id, item_id, customer_id, quantity, payment_amount AS amount, status
FROM stage.user_orders_log         
JOIN mart.d_calendar dc ON dc.day_num = EXTRACT(DAY FROM date_time)
AND dc.month_num = EXTRACT(MONTH FROM date_time)
AND dc.year_num = EXTRACT(YEAR FROM date_time)
WHERE date_time = %(increment_date)s;