TRUNCATE mart.customer_retention;
INSERT INTO mart.customer_retention (period_name, 
                                     period_id, 
                                     returning_customers_count, 
                                     new_customers_count, 
                                     refunded_customer_count, 
                                     customers_refunded,
                                     new_customers_revenue,
                                     returning_customers_revenue)
SELECT 
    'weekly'                                                          AS period_name,
    week_num                                                          AS period_id,
    SUM(CASE WHEN num_transactions > 1 THEN 1 ELSE 0 END)             AS returning_customers_count,
    SUM(CASE WHEN num_transactions = 1 THEN 1 ELSE 0 END)             AS new_customers_count,
    SUM(CASE WHEN num_returns > 0 THEN 1 ELSE 0 END)                  AS refunded_customer_count,
    SUM(num_returns)                                                  AS customers_refunded,
    SUM(CASE WHEN num_transactions = 1 THEN total_revenue ELSE 0 END) AS new_customers_revenue,
    SUM(CASE WHEN num_transactions > 1 THEN total_revenue ELSE 0 END) AS returning_customers_revenue
FROM
    (
    SELECT 
        dc.week_num,
        customer_id,
        COUNT(*)                                                                              AS num_transactions,
        SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END)                                  AS num_returns,
        SUM(CASE WHEN status = 'refunded' THEN (-1) * payment_amount ELSE payment_amount END) AS total_revenue
    FROM 
        mart.f_daily_sales fds
    JOIN mart.d_calendar dc ON dc.date_id = fds.date_id
    GROUP BY
            dc.week_num,
            customer_id
    ) as customers_orders
GROUP BY
    period_name,
    period_id;