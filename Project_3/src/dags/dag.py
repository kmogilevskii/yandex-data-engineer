from src.dags.project.tasks import get_increment, download_user_orders_log_increment, \
download_user_activity_log_increment, download_customer_research_increment , truncate_user_orders_log_increment, \
truncate_user_activity_log_increment, truncate_customer_research_increment, load_d_customer_to_pg, \
    load_d_item_to_pg, load_f_daily_sales_to_pg, calculate_customer_retention, load_user_orders_log_increment_to_pg, \
        load_user_activity_log_increment_to_pg, load_customer_research_increment_to_pg


from airflow.decorators import dag
from datetime import datetime, timedelta


@dag(
    default_args={
        "owner": "student",
        "email": ["student@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "target_path": "/lessons/increments",
        "increment_conn_name": "increment_conn",
        "pg_connection_name": "pg_connection",
    },
    schedule_interval=None,
    start_date=datetime(2022, 5, 18),
    catchup=True,
    tags=["Yandex Praktikum"],
)
def etl_update_user_data():
    """
    Yandex Praktikum ETL Project
    """
    increment_id = get_increment()
    (
        download_user_orders_log_increment(increment_id=increment_id)
        >> truncate_user_orders_log_increment()
        >> load_user_orders_log_increment_to_pg(),
        download_user_activity_log_increment(increment_id=increment_id)
        >> truncate_user_activity_log_increment()
        >> load_user_activity_log_increment_to_pg(),
        download_customer_research_increment(increment_id=increment_id)
        >> truncate_customer_research_increment()
        >> load_customer_research_increment_to_pg(),
    ) >> load_d_customer_to_pg() >> load_d_item_to_pg() >> load_f_daily_sales_to_pg() >> calculate_customer_retention()


t = etl_update_user_data()
