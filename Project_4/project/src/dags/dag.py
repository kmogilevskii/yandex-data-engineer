from src.dags.tasks import init_stg_restaurants, init_stg_couriers, init_stg_deliveries, init_stg_srv_wf_settings, truncate_stg_restaurants, truncate_stg_couriers, truncate_stg_deliveries, insert_into_stg_restaurants, insert_into_stg_couriers, insert_into_stg_deliveries, init_dds_restaurants, init_dds_couriers, init_dds_deliveries, init_dds_tips, init_dds_rates

from airflow.models import Variable
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
        "postgres_conn_id": "postgres",
        'headers': {
            "X-API-KEY": Variable.get("PROJECT_X-API-KEY"),
            "X-Nickname": Variable.get("PROJECT_NICKNAME"),
            "X-Cohort": str(Variable.get("PROJECT_COHORT"))
        },
        'url': Variable.get("PROJECT_URL")
    },
    schedule_interval=None,
    start_date=datetime(2022, 7, 29),
    catchup=True,
    tags=["Yandex Praktikum"],
)
def etl_update_user_data():
    """
    Yandex Praktikum ETL Project
    """
    init_stg_srv_wf_settings() >> (
        init_stg_restaurants() >> truncate_stg_restaurants() >> insert_into_stg_restaurants() >> init_dds_restaurants(),
        init_stg_couriers() >> truncate_stg_couriers() >> insert_into_stg_couriers() >> init_dds_couriers(),
        init_stg_deliveries() >> truncate_stg_deliveries() >> insert_into_stg_deliveries() >> init_dds_deliveries()
    ) >> init_dds_tips() >> init_dds_rates()


t = etl_update_user_data()
