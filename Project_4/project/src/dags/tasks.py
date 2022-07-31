from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from src.dags.utils import init_table, api_to_stg


@task
def init_stg_restaurants(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "stg.restaurants")


@task
def init_stg_couriers(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "stg.couriers")


@task
def init_stg_deliveries(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "stg.deliveries")


@task
def init_stg_srv_wf_settings(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "stg.srv_wf_settings")


@task
def truncate_restaurants(postgres_conn_id: str) -> None:
    hook = PostgresHook(postgres_conn_id)
    hook.run("TRUNCATE TABLE IF EXISTS stg.restaurants CASCADE;")


@task
def truncate_couriers(postgres_conn_id: str) -> None:
    hook = PostgresHook(postgres_conn_id)
    hook.run("TRUNCATE TABLE IF EXISTS stg.couriers CASCADE;")


@task
def truncate_deliveries(postgres_conn_id: str) -> None:
    hook = PostgresHook(postgres_conn_id)
    hook.run("TRUNCATE TABLE IF EXISTS stg.deliveries CASCADE;")


@task
def insert_into_restaurants(url: str,
                            headers: dict,
                            postgres_conn_id: str) -> None:
    api_to_stg(url=url,
               suffix='/restaurants',
               headers=headers,
               postgres_conn_id=postgres_conn_id,
               table_name='restaurants',
               id_col='_id')


@task
def insert_into_couriers(url: str,
                         headers: dict,
                         postgres_conn_id: str) -> None:
    api_to_stg(url=url,
               suffix='/couriers',
               headers=headers,
               postgres_conn_id=postgres_conn_id,
               table_name='couriers',
               id_col='_id')

@task
def insert_into_deliveries(url: str,
                           headers: dict,
                           postgres_conn_id: str) -> None:
    api_to_stg(url=url,
               suffix='/deliveries',
               headers=headers,
               postgres_conn_id=postgres_conn_id,
               table_name='deliveries',
               id_col='order_id')


@task
def init_dds_restaurants(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "dds.dm_restaurants")


@task
def init_dds_couriers(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "dds.dm_couriers")


@task
def init_dds_deliveries(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "dds.dm_deliveries")


@task
def init_dds_tips(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "dds.dm_tips")


@task
def init_dds_rates(postgres_conn_id: str) -> None:
    init_table(postgres_conn_id, "dds.dm_rates")