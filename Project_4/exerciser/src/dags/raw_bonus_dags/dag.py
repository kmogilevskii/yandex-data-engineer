import logging

import pendulum
from airflow import DAG
from airflow.decorators import task

from src.dags.config_const import ConfigConst
from src.dags.raw_bonus_dags.event_loader import EventLoader
from src.dags.raw_bonus_dags.pg_connect import ConnectionBuilder
from src.dags.raw_bonus_dags.ranks_loader import RankLoader
from src.dags.raw_bonus_dags.stg_settings_repository import StgEtlSettingsRepository
from src.dags.raw_bonus_dags.users_loader import UserLoader

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_case_stg_bonus_system',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
) as dag:

    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION_CLOUD)
    origin_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_ORIGIN_BONUS_SYSTEM_CONNECTION)

    settings_repository = StgEtlSettingsRepository(dwh_pg_connect)

    @task(task_id="ranks_dict_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect)
        rest_loader.load_ranks()

    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect)
        user_loader.load_users()

    ranks_dict = load_ranks()
    events = load_events()
    users = load_users()

    ranks_dict  # type: ignore
    events  # type: ignore
    users  # type: ignore
