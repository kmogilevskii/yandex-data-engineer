import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from bson.json_util import dumps, loads
import logging
from airflow.models.variable import Variable
from airflow.operators.postgres_operator import PostgresOperator
import os

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
url = Variable.get("PROJECT_URL")
nickname = Variable.get("PROJECT_NICKNAME")
cohort = Variable.get("PROJECT_COHORT")
api_key = Variable.get("PROJECT_X-API-KEY")
postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'

headers = {
        "X-API-KEY": api_key,
        "X-Nickname": nickname,
        "X-Cohort": str(cohort)
    }

def delete_data_staging(postgres_conn_id, table):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    query = f'DELETE from {table}'
    cur.execute(query)
    conn.commit()

def api_load_data_to_stg(url, headers, postgres_conn_id, method_url, table, key):
    # делаем запрос на формирование данных
    r = requests.get(url + method_url, headers=headers)
    # подключемся к базе данных для переноса информации
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    p_date = datetime.utcnow()
    response_dict_r = json.loads(r.content)

    # Когда мы получили ответ по API, у нас 2 пути.
    # Первый - если все хорошо , то мы записываем данные в базу данных по ресторанам и в лог с временем успешного процесса
    # Второй - если ответ пуст или ошибка, то мы записываем этот лог в базу данных как FAILED и вызываем ошибку DAGa

    if r.status_code == 200 and response_dict_r != []:
        logging.info('Connection succeeded, data recieved')
        for i in response_dict_r:
            obj_id = i[key]
            json_data = dumps(i, indent=2)
            cur.execute(
                "INSERT INTO %s (object_id, object_value, update_ts) VALUES ('%s','%s','%s')" % (
                table, obj_id, json_data, p_date))
            conn.commit()
        cur.execute(
            "INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings) VALUES ('%s', '%s');" % (p_date, table))
        conn.commit()
    else:
        cur.execute(
                "INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings) VALUES ('%s', 'FAILED %s');" % (p_date, table))
        conn.commit()
        logging.error('API not parsed completely/correctly')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="Project-Nemesis",
         default_args={},
         start_date=datetime(2015, 1, 1),
         schedule_interval='@daily',
         # dagrun_timeout=timedelta(minutes=180),
         tags=["Alchemy"], catchup=False) as dag:

    # Первая группа DAG отвечает за очистку staging слоя

    #DELETE FROM stg.project_restaurants

    with TaskGroup("ETL_clean_staging_temp_data") as group0:
        task11 = PythonOperator(
            task_id="delete_restaurants",
            python_callable=delete_data_staging,
            provide_context=True,
            op_kwargs={'postgres_conn_id': postgres_conn_id, 'table': 'stg.project_restaurants'},
        )
        task12 = PythonOperator(
            task_id="delete_couriers",
            python_callable=delete_data_staging,
            provide_context=True,
            op_kwargs={'postgres_conn_id': postgres_conn_id, 'table': 'stg.project_couriers'},
        )
        task13 = PythonOperator(
            task_id="delete_deliveries",
            python_callable=delete_data_staging,
            provide_context=True,
            op_kwargs={'postgres_conn_id': postgres_conn_id, 'table': 'stg.project_deliveries'},
        )
        [task11, task12, task13]


    # Вторая группа DAG отвечает за наполнение слоя stage из API

    with TaskGroup("ETL_staging_from_API") as group1:
        task1 = PythonOperator(
            task_id="API_restaurants",
            python_callable=api_load_data_to_stg,
            provide_context=True,
            op_kwargs={'url': url, 'headers': headers, 'postgres_conn_id': postgres_conn_id, 'method_url': '/restaurants', 'table': 'stg.project_restaurants', 'key': '_id'},
        )
        task2 = PythonOperator(
            task_id="API_couriers",
            python_callable=api_load_data_to_stg,
            provide_context=True,
            op_kwargs={'url': url, 'headers': headers, 'postgres_conn_id': postgres_conn_id, 'method_url': '/couriers', 'table': 'stg.project_couriers', 'key': '_id'},
        )
        task3 = PythonOperator(
            task_id="API_deliveries",
            python_callable=api_load_data_to_stg,
            provide_context=True,
            op_kwargs={'url': url, 'headers': headers, 'postgres_conn_id': postgres_conn_id, 'method_url': '/deliveries', 'table': 'stg.project_deliveries', 'key': 'order_id'},
        )
        [task1, task2, task3]

        # Третья группа DAG отвечает за наполнение слоя dds из stage

    with TaskGroup("ETL_DDS_from_staging") as group2:
        task5 = PostgresOperator(
            task_id="DDS_couriers",
            postgres_conn_id=postgres_conn_id,
            sql='sql/dds_dm_couriers.sql',
        )
        task4 = PostgresOperator(
                task_id="DDS_restaurants",
                postgres_conn_id=postgres_conn_id,
                sql='sql/dds_dm_restaurants.sql',
            )
        task6 = PostgresOperator(
                task_id="DDS_deliveries",
                postgres_conn_id=postgres_conn_id,
                sql='sql/dds_dm_deliveries.sql',
            )
        task7 = PostgresOperator(
            task_id="DDS_rate",
            postgres_conn_id=postgres_conn_id,
            sql='sql/dds_dm_rate.sql',
        )
        task8 = PostgresOperator(
            task_id="DDS_tips",
            postgres_conn_id=postgres_conn_id,
            sql='sql/dds_dm_tips.sql',
        )
        [task4 >> task5 >> task6 >> task7 >> task8]


group0 >> group1 >> group2