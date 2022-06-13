import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models.dagrun import DagRun

from src.dags.project.utils import read_file


@task
def get_increment(increment_conn_name: str, target_path: str, dag_run: DagRun = DagRun) -> str:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S")
    else:
        increment_date = dag_run.execution_date.strftime("%Y-%m-%dT%H:%M:%S")
    # clear the target directory
    for f in os.listdir(target_path):
        os.remove(os.path.join(target_path, f))

    increment_conn = BaseHook.get_connection(increment_conn_name)
    hostname = increment_conn.host
    report_id = increment_conn.password
    url = f"{hostname}/get_increment?report_id={report_id}&date={increment_date}"
    headers = {
        "X-Nickname": Variable.get("NICKNAME"),
        "X-Cohort": "1",
        "X-Project": "True",
        "X-API-KEY": Variable.get("API_KEY"),
    }
    with requests.Session() as session:
        session.headers.update(headers)
        response = session.get(url, headers=session.headers)
        increment_id = response.json()["data"]["increment_id"]
        print(f"Successfully got increment_id for {increment_date}")

    return increment_id


@task
def download_user_orders_log_increment(target_path: str, increment_id: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y%m%d")
    else:
        increment_date = dag_run.execution_date.strftime("%Y%m%d")
    hostname = Variable.get("HOSTNAME_TO_GET_FILES")
    with requests.Session() as session:
        url = (
            f'{hostname}/s3-sprint3/cohort_1/{Variable.get("NICKNAME")}/project/{increment_id}/user_orders_log_inc.csv'
        )
        r = session.get(url)
        open(f"{target_path}/{increment_date}_user_orders_log_inc.csv", "wb").write(r.content)


@task
def download_user_activity_log_increment(target_path: str, increment_id: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y%m%d")
    else:
        increment_date = dag_run.execution_date.strftime("%Y%m%d")
    hostname = Variable.get("HOSTNAME_TO_GET_FILES")
    with requests.Session() as session:
        url = f'{hostname}/s3-sprint3/cohort_1/{Variable.get("NICKNAME")}/project/{increment_id}/user_activity_log_inc.csv'
        r = session.get(url)
        open(f"{target_path}/{increment_date}_user_activity_log_inc.csv", "wb").write(r.content)


@task
def download_customer_research_increment(target_path: str, increment_id: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y%m%d")
    else:
        increment_date = dag_run.execution_date.strftime("%Y%m%d")
    hostname = Variable.get("HOSTNAME_TO_GET_FILES")
    with requests.Session() as session:
        url = f'{hostname}/s3-sprint3/cohort_1/{Variable.get("NICKNAME")}/project/{increment_id}/customer_research_inc.csv'
        r = session.get(url)
        open(f"{target_path}/{increment_date}_customer_research_inc.csv", "wb").write(r.content)


@task
def truncate_user_orders_log_increment(pg_connection_name: str, dag_run: DagRun = DagRun):
    if dag_run.conf.get("increment_date"):
        increment_date = dag_run.conf.get("increment_date")
    else:
        increment_date = dag_run.execution_date.strftime("%Y-%m-%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    delete_stmt = f"DELETE FROM stage.user_orders_log WHERE date_time = '{increment_date}'"
    conn = psycopg2.connect(
        database=pg_conn.schema, user=pg_conn.login, password=pg_conn.password, host=pg_conn.host, port=pg_conn.port
    )
    cur = conn.cursor()
    cur.execute(delete_stmt)
    conn.commit()
    cur.close()
    conn.close()


@task
def truncate_user_activity_log_increment(pg_connection_name: str, dag_run: DagRun = DagRun):
    if dag_run.conf.get("increment_date"):
        increment_date = dag_run.conf.get("increment_date")
    else:
        increment_date = dag_run.execution_date.strftime("%Y-%m-%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    delete_stmt = f"DELETE FROM stage.user_activity_log WHERE date_time = '{increment_date}'"
    conn = psycopg2.connect(
        database=pg_conn.schema, user=pg_conn.login, password=pg_conn.password, host=pg_conn.host, port=pg_conn.port
    )
    cur = conn.cursor()
    cur.execute(delete_stmt)
    conn.commit()
    cur.close()
    conn.close()


@task
def truncate_customer_research_increment(pg_connection_name: str, dag_run: DagRun = DagRun):
    if dag_run.conf.get("increment_date"):
        increment_date = dag_run.conf.get("increment_date")
    else:
        increment_date = dag_run.execution_date.strftime("%Y-%m-%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    delete_stmt = f"DELETE FROM stage.customer_research WHERE date_id = '{increment_date}'"
    conn = psycopg2.connect(
        database=pg_conn.schema, user=pg_conn.login, password=pg_conn.password, host=pg_conn.host, port=pg_conn.port
    )
    cur = conn.cursor()
    cur.execute(delete_stmt)
    conn.commit()
    cur.close()
    conn.close()


@task
def load_user_orders_log_increment_to_pg(pg_connection_name: str, target_path: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y%m%d")
    else:
        increment_date = dag_run.execution_date.strftime("%Y%m%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    f = pd.read_csv(target_path + f"/{increment_date}_user_orders_log_inc.csv")
    f = f.iloc[:, 1:]

    cols = ",".join(list(f.columns))
    insert_stmt = f"INSERT INTO stage.user_orders_log ({cols}) VALUES %s"

    conn = psycopg2.connect(
        database=pg_conn.schema, user=pg_conn.login, password=pg_conn.password, host=pg_conn.host, port=pg_conn.port
    )
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    conn.commit()
    cur.close()
    conn.close()


@task
def load_user_activity_log_increment_to_pg(pg_connection_name: str, target_path: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y%m%d")
    else:
        increment_date = dag_run.execution_date.strftime("%Y%m%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    f = pd.read_csv(target_path + f"/{increment_date}_user_activity_log_inc.csv")
    f = f.iloc[:, 1:]

    cols = ",".join(list(f.columns))
    insert_stmt = f"INSERT INTO stage.user_activity_log ({cols}) VALUES %s"

    conn = psycopg2.connect(
        database=pg_conn.schema, user=pg_conn.login, password=pg_conn.password, host=pg_conn.host, port=pg_conn.port
    )
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    conn.commit()
    cur.close()
    conn.close()


@task
def load_customer_research_increment_to_pg(pg_connection_name: str, target_path: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = datetime.strptime(dag_run.conf.get("increment_date"), "%Y-%m-%d").strftime("%Y%m%d")
    else:
        increment_date = dag_run.execution_date.strftime("%Y%m%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    f = pd.read_csv(target_path + f"/{increment_date}_customer_research_inc.csv")

    cols = ",".join(list(f.columns))
    insert_stmt = f"INSERT INTO stage.customer_research ({cols}) VALUES %s"

    conn = psycopg2.connect(
        database=pg_conn.schema, user=pg_conn.login, password=pg_conn.password, host=pg_conn.host, port=pg_conn.port
    )
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    conn.commit()
    cur.close()
    conn.close()


@task
def load_d_customer_to_pg(pg_connection_name: str) -> None:
    pg_conn = BaseHook.get_connection(pg_connection_name)
    conn = psycopg2.connect(database=pg_conn.schema,
                            user=pg_conn.login,
                            password=pg_conn.password,
                            host=pg_conn.host,
                            port=pg_conn.port)
    cur = conn.cursor()
    query = read_file('src/dags/project/queries/d_customer.sql')
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


@task
def load_d_item_to_pg(pg_connection_name: str) -> None:
    pg_conn = BaseHook.get_connection(pg_connection_name)
    conn = psycopg2.connect(database=pg_conn.schema,
                            user=pg_conn.login,
                            password=pg_conn.password,
                            host=pg_conn.host,
                            port=pg_conn.port)
    cur = conn.cursor()
    query = read_file('src/dags/project/queries/d_item.sql')
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


@task
def load_f_daily_sales_to_pg(pg_connection_name: str, dag_run: DagRun = DagRun) -> None:
    if dag_run.conf.get("increment_date"):
        increment_date = dag_run.conf.get("increment_date")
    else:
        increment_date = dag_run.execution_date.strftime("%Y-%m-%d")
    pg_conn = BaseHook.get_connection(pg_connection_name)
    conn = psycopg2.connect(database=pg_conn.schema,
                            user=pg_conn.login,
                            password=pg_conn.password,
                            host=pg_conn.host,
                            port=pg_conn.port)
    cur = conn.cursor()
    query = read_file('src/dags/project/queries/f_daily_sales.sql')
    cur.execute(query, {"increment_date": increment_date})
    conn.commit()
    cur.close()
    conn.close()


@task
def calculate_customer_retention(pg_connection_name: str):
    pg_conn = BaseHook.get_connection(pg_connection_name)
    conn = psycopg2.connect(database=pg_conn.schema,
                            user=pg_conn.login,
                            password=pg_conn.password,
                            host=pg_conn.host,
                            port=pg_conn.port)
    cur = conn.cursor()
    query = read_file('src/dags/project/queries/customer_retention.sql')
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
