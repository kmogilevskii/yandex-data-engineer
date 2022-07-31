from datetime import datetime
from pathlib import Path
import requests
import json
from airflow.hooks.postgres_hook import PostgresHook


def read_file(path: str) -> str:
    path = Path(__file__).parents[3].joinpath(path)
    with open(path) as f:
        return f.read()


def init_table(table_name: str, postgres_conn_id: str) -> None:
    hook = PostgresHook(postgres_conn_id)
    table_name = table_name.replace('.', '_')
    query = read_file(f'src/dags/init_queries/init_{table_name}.sql')
    hook.run(query)


def api_to_stg(url: str,
               suffix: str,
               headers: dict,
               postgres_conn_id: str,
               table_name: str,
               id_col: str) -> None:
    r = requests.get(url + suffix, headers=headers)
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    current_timestamp = datetime.utcnow()
    wf_setting = {
        'date': current_timestamp,
    }
    response = json.loads(r.content)
    if r.status_code == 200 and response:
        print(f"Received {len(response)} records for insertion.")
        query = f"INSERT INTO {table_name} (object_id, object_value, update_ts) VALUES "
        for record in response:
            object_id = record[id_col]
            record_json = json.dumps(record)
            query += f"('{object_id}', '{record_json}', '{current_timestamp}'),"
        hook.run(query[:-1])
        wf_setting['status'] = 'SUCCESS'
    else:
        print(f"Failed to fetch data for {table_name} from API. Status code {r.status_code}")
        wf_setting['status'] = 'FAILED'
    hook.run(f"INSERT INTO srv_wf_settings (workflow_key, workflow_settings) VALUES ('{table_name}_key', '{wf_setting}');")
