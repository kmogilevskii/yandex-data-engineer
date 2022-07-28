import json
from datetime import datetime

from src.dags.stg_orders_dags.pg_connect import PgConnect


class PgSaver:
    def __init__(self, connect: PgConnect) -> None:
        self.conn = connect.client()

    def init_collection(self, collection_name: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS stg.ordersystem_{collection_name} (
                        id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                        object_id varchar NOT NULL UNIQUE,
                        object_value text NOT NULL,
                        update_ts timestamp NOT NULL
                    );
                """.format(collection_name=collection_name)
            )
            self.conn.commit()

    def save_object(self, collection_name: str, id: str, update_ts: datetime, val) -> None:
        str_val = json.dumps(self._to_dict(val))
        self._upsert_value(collection_name, id, update_ts, str_val)

    def _upsert_value(self, collection_name: str, id: str, update_ts: datetime, val: str):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_{collection_name}(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value;
                """.format(collection_name=collection_name),
                {
                    "id": id,
                    "val": val,
                    "update_ts": update_ts
                }
            )
            self.conn.commit()

    def _to_dict(self, obj, classkey=None):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, dict):
            data = {}
            for (k, v) in obj.items():
                data[k] = self._to_dict(v, classkey)
            return data
        elif hasattr(obj, "_ast"):
            return self._to_dict(obj._ast())
        elif hasattr(obj, "__iter__") and not isinstance(obj, str):
            return [self._to_dict(v, classkey) for v in obj]
        elif hasattr(obj, "__dict__"):
            data = dict([(key, self._to_dict(value, classkey))
                         for key, value in obj.__dict__.items()
                         if not callable(value) and not key.startswith('_')])
            if classkey is not None and hasattr(obj, "__class__"):
                data[classkey] = obj.__class__.__name__
            return data
        else:
            return obj
