from datetime import datetime

from typing import Dict, List

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.pg_connect import PgConnect


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class BonusEventRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_events(self, event_type: str, last_loaded_record_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = %(event_type)s AND id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {
                    "event_type": event_type,
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs
