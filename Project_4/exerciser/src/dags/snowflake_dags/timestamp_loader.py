import json
from datetime import date, datetime, time
from typing import Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.snowflake_dags.order_repositories import OrderJsonObj, OrderRawRepository
from src.dags.pg_connect import PgConnect
from src.dags.snowflake_dags.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository


class TimestampDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TimestampDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_dds_timestamp(self, timestamp: TimestampDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                        VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s);
                    """,
                    {
                        "ts": timestamp.ts,
                        "year": timestamp.year,
                        "month": timestamp.month,
                        "day": timestamp.day,
                        "time": timestamp.time,
                        "date": timestamp.date
                    },
                )
                conn.commit()

    def get_timestamp(self, dt: datetime) -> Optional[TimestampDdsObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": dt},
            )
            obj = cur.fetchone()
        return obj


class TimestampLoader:
    WF_KEY = "timestamp_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw_orders = OrderRawRepository(pg)
        self.dds = TimestampDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_order_ts(self, order_raw: OrderJsonObj) -> TimestampDdsObj:
        order_json = json.loads(order_raw.object_value)
        dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
        t = TimestampDdsObj(id=0,
                            ts=dt,
                            year=dt.year,
                            month=dt.month,
                            day=dt.day,
                            time=dt.time(),
                            date=dt.date()
                            )

        return t

    def load_timestamps(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw_orders.load_raw_orders(last_loaded_id)
        for order in load_queue:

            ts_to_load = self.parse_order_ts(order)
            self.dds.insert_dds_timestamp(ts_to_load)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                order.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
