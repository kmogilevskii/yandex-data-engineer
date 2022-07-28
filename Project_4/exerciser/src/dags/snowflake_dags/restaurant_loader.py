from datetime import datetime
import json
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.pg_connect import PgConnect
from src.dags.snowflake_dags.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository


class RestaurantJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class RestaurantRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_restaurants(self, last_loaded_record_id: int) -> List[RestaurantJsonObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class RestaurantDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_restaurant(self, restaurant: RestaurantDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                        VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                    """,
                    {
                        "restaurant_id": restaurant.restaurant_id,
                        "restaurant_name": restaurant.restaurant_name,
                        "active_from": restaurant.active_from,
                        "active_to": restaurant.active_to
                    },
                )
                conn.commit()

    def get_restaurant(self, restaurant_id: str) -> Optional[RestaurantDdsObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        active_from,
                        active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj


class RestaurantLoader:
    WF_KEY = "restaurants_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = RestaurantRawRepository(pg)
        self.dds = RestaurantDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.object_value)
            t = RestaurantDdsObj(id=r.id,
                                 restaurant_id=rest_json['_id'],
                                 restaurant_name=rest_json['name'],
                                 active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                 active_to=datetime(year=2099, month=12, day=31)
                                 )

            res.append(t)
        return res

    def load_restaurants(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_restaurants(last_loaded_id)
        restaurants_to_load = self.parse_restaurants(load_queue)
        for r in restaurants_to_load:
            self.dds.insert_restaurant(r)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                r.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
