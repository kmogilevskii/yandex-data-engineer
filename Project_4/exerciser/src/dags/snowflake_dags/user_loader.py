import json
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.pg_connect import PgConnect
from src.dags.snowflake_dags.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository


class UserJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class UserDdsObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UserRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_restaurants(self, last_loaded_record_id: int) -> List[UserJsonObj]:
        with self._db.client().cursor(row_factory=class_row(UserJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class UserDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_user(self, user: UserDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_users(user_id, user_name, user_login)
                        VALUES (%(user_id)s, %(user_name)s, %(user_login)s);
                    """,
                    {
                        "user_id": user.user_id,
                        "user_name": user.user_name,
                        "user_login": user.user_login
                    },
                )
                conn.commit()

    def get_user(self, user_id: str) -> Optional[UserDdsObj]:
        with self._db.client().cursor(row_factory=class_row(UserDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        user_id,
                        user_name,
                        user_login
                    FROM dds.dm_users
                    WHERE user_id = %(user_id)s;
                """,
                {"user_id": user_id},
            )
            obj = cur.fetchone()
        return obj


class UserLoader:
    WF_KEY = "users_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_user_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = UserRawRepository(pg)
        self.dds = UserDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_users(self, raws: List[UserJsonObj]) -> List[UserDdsObj]:
        res = []
        for r in raws:
            user_json = json.loads(r.object_value)
            t = UserDdsObj(id=r.id,
                           user_id=user_json['_id'],
                           user_name=user_json['name'],
                           user_login=user_json['login'],
                           )

            res.append(t)
        return res

    def load_users(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_restaurants(last_loaded_id)
        users_to_load = self.parse_users(load_queue)
        for u in users_to_load:
            self.dds.insert_user(u)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                u.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
