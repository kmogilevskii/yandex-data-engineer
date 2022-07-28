import json
from typing import Dict, Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from pg_connect import PgConnect


class SettingRecord(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: str


class EtlSetting:
    def __init__(self, wf_key: str, setting: Dict) -> None:
        self.workflow_key = wf_key
        self.workflow_settings = setting


class DdsEtlSettingsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_setting(self, etl_key: str) -> Optional[EtlSetting]:
        with self._db.client() as conn:
            with conn.cursor(row_factory=class_row(SettingRecord)) as cur:
                cur.execute(
                    """
                        SELECT
                            id,
                            workflow_key,
                            workflow_settings
                        FROM dds.srv_wf_settings
                        WHERE workflow_key = %(etl_key)s;
                    """,
                    {"etl_key": etl_key},
                )
                obj = cur.fetchone()

        if not obj:
            return None

        return EtlSetting(obj.workflow_key, json.loads(obj.workflow_settings))

    def save_setting(self, sett: EtlSetting) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                        VALUES (%(etl_key)s, %(etl_setting)s)
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    {
                        "etl_key": sett.workflow_key,
                        "etl_setting": json.dumps(sett.workflow_settings)
                    },
                )
                conn.commit()
