from typing import List

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.raw_bonus_dags.pg_connect import PgConnect


class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


class RanksOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ranks(self) -> List[RankObj]:
        with self._db.client().cursor(row_factory=class_row(RankObj)) as cur:
            cur.execute(
                """
                    SELECT id, name, bonus_percent, min_payment_threshold
                    FROM ranks;
                """
            )
            objs = cur.fetchall()
        return objs


class RankDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_ranks(self, ranks: List[RankObj]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for rank in ranks:
                    cur.execute(
                        """
                            INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                            VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                            ON CONFLICT (id) DO UPDATE
                            SET
                                name = EXCLUDED.name,
                                bonus_percent = EXCLUDED.bonus_percent,
                                min_payment_threshold = EXCLUDED.min_payment_threshold;
                        """,
                        {
                            "id": rank.id,
                            "name": rank.name,
                            "bonus_percent": rank.bonus_percent,
                            "min_payment_threshold": rank.min_payment_threshold
                        },
                    )
                conn.commit()


class RankLoader:
    WF_KEY = "ranks_origin_to_stg_workflow"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect) -> None:
        self.origin = RanksOriginRepository(pg_origin)
        self.stg = RankDestRepository(pg_dest)

    def load_ranks(self):
        load_queue = self.origin.list_ranks()
        self.stg.insert_ranks(load_queue)
