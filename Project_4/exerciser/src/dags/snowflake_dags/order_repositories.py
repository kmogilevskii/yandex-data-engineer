
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.pg_connect import PgConnect


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class OrderRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_orders(self, last_loaded_record_id: int) -> List[OrderJsonObj]:
        with self._db.client().cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class OrderDdsObj(BaseModel):
    id: int
    order_key: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    order_status: str


class OrderDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_order(self, order: OrderDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_orders(order_key, restaurant_id, timestamp_id, user_id, order_status)
                        VALUES (%(order_key)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s, %(order_status)s);
                    """,
                    {
                        "order_key": order.order_key,
                        "restaurant_id": order.restaurant_id,
                        "timestamp_id": order.timestamp_id,
                        "user_id": order.user_id,
                        "order_status": order.order_status
                    },
                )
                conn.commit()

    def get_order(self, order_id: str) -> Optional[OrderDdsObj]:
        with self._db.client().cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_key,
                        restaurant_id,
                        timestamp_id,
                        user_id,
                        order_status
                    FROM dds.dm_orders
                    WHERE order_key = %(order_id)s;
                """,
                {"order_id": order_id},
            )
            obj = cur.fetchone()
        return obj
