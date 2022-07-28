import json
from datetime import datetime
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel

from src.dags.pg_connect import PgConnect
from src.dags.snowflake_dags.restaurant_loader import (RestaurantDdsRepository, RestaurantJsonObj,
                               RestaurantRawRepository)
from src.dags.snowflake_dags.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository


class ProductDdsObj(BaseModel):
    id: int

    product_id: str
    product_name: str
    product_price: float

    active_from: datetime
    active_to: datetime

    restaurant_id: int


class ProductDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_dds_products(self, products: List[ProductDdsObj]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            INSERT INTO dds.dm_products(
                                product_id,
                                product_name,
                                product_price,
                                active_from,
                                active_to,
                                restaurant_id)
                            VALUES (
                                %(product_id)s,
                                %(product_name)s,
                                %(product_price)s,
                                %(active_from)s,
                                %(active_to)s,
                                %(restaurant_id)s);
                        """,
                        {
                            "product_id": product.product_id,
                            "product_name": product.product_name,
                            "product_price": product.product_price,
                            "active_from": product.active_from,
                            "active_to": product.active_to,
                            "restaurant_id": product.restaurant_id
                        },
                    )
                conn.commit()

    def get_product(self, product_id: str) -> Optional[ProductDdsObj]:
        with self._db.client().cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj

    def list_products(self) -> List[ProductDdsObj]:
        with self._db.client().cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj


class ProductLoader:
    WF_KEY = "menu_products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = RestaurantRawRepository(pg)
        self.dds_products = ProductDdsRepository(pg)
        self.dds_restaurants = RestaurantDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_restaurants_menu(self, restorant_raw: RestaurantJsonObj, restorant_version_id: int) -> List[ProductDdsObj]:
        res = []
        rest_json = json.loads(restorant_raw.object_value)
        for prod_json in rest_json['menu']:
            t = ProductDdsObj(id=0,
                              product_id=prod_json['_id'],
                              product_name=prod_json['name'],
                              product_price=prod_json['price'],
                              active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                              active_to=datetime(year=2099, month=12, day=31),
                              restaurant_id=restorant_version_id
                              )

            res.append(t)
        return res

    def load_products(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_restaurants(last_loaded_id)
        for restaurant in load_queue:
            restaurant_version = self.dds_restaurants.get_restaurant(restaurant.object_id)
            if not restaurant_version:
                return

            products_to_load = self.parse_restaurants_menu(restaurant, restaurant_version.id)
            self.dds_products.insert_dds_products(products_to_load)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                restaurant.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
