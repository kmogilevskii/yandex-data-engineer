import json
from datetime import datetime

from src.dags.snowflake_dags.order_repositories import (OrderDdsObj, OrderDdsRepository, OrderJsonObj,
                                OrderRawRepository)
from src.dags.pg_connect import PgConnect
from src.dags.snowflake_dags.restaurant_loader import RestaurantDdsRepository
from src.dags.snowflake_dags.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from src.dags.snowflake_dags.timestamp_loader import TimestampDdsRepository
from src.dags.snowflake_dags.user_loader import UserDdsRepository


class OrderLoader:
    WF_KEY = "orders_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = OrderRawRepository(pg)
        self.dds_users = UserDdsRepository(pg)
        self.dds_timestamps = TimestampDdsRepository(pg)
        self.dds_restaurants = RestaurantDdsRepository(pg)
        self.dds_orders = OrderDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_order(self, order_raw: OrderJsonObj, restorant_id: int, timestamp_id: int, user_id: int) -> OrderDdsObj:
        order_json = json.loads(order_raw.object_value)

        t = OrderDdsObj(id=0,
                        order_key=order_json['_id'],
                        restaurant_id=restorant_id,
                        timestamp_id=timestamp_id,
                        user_id=user_id,
                        order_status=order_json['final_status']
                        )

        return t

    def load_orders(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_orders(last_loaded_id)
        for order in load_queue:
            order_json = json.loads(order.object_value)
            restaurant = self.dds_restaurants.get_restaurant(order_json['restaurant']['id'])
            if not restaurant:
                continue

            dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
            timestamp = self.dds_timestamps.get_timestamp(dt)
            if not timestamp:
                continue

            user = self.dds_users.get_user(order_json['user']['id'])
            if not user:
                continue

            order_to_load = self.parse_order(order, restaurant.id, timestamp.id, user.id)
            self.dds_orders.insert_order(order_to_load)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                order.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
