import json
from datetime import datetime
from typing import Dict, List, Tuple

from pydantic import BaseModel

from src.dags.snowflake_dags.bonus_event_repository import BonusEventRepository
from src.dags.snowflake_dags.order_repositories import OrderDdsRepository
from src.dags.pg_connect import PgConnect
from src.dags.snowflake_dags.products_loader import ProductDdsObj, ProductDdsRepository
from src.dags.snowflake_dags.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository


class FctProductDdsObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class ProductPaymentJsonObj:
    def __init__(self, d: Dict) -> None:
        self.product_id: str = d["product_id"]
        self.product_name: str = d["product_name"]
        self.price: float = d["price"]
        self.quantity: int = d["quantity"]
        self.product_cost: float = d["product_cost"]
        self.bonus_payment: float = d["bonus_payment"]
        self.bonus_grant: float = d["bonus_grant"]


class BonusPaymentJsonObj:
    EVENT_TYPE = "bonus_transaction"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.order_id: str = d["order_id"]
        self.order_date: datetime = datetime.strptime(d["order_date"], "%Y-%m-%d %H:%M:%S")
        self.product_payments = [ProductPaymentJsonObj(it) for it in d["product_payments"]]


class FctProductDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_facts(self, facts: List[FctProductDdsObj]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for fact in facts:
                    cur.execute(
                        """
                            INSERT INTO dds.fct_product_sales(
                                product_id,
                                order_id,
                                count,
                                price,
                                total_sum,
                                bonus_payment,
                                bonus_grant
                            )
                            VALUES (
                                %(product_id)s,
                                %(order_id)s,
                                %(count)s,
                                %(price)s,
                                %(total_sum)s,
                                %(bonus_payment)s,
                                %(bonus_grant)s
                            );
                        """,
                        {
                            "product_id": fact.product_id,
                            "order_id": fact.order_id,
                            "count": fact.count,
                            "price": fact.price,
                            "total_sum": fact.total_sum,
                            "bonus_payment": fact.bonus_payment,
                            "bonus_grant": fact.bonus_grant
                        },
                    )
                conn.commit()


class FctProductsLoader:
    PAYMENT_EVENT = "bonus_transaction"
    WF_KEY = "fact_product_events_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_event_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw_events = BonusEventRepository(pg)
        self.dds_orders = OrderDdsRepository(pg)
        self.dds_products = ProductDdsRepository(pg)
        self.dds_facts = FctProductDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_order_products(self,
                             order_raw: BonusPaymentJsonObj,
                             order_id: int,
                             products: Dict[str, ProductDdsObj]
                             ) -> Tuple[bool, List[FctProductDdsObj]]:

        res = []

        for p_json in order_raw.product_payments:
            if p_json.product_id not in products:
                return (False, [])

            t = FctProductDdsObj(id=0,
                                 order_id=order_id,
                                 product_id=products[p_json.product_id].id,
                                 count=p_json.quantity,
                                 price=p_json.price,
                                 total_sum=p_json.product_cost,
                                 bonus_grant=p_json.bonus_grant,
                                 bonus_payment=p_json.bonus_payment
                                 )
            res.append(t)

        return (True, res)

    def load_product_facts(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw_events.load_raw_events(self.PAYMENT_EVENT, last_loaded_id)

        products = self.dds_products.list_products()
        prod_dict = {}
        for p in products:
            prod_dict[p.product_id] = p

        for order_raw in load_queue:
            payment_obj = BonusPaymentJsonObj(json.loads(order_raw.event_value))
            order = self.dds_orders.get_order(payment_obj.order_id)
            if not order:
                continue

            (success, facts_to_load) = self.parse_order_products(payment_obj, order.id, prod_dict)
            if not success:
                continue

            self.dds_facts.insert_facts(facts_to_load)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                order.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
