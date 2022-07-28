from typing import Dict, List

from src.dags.stg_orders_dags.mongo_connect import MongoConnect


class CollectionLoader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_documents(self, collection_name: str, limit) -> List[Dict]:
        sort = [('update_ts', 1)]
        docs = list(self.dbs.get_collection(collection_name).find(sort=sort, limit=limit))
        return docs
