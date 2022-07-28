from logging import Logger

from bson.objectid import ObjectId

from src.dags.stg_orders_dags.collection_loader import CollectionLoader
from src.dags.stg_orders_dags.pg_saver import PgSaver


class CollectionCopier:
    _LOG_THRESHOLD = 100
    _SESSION_LIMIT = 10000

    def __init__(self, collection_loader: CollectionLoader, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.log = logger

    def _parse_object_ids(self, obj):
        if isinstance(obj, dict):
            data = {}
            for (k, v) in obj.items():
                data[k] = self._parse_object_ids(v)
            return data
        elif hasattr(obj, "__iter__") and not isinstance(obj, str):
            return [self._parse_object_ids(v) for v in obj]
        elif isinstance(obj, ObjectId):
            return str(obj)
        else:
            return obj

    def run_copy(self, collection: str) -> int:
        data = self.collection_loader.get_documents(collection, self._SESSION_LIMIT)
        self.log.info(f"found {len(data)} documents to sync from {collection}.")

        self.pg_saver.init_collection(collection)

        i = 0

        for d in data:
            d_parsed_ids = self._parse_object_ids(d)
            self.pg_saver.save_object(collection, str(d["_id"]), d["update_ts"], d_parsed_ids)

            i += 1
            if i % self._LOG_THRESHOLD == 0:
                self.log.info(f"processed {i} documents of {len(data)} while syncing {collection}.")

        return len(data)
