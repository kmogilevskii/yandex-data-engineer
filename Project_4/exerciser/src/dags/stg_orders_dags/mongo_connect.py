from typing import List
from urllib.parse import quote_plus as quote

from pymongo.mongo_client import MongoClient


class MongoConnect:
    def __init__(self,
                 cert_path: str,
                 user: str,
                 pw: str,
                 hosts: List[str],
                 rs: str,
                 auth_db: str,
                 main_db: str
                 ) -> None:

        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=','.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)

    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]
